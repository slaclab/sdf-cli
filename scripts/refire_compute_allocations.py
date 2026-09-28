#!/usr/bin/env python3
"""
One-off migration helper for the preemptable slurm association hierarchy.

For every repo/partition in a facility that has a current compute allocation, find the most recent
RepoComputeAllocation request and refire it. The running reporegistration coactd picks the refire up
from the change stream and re-runs ensure-repo/ensure-users, which builds the new
<fac>:<repo>@<part> and <fac>:<repo>@<part>^preemptable leaves.
"""

import base64
import sys
import time
from os import getenv

import click
import pendulum as pdl
from gql import Client, gql
from gql.transport.requests import RequestsHTTPTransport
from loguru import logger

SDF_COACT_URI = getenv("SDF_COACT_URI", "coact-dev.slac.stanford.edu:443/graphql-service")
COACT_USERNAME = getenv("COACT_USERNAME", "sdf-bot")
COACT_PASSWORD_FILE = getenv("COACT_PASSWORD_FILE", "./etc/.secrets/password")
REFIRE_TIMEOUT = int(getenv("REFIRE_TIMEOUT", "900"))

# statuses at which a request has been acted upon, so its allocation was (attempted to be) enacted
ELIGIBLE_STATUSES = ("Completed", "Incomplete", "Approved")

# coactd's upsert_repo_compute_allocation defaults a missing end to start + 5 years
DEFAULT_END_DELTA = pdl.duration(years=5)

FLOAT_TOLERANCE = 1e-6

REPOS_GQL = gql("""
    query repos( $filter: RepoInput ) {
      repos( filter: $filter ) {
        name
        facility
        features {
          name
          state
        }
        currentComputeAllocations {
          Id
          clustername
          start
          end
          percentOfFacility
          allocated
        }
      }
    }
""")

REQUESTS_GQL = gql("""
    query requests( $filter: CoactRequestFilter ) {
      requests( fetchprocessed: true, showmine: false, filter: $filter ) {
        Id
        reqtype
        approvalstatus
        timeofrequest
        reponame
        facilityname
        clustername
        start
        end
        percentOfFacility
        allocated
      }
    }
""")

REQUEST_REFIRE_GQL = gql("""
    mutation requestRefire( $id: String! ) {
      requestRefire( id: $id )
    }
""")


def connect(timeout: int = 60) -> Client:
    with open(COACT_PASSWORD_FILE, "r") as f:
        password = f.read().strip()
    mux = f"{COACT_USERNAME}:{password}".encode("ascii")
    headers = {"Authorization": f"Basic {base64.b64encode(mux).decode('ascii')}"}
    transport = RequestsHTTPTransport(url=f"https://{SDF_COACT_URI}", headers=headers, timeout=timeout)
    return Client(transport=transport, fetch_schema_from_transport=False)


def parse_dt(value) -> pdl.DateTime | None:
    if value in (None, ""):
        return None
    return pdl.parse(str(value), tz="UTC")


def slurm_enabled(repo: dict) -> bool:
    for feature in repo.get("features") or []:
        if feature.get("name") == "slurm":
            return bool(feature.get("state"))
    return False


def pick_latest_requests(requests: list) -> dict:
    """Map (reponame, clustername) to the most recent acted-upon RepoComputeAllocation request."""
    latest = {}
    for req in sorted(requests, key=lambda r: parse_dt(r.get("timeofrequest")) or pdl.datetime(1970, 1, 1), reverse=True):
        if req.get("reqtype") != "RepoComputeAllocation" or req.get("approvalstatus") not in ELIGIBLE_STATUSES:
            continue
        key = (req.get("reponame"), req.get("clustername"))
        if key not in latest:
            latest[key] = req
    return latest


def classify(repo: dict, alloc: dict, req: dict | None) -> tuple:
    """Return (outcome, detail) for a repo allocation; only an outcome of 'ok' is safe to refire."""
    if req is None:
        return "no-request", "allocation exists but no acted-upon RepoComputeAllocation request"
    if not slurm_enabled(repo):
        return "slurm-disabled", "refire would remove the slurm accounts"
    req_start = parse_dt(req.get("start"))
    if req_start is None:
        return "no-start", "request has no start; coactd cannot service it"

    req_end = parse_dt(req.get("end")) or req_start + DEFAULT_END_DELTA
    expected = {
        "start": req_start,
        "end": req_end,
        "percentOfFacility": float(req.get("percentOfFacility") or 0),
        "allocated": float(req.get("allocated") or 0),
    }
    current = {
        "start": parse_dt(alloc.get("start")),
        "end": parse_dt(alloc.get("end")),
        "percentOfFacility": float(alloc.get("percentOfFacility") or 0),
        "allocated": float(alloc.get("allocated") or 0),
    }
    drift = []
    for field in ("start", "end"):
        if expected[field] != current[field]:
            drift.append(f"{field}: request={expected[field]} current={current[field]}")
    for field in ("percentOfFacility", "allocated"):
        if abs(expected[field] - current[field]) > FLOAT_TOLERANCE:
            drift.append(f"{field}: request={expected[field]} current={current[field]}")
    if drift:
        return "drift", "; ".join(drift)
    return "ok", ""


def wait_for_request(client: Client, facility: str, repo: str, req_id: str, timeout: int, interval: int = 10) -> str:
    """Poll until coactd moves the request out of Approved; returns the final status or 'timeout'."""
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        time.sleep(interval)
        resp = client.execute(REQUESTS_GQL, variable_values={
            "filter": {"reqtype": "RepoComputeAllocation", "facilityname": facility, "reponame": repo}
        })
        for req in resp.get("requests") or []:
            if req["Id"] == req_id and req["approvalstatus"] != "Approved":
                return req["approvalstatus"]
    return "timeout"


@click.command()
@click.option("--facility", required=True, help="Facility whose repos should be migrated")
@click.option("--continue-on-error", is_flag=True, help="Keep going after a refire ends Incomplete or times out")
@click.option("--dry-run", is_flag=True, help="Show what would be refired without refiring")
def main(facility, continue_on_error, dry_run):
    """Refire the latest RepoComputeAllocation for each repo/partition in FACILITY."""
    client = connect()

    repos = client.execute(REPOS_GQL, variable_values={"filter": {"facility": facility}}).get("repos") or []
    requests = client.execute(REQUESTS_GQL, variable_values={
        "filter": {"reqtype": "RepoComputeAllocation", "facilityname": facility}
    }).get("requests") or []
    latest = pick_latest_requests(requests)

    targets = []
    for r in sorted(repos, key=lambda x: x["name"]):
        for alloc in sorted(r.get("currentComputeAllocations") or [], key=lambda a: a["clustername"]):
            req = latest.get((r["name"], alloc["clustername"]))
            outcome, detail = classify(r, alloc, req)
            targets.append((r["name"], alloc["clustername"], req["Id"] if req else "-", outcome, detail))

    results = []
    failed = False
    for name, cluster, req_id, outcome, detail in targets:
        if outcome != "ok":
            logger.warning(f"skipping {facility}:{name}@{cluster} ({req_id}): {outcome} {detail}")
            results.append((name, cluster, req_id, f"skipped-{outcome}"))
            continue
        if dry_run:
            logger.info(f"would refire {facility}:{name}@{cluster} request {req_id}")
            results.append((name, cluster, req_id, "would-refire"))
            continue
        if failed and not continue_on_error:
            results.append((name, cluster, req_id, "not-attempted"))
            continue

        logger.info(f"refiring {facility}:{name}@{cluster} request {req_id}")
        client.execute(REQUEST_REFIRE_GQL, variable_values={"id": req_id})
        status = wait_for_request(client, facility, name, req_id, REFIRE_TIMEOUT)
        outcome = {"Completed": "refired-complete", "Incomplete": "refired-incomplete"}.get(status, status)
        if outcome != "refired-complete":
            logger.error(f"{facility}:{name}@{cluster} request {req_id} ended {outcome}")
            failed = True
        results.append((name, cluster, req_id, outcome))

    click.echo(f"{'repo':<32} {'partition':<16} {'request':<26} outcome")
    for name, cluster, req_id, outcome in results:
        click.echo(f"{name:<32} {cluster:<16} {req_id:<26} {outcome}")

    sys.exit(1 if failed else 0)


if __name__ == "__main__":
    main()
