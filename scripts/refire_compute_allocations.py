#!/usr/bin/env python3
"""
One-off migration helper for the preemptable slurm association hierarchy.

For every repo/partition in a facility that has a current compute allocation, find the most recent
RepoComputeAllocation request and refire it. The running reporegistration coactd picks the refire up
from the change stream and re-runs ensure-repo/ensure-users, which builds the new
<fac>:<repo>@<part> and <fac>:<repo>@<part>^preemptable leaves.

Allocations that predate the request model have no request to refire; for those a request mirroring
the existing allocation is generated and approved instead, which coactd services the same way.

Before anything is changed the whole facility is checked: every request must match its current Coact
allocation, and every slurm leaf must carry the limits Coact would enact. Any discrepancy aborts the
run without changing anything so it can be resolved by hand.
"""

import base64
import subprocess
import sys
import time
from math import ceil
from os import getenv
from typing import Optional

import click
import pendulum as pdl
from gql import Client, gql
from gql.transport.exceptions import TransportProtocolError
from gql.transport.requests import RequestsHTTPTransport
from loguru import logger

SDF_COACT_URI = getenv("SDF_COACT_URI", "coact-dev.slac.stanford.edu:443/graphql-service-dev")
COACT_USERNAME = getenv("COACT_USERNAME", "sdf-bot")
COACT_PASSWORD_FILE = getenv("COACT_PASSWORD_FILE", "./etc/.secrets/password")
REFIRE_TIMEOUT = int(getenv("REFIRE_TIMEOUT", "900"))
SACCTMGR = getenv("SACCTMGR", "sacctmgr")

# statuses at which a request has been acted upon, so its allocation was (attempted to be) enacted
ELIGIBLE_STATUSES = ("Completed", "Incomplete", "Approved")

# coactd's upsert_repo_compute_allocation defaults a missing end to start + 5 years
DEFAULT_END_DELTA = pdl.duration(years=5)

FLOAT_TOLERANCE = 1e-6

# outcomes that mean Coact and/or slurm need a human before anything is changed
DISCREPANCIES = ("drift", "coact-mismatch", "pending-request", "slurm-missing", "slurm-mismatch")

# slurm TRES this script compares; anything else on the association is ignored
SLURM_TRES = ("cpu", "mem", "node", "gres/gpu")

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
          cpus: allocatedCpusCount
          memory: allocatedMemGb
          nodes: allocatedNodesCount
          gpus: allocatedGpusCount
        }
      }
    }
""")

FACILITY_GQL = gql("""
    query facility( $facility: String! ) {
      facility( filter: {name: $facility} ) {
        computepurchases {
          clustername
          purchased
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

REQUEST_CREATE_GQL = gql("""
    mutation requestRepoComputeAllocation( $request: CoactRequestInput! ) {
      requestRepoComputeAllocation( request: $request ) {
        Id
        start
        end
        percentOfFacility
        allocated
      }
    }
""")

REQUEST_APPROVE_GQL = gql("""
    mutation requestApprove( $id: String! ) {
      requestApprove( id: $id )
    }
""")

REQUEST_REJECT_GQL = gql("""
    mutation requestReject( $id: String!, $notes: String! ) {
      requestReject( id: $id, notes: $notes )
    }
""")


def connect(timeout: int = 60) -> Client:
    with open(COACT_PASSWORD_FILE, "r") as f:
        password = f.read().strip()
    mux = f"{COACT_USERNAME}:{password}".encode("ascii")
    headers = {"Authorization": f"Basic {base64.b64encode(mux).decode('ascii')}"}
    transport = RequestsHTTPTransport(url=f"https://{SDF_COACT_URI}", headers=headers, timeout=timeout)
    return Client(transport=transport, fetch_schema_from_transport=False)


def parse_dt(value) -> Optional[pdl.DateTime]:
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


def pick_pending_requests(requests: list) -> dict:
    """Map (reponame, clustername) to a RepoComputeAllocation request that has not been acted on yet."""
    pending = {}
    for req in requests:
        if req.get("reqtype") == "RepoComputeAllocation" and req.get("approvalstatus") in (None, "NotActedOn"):
            pending.setdefault((req.get("reponame"), req.get("clustername")), req)
    return pending


def allocation_drift(req: dict, alloc: dict) -> list:
    """Differences between what coactd would upsert from req and the current allocation."""
    req_start = parse_dt(req.get("start"))
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
    return drift


def classify(repo: dict, alloc: dict, req: Optional[dict], pending: Optional[dict] = None, purchased: Optional[float] = None) -> tuple:
    """Return (outcome, detail) for a repo allocation; 'ok' is safe to refire and 'generate' safe to create a request for."""
    if not slurm_enabled(repo):
        return "slurm-disabled", "refire would remove the slurm accounts"

    if req is None:
        if parse_dt(alloc.get("start")) is None:
            return "no-start", "allocation has no start; coactd cannot service it"
        if pending is not None:
            return "pending-request", f"request {pending['Id']} is awaiting approval"
        if purchased is None:
            return "coact-mismatch", f"facility has no current purchase on {alloc['clustername']}; a request cannot be created"
        # requestRepoComputeAllocation recomputes allocated from the current purchase, so it must agree
        would_allocate = purchased / 100.0 * float(alloc.get("percentOfFacility") or 0)
        if abs(would_allocate - float(alloc.get("allocated") or 0)) > FLOAT_TOLERANCE:
            return "coact-mismatch", f"allocated: request would be {would_allocate} current={alloc.get('allocated')}"
        return "generate", ""

    if parse_dt(req.get("start")) is None:
        return "no-start", "request has no start; coactd cannot service it"
    drift = allocation_drift(req, alloc)
    if drift:
        return "drift", "; ".join(drift)
    return "ok", ""


def slurm_account(facility: str, repo: str, cluster: str) -> str:
    # the old leaf and the new regular leaf share this name (the default repo only ever has this one)
    return f"{facility}:{repo}@{cluster}".lower()


def expected_slurm_limits(repo: str, alloc: dict) -> dict:
    """Limits ensure-repo.yaml sets on slurm_account() from coactd's inputs; -1 is unlimited and None is 0 or unlimited."""
    if repo.lower() == "default":
        return {tres: -1 for tres in SLURM_TRES}
    cpus = int(alloc.get("cpus") or 0)
    memory = int(alloc.get("memory") or 0) * 1024
    nodes = int(ceil(alloc.get("nodes") or 0))
    gpus = int(alloc.get("gpus") or 0)
    return {
        "cpu": cpus if cpus else -1,
        "mem": memory if memory else -1,
        # a repo with no nodes was unlimited on the old leaf and is held at 0 on the new regular leaf
        "node": nodes if nodes else None,
        "gres/gpu": gpus if gpus else -1,
    }


def parse_mem_mb(value: str) -> int:
    units = {"K": 1 / 1024, "M": 1, "G": 1024, "T": 1024 ** 2, "P": 1024 ** 3}
    if value and value[-1].upper() in units:
        return int(round(float(value[:-1]) * units[value[-1].upper()]))
    return int(value)


def parse_grptres(grptres: str) -> dict:
    """Parse a sacctmgr GrpTRES string; any TRES that is not set is unlimited (-1)."""
    limits = {tres: -1 for tres in SLURM_TRES}
    for item in filter(None, (grptres or "").split(",")):
        key, _, value = item.partition("=")
        if key in limits:
            limits[key] = parse_mem_mb(value) if key == "mem" else int(value)
    return limits


def slurm_limits(account: str) -> list:
    """GrpTRES limits of every account level association for account (empty if it does not exist)."""
    cmd = [SACCTMGR, "show", "assoc", "where", f"account={account}", "format=Account,User,GrpTRES", "-P", "--noheader"]
    try:
        out = subprocess.run(cmd, capture_output=True, text=True, timeout=60, check=True).stdout
    except FileNotFoundError:
        raise click.ClickException(f"{SACCTMGR} not found; run this on the daemon host or set SACCTMGR")
    except subprocess.CalledProcessError as e:
        raise click.ClickException(f"{' '.join(cmd)} failed: {e.stderr.strip()}")
    limits = []
    for line in out.splitlines():
        parts = line.split("|")
        if len(parts) >= 3 and parts[0].lower() == account and parts[1] == "":
            limits.append(parse_grptres(parts[2]))
    return limits


def slurm_discrepancy(expected: dict, current: list) -> Optional[tuple]:
    """Return (outcome, detail) when slurm does not already hold what Coact would enact."""
    if not current:
        return "slurm-missing", "account does not exist in slurm"
    diffs = []
    for limits in current:
        for tres, want in expected.items():
            have = limits[tres]
            if (want is None and have not in (-1, 0)) or (want is not None and have != want):
                diffs.append(f"{tres}: coact={'0 or unlimited' if want is None else want} slurm={have}")
    if diffs:
        return "slurm-mismatch", "; ".join(diffs)
    return None


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


def generate_request(client: Client, facility: str, repo: str, alloc: dict) -> tuple:
    """Create and approve a request mirroring alloc; returns (request id, outcome)."""
    resp = client.execute(REQUEST_CREATE_GQL, variable_values={"request": {
        "reqtype": "RepoComputeAllocation",
        "reponame": repo,
        "facilityname": facility,
        "clustername": alloc["clustername"],
        "percentOfFacility": alloc["percentOfFacility"],
        "start": alloc["start"],
        "end": alloc["end"],
        "dontsendemail": True,
        "notes": f"generated by refire_compute_allocations for slurm hierarchy migration; mirrors existing allocation {alloc['Id']}",
    }})
    req = resp["requestRepoComputeAllocation"]

    # the server recomputes allocated; never approve something that would rewrite the allocation
    drift = allocation_drift(req, alloc)
    if drift:
        notes = "generated request does not match the existing allocation: " + "; ".join(drift)
        logger.error(f"{facility}:{repo}@{alloc['clustername']} request {req['Id']}: {notes}")
        client.execute(REQUEST_REJECT_GQL, variable_values={"id": req["Id"], "notes": notes})
        return req["Id"], "generated-mismatch"

    client.execute(REQUEST_APPROVE_GQL, variable_values={"id": req["Id"]})
    status = wait_for_request(client, facility, repo, req["Id"], REFIRE_TIMEOUT)
    return req["Id"], {"Completed": "generated-complete", "Incomplete": "generated-incomplete"}.get(status, status)


@click.command()
@click.option("--facility", required=True, help="Facility whose repos should be migrated")
@click.option("--continue-on-error", is_flag=True, help="Keep going after a refire ends Incomplete or times out")
@click.option("--dry-run", is_flag=True, help="Show what would be refired without refiring")
def main(facility, continue_on_error, dry_run):
    """Refire the latest RepoComputeAllocation for each repo/partition in FACILITY."""
    # dev and prod have separate databases and daemons, so make the target obvious before anything is refired
    logger.info(f"using https://{SDF_COACT_URI} as {COACT_USERNAME}")
    client = connect()

    try:
        repos = client.execute(REPOS_GQL, variable_values={"filter": {"facility": facility}}).get("repos") or []
    except TransportProtocolError as e:
        raise click.ClickException(
            f"https://{SDF_COACT_URI} did not return GraphQL; check SDF_COACT_URI points at the basic auth "
            f"graphql-service endpoint ({str(e)[:120]}...)"
        )
    requests = client.execute(REQUESTS_GQL, variable_values={
        "filter": {"reqtype": "RepoComputeAllocation", "facilityname": facility}
    }).get("requests") or []
    latest = pick_latest_requests(requests)
    pending = pick_pending_requests(requests)
    purchases = {
        p["clustername"]: p["purchased"]
        for p in client.execute(FACILITY_GQL, variable_values={"facility": facility})["facility"].get("computepurchases") or []
    }

    # check everything up front so a discrepancy anywhere leaves the whole facility untouched
    targets = []
    for r in sorted(repos, key=lambda x: x["name"]):
        for alloc in sorted(r.get("currentComputeAllocations") or [], key=lambda a: a["clustername"]):
            key = (r["name"], alloc["clustername"])
            req = latest.get(key)
            outcome, detail = classify(r, alloc, req, pending.get(key), purchases.get(alloc["clustername"]))
            if outcome in ("ok", "generate"):
                account = slurm_account(facility, r["name"], alloc["clustername"])
                found = slurm_discrepancy(expected_slurm_limits(r["name"], alloc), slurm_limits(account))
                if found:
                    outcome, detail = found[0], f"{account}: {found[1]}"
            targets.append((r["name"], alloc, req["Id"] if req else "-", outcome, detail))

    discrepancies = [t for t in targets if t[3] in DISCREPANCIES]
    if discrepancies:
        for name, alloc, req_id, outcome, detail in discrepancies:
            logger.error(f"{facility}:{name}@{alloc['clustername']} ({req_id}): {outcome} {detail}")
        results = [(name, alloc["clustername"], req_id, outcome) for name, alloc, req_id, outcome, _ in targets]
        print_results(results)
        raise click.ClickException(f"{len(discrepancies)} discrepancies in {facility}; nothing was changed")

    results = []
    failed = False
    for name, alloc, req_id, outcome, detail in targets:
        cluster = alloc["clustername"]
        if outcome not in ("ok", "generate"):
            logger.warning(f"skipping {facility}:{name}@{cluster} ({req_id}): {outcome} {detail}")
            results.append((name, cluster, req_id, f"skipped-{outcome}"))
            continue
        if dry_run:
            action = "refire" if outcome == "ok" else "generate"
            logger.info(f"would {action} {facility}:{name}@{cluster} request {req_id}")
            results.append((name, cluster, req_id, f"would-{action}"))
            continue
        if failed and not continue_on_error:
            results.append((name, cluster, req_id, "not-attempted"))
            continue

        if outcome == "ok":
            logger.info(f"refiring {facility}:{name}@{cluster} request {req_id}")
            client.execute(REQUEST_REFIRE_GQL, variable_values={"id": req_id})
            status = wait_for_request(client, facility, name, req_id, REFIRE_TIMEOUT)
            outcome = {"Completed": "refired-complete", "Incomplete": "refired-incomplete"}.get(status, status)
        else:
            logger.info(f"generating a request for {facility}:{name}@{cluster} from allocation {alloc['Id']}")
            req_id, outcome = generate_request(client, facility, name, alloc)
        if outcome not in ("refired-complete", "generated-complete"):
            logger.error(f"{facility}:{name}@{cluster} request {req_id} ended {outcome}")
            failed = True
        results.append((name, cluster, req_id, outcome))

    print_results(results)
    sys.exit(1 if failed else 0)


def print_results(results: list) -> None:
    click.echo(f"{'repo':<32} {'partition':<16} {'request':<26} outcome")
    for name, cluster, req_id, outcome in results:
        click.echo(f"{name:<32} {cluster:<16} {req_id:<26} {outcome}")


if __name__ == "__main__":
    main()
