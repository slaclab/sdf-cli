#!/usr/bin/env python3
"""
One-off migration helper for the preemptable slurm association hierarchy.

For every repo/partition in a facility that has a current compute allocation, find the most recent
RepoComputeAllocation request and refire it. The running reporegistration coactd picks the refire up
from the change stream and re-runs ensure-repo/ensure-users, which builds the new
<fac>:<repo>@<part> and <fac>:<repo>@<part>^preemptable leaves. The refire also moves the node limit off
the repo leaves onto <fac>:_regular_@<part>, so the leaves' current node limit is not compared.

Allocations that predate the request model have no request to refire; for those a request mirroring
the existing allocation is generated and approved instead, which coactd services the same way.

Before anything is changed the whole facility is checked: every request must match its current Coact
allocation, and every slurm account the refire touches is compared, TRES by TRES (cpu, mem, gres/gpu,
node), against the limits the refire will leave behind. That covers both trees:

    <fac>:<repo>@<part>               normal leaf (not for default)
    <fac>:<repo>@<part>^preemptable   preemptable leaf (<fac>:default@<part> for default)
    <fac>:_regular_@<part>            facility ceiling, node=ceil(purchased + burst)
    <fac>:_preemptable_@<part>        no limits

Every difference is reported. One is "expected" when slurm still holds what the old hierarchy set
(the per repo node limit, a missing ^preemptable leaf, the unburst node ceiling); the refire resolves
those. Anything else is UNEXPECTED and aborts the run without changing anything so it can be resolved
by hand. A node=0 overage hold on <fac>:_regular_@<part> is kept by the refire and reported as held.
Slurm is compared even for allocations Coact already blocks, so one --dry-run reports every problem.

--override lets unexpected slurm differences through so the refire enacts Coact's limits onto slurm. It
never goes the other way: discrepancies that would change Coact (drift, coact-mismatch, pending-request)
still abort the run.
"""

import base64
import subprocess
import sys
import time
from math import ceil
from os import getenv
from typing import NamedTuple, Optional

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

# per TRES verdicts: expected differences are resolved by the refire, unexpected ones need a human,
# and held is an overage node=0 hold the refire deliberately keeps
EXPECTED, UNEXPECTED, HELD = "expected", "UNEXPECTED", "held"

# stands in for a TRES value when the whole account does not exist yet
MISSING = "missing"

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

FACILITY_NAMES_GQL = gql("""
    query facilityNames {
      facilityNames
    }
""")

FACILITY_GQL = gql("""
    query facility( $facility: String! ) {
      facility( filter: {name: $facility} ) {
        computepurchases {
          clustername
          purchased
          burstNodes
        }
      }
    }
""")

# same as RepoRegistration.CLUSTER_NODE_RESOURCES_CGL in modules/coactd.py
CLUSTER_GQL = gql("""
    query clusters( $cluster: String! ) {
      clusters( filter: {name: $cluster} ) {
        name
        nodecpucount
        nodememgb
        nodegpucount
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


class AccountCheck(NamedTuple):
    """The limits the refire leaves on one slurm account, and the ones the old hierarchy left there."""
    account: str
    # TRES the refire sets, and to what; -1 is unlimited
    future: dict
    # TRES -> values the old hierarchy left that the refire replaces
    legacy: dict
    # the refire creates the account when it does not exist yet
    missing_ok: bool
    # the overage daemon may hold the account at node=0, which the refire keeps
    holdable: bool = False
    # the refire sets limits on the account at all
    managed: bool = True


class Diff(NamedTuple):
    account: str
    tres: str
    slurm: object
    future: object
    verdict: str
    note: str = ""


def allocation_resources(alloc: dict) -> tuple:
    """(cpus, memory MB, gpus, nodes) exactly as coactd passes them to ensure-repo.yaml."""
    return (
        int(alloc.get("cpus") or 0),
        int(alloc.get("memory") or 0) * 1024,
        int(alloc.get("gpus") or 0),
        int(ceil(alloc.get("nodes") or 0)),
    )


def repo_checks(facility: str, repo: str, cluster: str, alloc: dict) -> list:
    """The repo leaves ensure_repo.yaml sets up for alloc."""
    leaf = f"{facility}:{repo}@{cluster}".lower()
    if repo.lower() == "default":
        # default only has the preemptable leaf, unsuffixed and unlimited, before and after
        return [AccountCheck(leaf, {tres: -1 for tres in SLURM_TRES}, {}, missing_ok=False)]

    cpus, memory, gpus, nodes = allocation_resources(alloc)
    # ensure_repo.yaml _zero_: nodes is the unscaled allocation; cpus can round to zero for a tiny one
    zero = nodes == 0 or cpus == 0
    mem = -1 if zero or not memory else memory
    gpu = -1 if zero or not gpus else gpus
    # a repo with no allocation is held at cpu=0 for normal jobs and unlimited for preemptable ones
    regular = {"cpu": 0 if zero else cpus, "mem": mem, "gres/gpu": gpu, "node": -1}
    preempt = {"cpu": -1 if zero else cpus, "mem": mem, "gres/gpu": gpu, "node": -1}
    # the old ensure_repo.yaml set each limit on the shared leaf on its own, node included
    legacy = {"cpu": (cpus or -1,), "mem": (memory or -1,), "gres/gpu": (gpus or -1,), "node": (nodes or -1,)}
    return [
        AccountCheck(leaf, regular, legacy, missing_ok=False),
        AccountCheck(f"{leaf}^preemptable", preempt, {}, missing_ok=True),
    ]


def partition_checks(facility: str, cluster: str, purchased: Optional[float], burst: Optional[float], per_node: Optional[dict]) -> list:
    """The facility accounts ensure_repo.yaml sets up on cluster, from coactd's facility_* extravars."""
    checks = []
    regular = f"{facility}:_regular_@{cluster}".lower()
    # without a purchase coactd passes no facility_* vars and the ceiling is left untouched
    if purchased and purchased > 0:
        burst_ceiling = int(ceil(purchased + (burst or 0)))
        future = {"node": burst_ceiling}
        # without a cluster definition coactd sets no cpu/mem/gpu ceiling, so there is nothing to compare
        if per_node:
            nodes = ceil(purchased)
            # cpu and mem burst with the node ceiling, gpus stay at the purchase; zero means the
            # cluster has none of that resource, so it is left unlimited
            future["cpu"] = int(burst_ceiling * (per_node.get("nodecpucount") or 0)) or -1
            future["mem"] = int(burst_ceiling * (per_node.get("nodememgb") or 0) * 1024) or -1
            future["gres/gpu"] = int(nodes * (per_node.get("nodegpucount") or 0)) or -1
        # before burst the overage daemon restored node=ceil(purchased), or never set it, and nothing set cpu/mem/gpu
        legacy = {"node": (ceil(purchased), -1), "cpu": (-1,), "mem": (-1,), "gres/gpu": (-1,)}
        if per_node:
            # an earlier refire capped cpu/mem at the purchase alone, before they burst
            nodes = ceil(purchased)
            legacy["cpu"] += (int(nodes * (per_node.get("nodecpucount") or 0)) or -1,)
            legacy["mem"] += (int(nodes * (per_node.get("nodememgb") or 0) * 1024) or -1,)
        checks.append(AccountCheck(regular, future, legacy, missing_ok=True, holdable=True))
    checks.append(AccountCheck(
        f"{facility}:_preemptable_@{cluster}".lower(), {tres: -1 for tres in SLURM_TRES}, {}, missing_ok=True, managed=False,
    ))
    return checks


def compare(check: AccountCheck, current: list) -> list:
    """Every TRES on check.account (one entry in current per association) that differs from check.future."""
    if not current:
        if check.missing_ok:
            return [Diff(check.account, "-", MISSING, "created", EXPECTED, "the refire creates it")]
        return [Diff(check.account, "-", MISSING, "exists", UNEXPECTED, "account does not exist in slurm")]
    diffs = []
    for limits in current:
        for tres in SLURM_TRES:
            if tres not in check.future or limits[tres] == check.future[tres]:
                continue
            have, want = limits[tres], check.future[tres]
            if check.holdable and tres == "node" and have == 0:
                diffs.append(Diff(check.account, tres, have, want, HELD, "overage hold, kept by the refire"))
            elif have in check.legacy.get(tres, ()):
                diffs.append(Diff(check.account, tres, have, want, EXPECTED, "left by the old hierarchy"))
            elif not check.managed:
                diffs.append(Diff(check.account, tres, have, want, UNEXPECTED, "the refire does not change it"))
            else:
                diffs.append(Diff(check.account, tres, have, want, UNEXPECTED))
    return diffs


def describe(diff: Diff) -> str:
    note = f": {diff.note}" if diff.note else ""
    return f"{diff.account} {diff.tres}: slurm={diff.slurm} future={diff.future} ({diff.verdict}{note})"


def summarize(diffs: list) -> str:
    counts = {verdict: sum(1 for d in diffs if d.verdict == verdict) for verdict in (EXPECTED, UNEXPECTED, HELD)}
    text = f"{counts[EXPECTED]} expected, {counts[UNEXPECTED]} unexpected"
    return f"{text}, {counts[HELD]} held" if counts[HELD] else text


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
@click.option("--override", is_flag=True, help="Enforce Coact's limits onto slurm where they differ, instead of aborting")
@click.option("--dry-run", is_flag=True, help="Show what would be refired without refiring")
def main(facility, continue_on_error, override, dry_run):
    """Refire the latest RepoComputeAllocation for each repo/partition in FACILITY."""
    # dev and prod have separate databases and daemons, so make the target obvious before anything is refired
    logger.info(f"using https://{SDF_COACT_URI} as {COACT_USERNAME}")
    client = connect()

    try:
        facility_names = client.execute(FACILITY_NAMES_GQL).get("facilityNames") or []
    except TransportProtocolError as e:
        raise click.ClickException(
            f"https://{SDF_COACT_URI} did not return GraphQL; check SDF_COACT_URI points at the basic auth "
            f"graphql-service endpoint ({str(e)[:120]}...)"
        )
    # facility lookups are exact matches, and an unknown name fails deep inside the facility query
    if facility not in facility_names:
        similar = [name for name in facility_names if name.lower() == facility.lower()]
        hint = f"; did you mean {', '.join(similar)}?" if similar else f"; known facilities: {', '.join(sorted(facility_names))}"
        raise click.ClickException(f"facility {facility} does not exist in Coact{hint}")

    repos = client.execute(REPOS_GQL, variable_values={"filter": {"facility": facility}}).get("repos") or []
    requests = client.execute(REQUESTS_GQL, variable_values={
        "filter": {"reqtype": "RepoComputeAllocation", "facilityname": facility}
    }).get("requests") or []
    latest = pick_latest_requests(requests)
    pending = pick_pending_requests(requests)
    purchases = {
        p["clustername"].lower(): (p["purchased"], p.get("burstNodes") or 0)
        for p in client.execute(FACILITY_GQL, variable_values={"facility": facility})["facility"].get("computepurchases") or []
    }

    limits_cache = {}

    def check_slurm(checks: list) -> list:
        diffs = []
        for check in checks:
            if check.account not in limits_cache:
                limits_cache[check.account] = slurm_limits(check.account)
            diffs.extend(compare(check, limits_cache[check.account]))
        return diffs

    def log_diffs(diffs: list) -> None:
        for diff in diffs:
            if diff.verdict != UNEXPECTED:
                logger.info(describe(diff))
            elif override:
                # coactd enacts slurm from coact, so proceeding overwrites slurm with coact's values
                logger.warning(f"overriding {describe(diff)}")
            else:
                logger.error(describe(diff))

    # check everything up front so a discrepancy anywhere leaves the whole facility untouched
    targets = []
    partition_diffs = {}
    for r in sorted(repos, key=lambda x: x["name"]):
        for alloc in sorted(r.get("currentComputeAllocations") or [], key=lambda a: a["clustername"]):
            cluster = alloc["clustername"]
            key = (r["name"], cluster)
            req = latest.get(key)
            purchased, burst = purchases.get(cluster.lower(), (None, 0))
            outcome, detail = classify(r, alloc, req, pending.get(key), purchased)
            diffs = []
            # slurm is read only here, so it is checked even when coact already rules the allocation out,
            # letting a single dry run surface every problem; a slurm-disabled repo loses its accounts instead
            if outcome != "slurm-disabled":
                if cluster not in partition_diffs:
                    if not purchased or purchased <= 0:
                        logger.warning(f"{facility} has no purchase on {cluster}; the refire leaves {facility}:_regular_@{cluster} untouched")
                    per_node = next((c for c in client.execute(CLUSTER_GQL, variable_values={"cluster": cluster}).get("clusters") or []
                                     if c.get("name", "").lower() == cluster.lower()), None)
                    if per_node is None:
                        logger.warning(f"no cluster definition for {cluster}; its facility cpu/mem/gpu ceiling is not compared")
                    partition_diffs[cluster] = check_slurm(partition_checks(facility, cluster, purchased, burst, per_node))
                    log_diffs(partition_diffs[cluster])
                diffs = check_slurm(repo_checks(facility, r["name"], cluster, alloc))
                log_diffs(diffs)
                unexpected = [d for d in diffs if d.verdict == UNEXPECTED]
                if unexpected and outcome in ("ok", "generate"):
                    detail = "; ".join(describe(d) for d in unexpected)
                    if not override:
                        outcome = "slurm-missing" if any(d.slurm == MISSING for d in unexpected) else "slurm-mismatch"
                elif unexpected:
                    # keep the coact outcome, which already blocks the allocation, and note slurm alongside it
                    detail = "; ".join([detail] + [describe(d) for d in unexpected])
            targets.append((r["name"], alloc, req["Id"] if req else "-", outcome, detail, diffs))

    print_diffs([d for diffs in partition_diffs.values() for d in diffs] + [d for t in targets for d in t[5]])

    discrepancies = [t for t in targets if t[3] in DISCREPANCIES]
    # partition accounts are shared by every repo on the partition, so they block the whole run
    partition_unexpected = [d for diffs in partition_diffs.values() for d in diffs if d.verdict == UNEXPECTED]
    if discrepancies or (partition_unexpected and not override):
        for name, alloc, req_id, outcome, detail, _ in discrepancies:
            logger.error(f"{facility}:{name}@{alloc['clustername']} ({req_id}): {outcome} {detail}")
        results = [(name, alloc["clustername"], req_id, outcome, summarize(diffs)) for name, alloc, req_id, outcome, _, diffs in targets]
        print_results(results)
        count = len(discrepancies) + (len(partition_unexpected) if not override else 0)
        raise click.ClickException(f"{count} discrepancies in {facility}; nothing was changed")

    results = []
    failed = False
    for name, alloc, req_id, outcome, detail, diffs in targets:
        cluster = alloc["clustername"]
        summary = summarize(diffs)
        if outcome not in ("ok", "generate"):
            logger.warning(f"skipping {facility}:{name}@{cluster} ({req_id}): {outcome} {detail}")
            results.append((name, cluster, req_id, f"skipped-{outcome}", summary))
            continue
        if dry_run:
            action = "refire" if outcome == "ok" else "generate"
            suffix = " (override)" if detail else ""
            logger.info(f"would {action} {facility}:{name}@{cluster} request {req_id}{suffix}")
            results.append((name, cluster, req_id, f"would-{action}{suffix}", summary))
            continue
        if failed and not continue_on_error:
            results.append((name, cluster, req_id, "not-attempted", summary))
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
        results.append((name, cluster, req_id, outcome, summary))

    print_results(results)
    sys.exit(1 if failed else 0)


def print_diffs(diffs: list) -> None:
    if not diffs:
        click.echo("slurm already matches the future hierarchy")
        return
    click.echo(f"{'account':<48} {'tres':<9} {'slurm':>10} {'future':>10} {'verdict':<11} note")
    for d in diffs:
        click.echo(f"{d.account:<48} {d.tres:<9} {str(d.slurm):>10} {str(d.future):>10} {d.verdict:<11} {d.note}")
    click.echo("")


def print_results(results: list) -> None:
    click.echo(f"{'repo':<32} {'partition':<16} {'request':<26} {'outcome':<28} slurm diffs")
    for name, cluster, req_id, outcome, summary in results:
        click.echo(f"{name:<32} {cluster:<16} {req_id:<26} {outcome:<28} {summary}")


if __name__ == "__main__":
    main()
