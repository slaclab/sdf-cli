"""
Click-based implementation of the Coact command group.

This module provides a click.Group-based CommandManager replacement that
registers subcommands using click decorators instead of cliff's CommandManager.
"""

from loguru import logger
from typing import Any, Iterator, Optional, Sequence, TypedDict
from functools import wraps
from string import Template
import re
import math
import sys

import click
import json
import subprocess
from timeit import default_timer as timer

from .slurmrest import SlurmrestClient

import pendulum as pdl
from gql import gql

import requests
from urllib.parse import urlparse

# Import base classes from modules.base
from .base import GraphQlMixin, common_options, graphql_options, configure_logging_from_verbose
from .utils.graphql import GraphQlClient

# get local timezone
_now = pdl.now()

# Using loguru logger

# Define context settings to support -h for help
CONTEXT_SETTINGS = dict(help_option_names=['-h', '--help'])


class OveragePoint(TypedDict):
    facility: str
    cluster: str
    qos: str
    window_mins: int
    percentages: Sequence[float]
    percent_used: float
    held: bool
    over: bool
    change: bool
    purchased_nodes: int

class FacilityNodeUsage(TypedDict):
    facility: str
    cluster: str
    nodes: int


def parse_datetime(value: Any, timezone=_now.timezone, force_tz: bool = False):
    """Parse various datetime formats into pendulum DateTime objects."""
    dt = None
    kwargs = {}
    if force_tz:
        kwargs["tz"] = timezone
    if isinstance(value, int):
        dt = pdl.from_timestamp(value, **kwargs)
    else:
        dt = pdl.parse(value, **kwargs)
    return dt


def datetime_converter(o: Any) -> Optional[str]:
    """Convert pendulum DateTime to UTC string format."""
    if isinstance(o, pdl.DateTime):
        return str(o.in_tz("UTC")).replace("+00:00", "Z")
    return None


def time_function(level="INFO"):
    """Decorator to time function execution and log the duration."""
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            s = timer()
            result = func(*args, **kwargs)
            e = timer()
            logger.log(
                level.upper(),
                f"function {func.__name__!r} executed in {(e - s):.2f}s"
            )
            return result
        return wrapper
    return decorator


# Create the main coact group
@click.group(name='coact', help="Coact-Slurm integration tools", context_settings=CONTEXT_SETTINGS)
@click.pass_context
def coact(ctx):
    """Coact command group for slurm job management and accounting."""
    ctx.ensure_object(dict)


# ============================================================================
# SlurmDump Command
# ============================================================================

@coact.command(name='slurmdump')
@common_options
@click.option('--date', default='2023-10-18', help='Import jobs from this date')
@click.option('--starttime', default='00:00:00', help='Start time of job imports')
@click.option('--endtime', default='23:59:59', help='End time of job imports')
@click.pass_context
def slurm_dump(ctx, verbose, date, starttime, endtime):
    """Dumps data from slurm into flat files for later ingestion."""
    configure_logging_from_verbose(verbose)
    ctx.obj['verbose'] = verbose

    for job_data in run_sacct(
        date=date,
        start_time=starttime,
        end_time=endtime,
        verbose=verbose > 0
    ):
        # Convert job object to a readable format for CLI output
        job_line = json.dumps(job_data, default=str, separators=(',', ':'))
        click.echo(job_line)


def run_sacct(
    date: str = "2023-10-12",
    start_time: str = "00:00:00",
    end_time: str = "23:59:59",
    verbose: bool = False
) -> Any:
    """Get job data from SLURM REST API - returns job objects for efficient processing."""
    # Convert date and time to datetime format for REST API
    start_datetime = f"{date}T{start_time}"
    end_datetime = f"{date}T{end_time}"

    if verbose:
        logger.info(f"Using SLURM REST API with start_time={start_datetime}, end_time={end_datetime}")

    try:
        client = SlurmrestClient()
        jobs_response = client.get_jobs(
            start_time=start_datetime,
            end_time=end_datetime
        )

        # Always return job objects for efficient processing
        for job_data in client.process_jobs_for_import(jobs_response):
            yield job_data

    except Exception as e:
        logger.error(f"Failed to get jobs from SLURM REST API: {e}")
        # Re-raise to maintain error handling behavior
        raise



# ============================================================================
# SlurmImport Command
# ============================================================================

@coact.command(name='slurmimport')
@click.option('--print', 'print_output', is_flag=True, help='Verbose output')
@click.option('--debug', is_flag=True, help='Debug output')
@graphql_options
@click.option('--batch', default=150000, type=int, help='Batch upload size')
@click.option(
    '--data',
    type=click.File('r'),
    default='-',
    help='Data to read from (default: stdin)'
)
@click.option(
    '--output',
    type=click.Choice(['json', 'upload']),
    default='json',
    help='Output format'
)
@click.option(
    '--exit-on-error',
    is_flag=True,
    default=False,
    help='Terminate if cannot parse data'
)
@click.pass_context
def slurm_import(ctx, print_output, debug, username, password_file, batch, data, output, exit_on_error):
    """Reads sacctmgr info from slurm and translates it to coact accounting stats."""
    if debug:
        configure_logging_from_verbose(2)

    ctx.obj['verbose'] = print_output
    ctx.obj['exit_on_error'] = exit_on_error

    importer = SlurmImporter(
        username=username,
        password_file=password_file,
        verbose=print_output,
        exit_on_error=exit_on_error
    )

    importer.run(data, output, batch)


class SlurmImporter(GraphQlMixin):
    """Handles the slurm import logic."""

    def __init__(self, username: str, password_file: str, verbose: bool = False, exit_on_error: bool = False):
        self.username = username
        self.password_file = password_file
        self.verbose = verbose
        self.exit_on_error = exit_on_error
        self._allocid = {}
        self._clusters = {}

    def run(self, data, output_format: str, batch_size: int) -> None:
        """Run the import process using job objects."""
        self.back_channel = self.connect_graph_ql(
            username=self.username,
            password_file=self.password_file,
            timeout=300
        )
        self.get_metadata()

        buffer = []
        s = timer()

        # Process job objects directly (data is a file of newline-delimited JSON from slurmdump)
        for line in data:
            line = line.strip()
            if not line:
                continue
            try:
                job_data = json.loads(line)
            except json.JSONDecodeError as e:
                logger.error(f"Failed to parse JSON line: {e}")
                if self.exit_on_error:
                    sys.exit(1)
                continue
            if self.verbose:
                logger.info(f"Processing job {job_data.get('job_id')}")

            job = self.convert_slurmrest(job_data)

            if job:
                buffer.append(job)
                if len(buffer) >= batch_size:
                    self.generate_output(buffer, output_format)
                    buffer = []

        if len(buffer) > 0:
            self.generate_output(buffer, output_format)
        else:
            logger.warning("No buffers to output")

        duration = timer() - s
        logger.info(f"import completed in {duration:,.02f}")

    @staticmethod
    def _kilos_to_int(s: str) -> int:
        """Parse a Slurm TRES value string like '128', '4K', '32G' into an integer."""
        m = re.match(r"(^[0-9.]+)([KMG])?", s.upper())
        if m:
            mul = 1
            g = m.group(2)
            if g:
                p = "KMG".find(g)
                if p >= 0:
                    mul = math.pow(2, (p + 1) * 10)
                else:
                    raise ValueError(f"Can't handle multiplier={g!r} for value={s!r}")
            return int(float(m.group(1)) * mul)
        raise ValueError(f"Can't parse TRES value {s!r}")

    @staticmethod
    def _calc_resource_hours(
        start_time,
        end_time,
        allocated_tres: str,
        cluster: dict,
        alloc_nodes: int,
    ) -> float:
        """
        Calculate normalised resource-hours for a job.

        Mirrors the original calc_resource_hours() from commands/coact.py:672.
        The metric is: elapsed_secs * alloc_nodes * max_resource_ratio * cluster_cpus / 3600
        where max_resource_ratio is the maximum fraction of per-node resources used
        across cpu, gpu, and memory.
        """
        # Elapsed time in seconds; floor at 1s
        elapsed_secs = (end_time - start_time).total_seconds()
        if elapsed_secs <= 0:
            elapsed_secs = 1.0

        # Parse allocated TRES into per-node usage fractions
        used = {}
        if allocated_tres:
            for x in allocated_tres.split(","):
                k, v = x.split("=")
                if "gpu" in k:
                    k = "gpu"
                if alloc_nodes > 0:
                    used[k] = SlurmImporter._kilos_to_int(v) * 1.0 / alloc_nodes

        # Find the maximum ratio of used/available for cpu, gpu, mem
        max_ratio = 0.0
        for resource in ("cpu", "gpu", "mem"):
            if resource in used and resource in cluster:
                ratio = used[resource] / cluster[resource]
                if ratio > max_ratio:
                    max_ratio = ratio
                logger.debug(f"    {resource}: used {used[resource]} / {cluster[resource]} -> {ratio:.5f}")

        return elapsed_secs * alloc_nodes * max_ratio * cluster["cpu"] / 3600.0

    def convert_slurmrest(self, job_data: dict) -> Optional[dict]:
        """Convert a JobData dict (from process_jobs_for_import) into a GQL Job input dict."""
        try:
            remapped_job = self.remap_job_slurmrest(job_data)
            if not remapped_job:
                logger.warning(f"Failed to remap job {job_data.get('job_id')}")
                return None

            account = remapped_job['account']
            partition = remapped_job['partition']
            start_time = remapped_job.get('start_time')
            end_time = remapped_job.get('end_time')

            # Require usable timestamps
            if not start_time or not end_time:
                logger.warning(f"Job {job_data.get('job_id')}: missing start/end time, skipping")
                return None

            # Convert to pendulum if they came in as isoformat strings (from JSON round-trip)
            if isinstance(start_time, str):
                start_time = parse_datetime(start_time)
            if isinstance(end_time, str):
                end_time = parse_datetime(end_time)

            # Determine facility and repo from account ("facility:repo")
            try:
                facility, repo = account.split(":", 1)
            except ValueError:
                logger.warning(f"Job {job_data.get('job_id')}: cannot split account {account!r}, skipping")
                return None

            # Look up allocation ID matching this partition and start time
            allocId = None
            try:
                allocId = self.get_alloc_id(
                    facility.lower(), repo.lower(), partition.lower(), start_time
                )
            except Exception as e:
                logger.warning(f"Job {job_data.get('job_id')}: {e}")
                if self.exit_on_error:
                    sys.exit(1)
                return None

            # Calculate resource hours (skip if partition unknown)
            resource_hours = 0.0
            if partition in self._clusters:
                alloc_nodes = remapped_job.get('allocated_nodes', 0)
                allocated_tres = remapped_job.get('allocated_tres', '')
                resource_hours = self._calc_resource_hours(
                    start_time=start_time,
                    end_time=end_time,
                    allocated_tres=allocated_tres,
                    alloc_nodes=alloc_nodes,
                    cluster=self._clusters[partition],
                )
            else:
                logger.warning(f"Job {job_data.get('job_id')}: partition {partition!r} not in coact clusters, skipping")
                return None

            if resource_hours == 0.0:
                return None

            # Normalise QOS (mirrors commands/coact.py:773–783)
            qos = remapped_job.get('qos', 'normal')
            try:
                # Strip Slurm internal QOS decoration e.g. "42^normal@roma" → "normal"
                qos = qos.split("^")[1].split("@")[0]
            except (IndexError, AttributeError):
                pass
            if qos not in ("scavenger", "preemptable", "normal"):
                logger.warning(f"Job {job_data.get('job_id')}: unexpected qos {qos!r}")

            return {
                "jobId": str(remapped_job['job_id']),
                "username": remapped_job['user'],
                "allocationId": allocId,
                "qos": qos,
                "startTs": str(start_time.in_tz("UTC")).replace(" ", "T").replace("+00:00", ".000Z"),
                "endTs": str(end_time.in_tz("UTC")).replace(" ", "T").replace("+00:00", ".000Z"),
                "resourceHours": resource_hours,
            }

        except Exception as e:
            logger.error(f"Error converting job {job_data.get('job_id', 'unknown')}: {e}")
            if self.exit_on_error:
                sys.exit(1)
            return None

    def remap_job_slurmrest(self, job_data: dict) -> Optional[dict]:
        """Apply job remapping logic directly to job object."""
        # Apply the same filtering logic as the original remap_job
        account = job_data.get('account', '')
        user = job_data.get('user', '')
        partition = job_data.get('partition', '')

        # Skip certain accounts and users (same logic as original)
        if (
            account in ("shared", "shared:default")
            or account.startswith("shared")
            or user in ("jonl", "vanilla", "yemi", "yangw", "pav", "root", "reranna", "ppascual", "renata")
            or partition in ("fermi-transfer",)
        ):
            logger.info(f"Skipping a job {job_data.get('job_id')} due to specific account ({account}), user ({user}), or parition ({partition}).")
            return None

        # Clean up partition if it has commas
        if "," in partition:
            partition = partition.split(",")[0]

        # Clean up account if it has @ symbol
        if "@" in account:
            account = account.split("@")[0]

        # Fix QOS
        qos = job_data.get('qos', 'Unknown')
        if qos == "Unknown":
            qos = "normal"

        # Return the cleaned job data
        return {
            **job_data,
            'account': account,
            'partition': partition,
            'qos': qos
        }


    def get_metadata(self) -> bool:
        """Fetch repository and allocation metadata."""
        REPOS_GQL = gql("""
        query{
            repos(filter:{}){
                Id
                name
                facility
                principal
                leaders
                users
                currentComputeAllocations{
                    Id
                    clustername
                    start
                    end
                }
            }
            clusters(filter:{}){
                name
                memberprefixes
                cpu: nodecpucount
                gpu: nodegpucount
                mem: nodememgb
                gpumem: nodegpumemgb
            }
        }
        """)
        logger.trace(f"Fetching metadata from GraphQL")
        resp = self.back_channel.execute(REPOS_GQL)
        logger.trace(f"Metadata response: {resp}")

        self._allocid = {}
        for repo in resp["repos"]:
            for alloc in repo.get("currentComputeAllocations", []):
                key = (
                    repo["facility"].lower(),
                    repo["name"].lower(),
                    alloc["clustername"].lower(),
                )
                if key not in self._allocid:
                    self._allocid[key] = {}

                if "start" in alloc and "end" in alloc:
                    time_range = (
                        parse_datetime(alloc["start"]),
                        parse_datetime(alloc["end"]),
                    )
                    self._allocid[key][time_range] = alloc["Id"]
                    if self.verbose:
                        click.echo(f"found alloc: {key}: {time_range}")
                else:
                    logger.warning(f"{key} has no allocations")

        self._clusters = {}
        for cluster in resp["clusters"]:
            name = cluster["name"]
            self._clusters[name] = {}
            for k, v in cluster.items():
                if k in ("mem",):
                    v = v * 1073741824
                self._clusters[name][k] = v

        return True

    def get_alloc_id(self, facility: str, repo: str, cluster: str, time) -> str:
        """Get allocation ID for a given facility, repo, cluster and time."""
        key = (facility, repo, cluster)
        if key in self._allocid:
            time_ranges = self._allocid[key]
            for t, _id in time_ranges.items():
                logger.trace(f"Matching {t[0].isoformat()} <= {time.in_timezone(_now.timezone).isoformat()} < {t[1].isoformat()}?")
                if time >= t[0] and time < t[1]:
                    logger.trace(f"Found match, returning alloc id {_id}")
                    return _id
        raise Exception(f"could not determine alloc_id for {facility}:{repo} at {cluster} at timestamp {time}")

    def generate_output(self, jobs: list, destination: str):
        """Output the buffered jobs."""
        try:
            if destination == "json":
                return self.output_json(jobs)
            elif destination == "upload":
                return self.upload_jobs(jobs)
            else:
                raise NotImplementedError(f"unsupported output {destination}")
        except Exception as e:
            logger.exception(f"generation failed: {e}")

    def upload_jobs(self, jobs: list) -> bool:
        """Upload jobs to the GraphQL service."""
        import_gql = gql("""
            mutation jobsImport($jobs: [Job!]!) {
                jobsImport(jobs: $jobs) {
                    insertedCount
                    upsertedCount
                    modifiedCount
                    deletedCount
                }
            }
        """)
        logger.trace(f"Uploading {len(jobs)} jobs...")
        s = timer()
        result = self.back_channel.execute(import_gql, {"jobs": jobs})["jobsImport"]
        e = timer()
        duration = e - s
        logger.info(
            f"imported jobs Inserted={result['insertedCount']}, Upserted={result['upsertedCount']}, "
            f"Deleted={result['deletedCount']}, Modified={result['modifiedCount']} in {duration:,.02f}s"
        )
        return True

    def output_json(self, jobs: list, indent: int = 2):
        """Output jobs as JSON."""
        click.echo(json.dumps(jobs, indent=indent, default=datetime_converter))


# ============================================================================
# SlurmRecalculate Command
# ============================================================================

@coact.command(name='slurmrecalculate')
@click.option('--date', default='2023-10-18', help='Recalculate jobs from this date')
@common_options
@graphql_options
@click.pass_context
def slurm_recalculate(ctx, date, verbose, username, password_file):
    """Recalculate the usage numbers from slurm jobs in Coact."""
    configure_logging_from_verbose(verbose)
    ctx.obj['verbose'] = verbose

    client = GraphQlClient()
    back_channel = client.connect_graph_ql(
        username=username,
        password_file=password_file,
        timeout=300
    )

    s = timer()
    result = back_channel.execute(
        gql('mutation update { jobsAggregateForDate(thedate: "' + date + 'T08:00:00.0000Z" ){ status } }')
    )
    assert result["jobsAggregateForDate"]["status"] is True
    e = timer()
    duration = e - s
    logger.info(f"recalculated jobs in {duration:,.02f}s")


# ============================================================================
# Overage Command
# ============================================================================

@coact.command(name='overage')
@click.option('--date', default=lambda: pdl.now().format('YYYY-MM-DD'), help='Recalculate jobs from this date (default: today)')
@common_options
@graphql_options
@click.option('--windows', type=int, multiple=True, default=[15, 60, 10080, 43800], help='Time windows to collate overage calculations')
@click.option('--threshold', type=float, default=100.0, help='Percentage at which to be considered over allocation')
@click.option('--dry-run', is_flag=True, default=False, help='Do not actually enforce job holding')
@click.option('--influxdb-url', default='http://localhost:8086', help='InfluxDB server URL (default: http://localhost:8086)')
@click.option('--influxdb-username', default=None, help='InfluxDB username')
@click.option('--influxdb-password', default=None, help='InfluxDB password')
@click.option('--influxdb-database', default='coact', help='InfluxDB database name (default: coact)')
@click.pass_context
def overage(
        ctx,
        date: str,
        verbose: int,
        username: str,
        password_file: str,
        windows: Sequence[int],
        threshold: float,
        dry_run: bool,
        influxdb_url: str,
        influxdb_username: str,
        influxdb_password: str,
        influxdb_database: str
    ):
    """Recalculate the usage numbers from slurm jobs in Coact."""
    configure_logging_from_verbose(verbose)
    ctx.obj['verbose'] = verbose

    # create data collection object
    usages = FacilityUsage(
        username=username,
        password_file=password_file,
        windows=list(windows),
        threshold=threshold,
        dry_run=dry_run
    )

    # iterate and collect data, initiate toggle as needed
    data = []
    for point in usages.get(date):
        data.append(point)
        # Toggle job blocking only if held state needs to change
        if point['held'] is not None and point['change']:
            toggle_job_blocking(execute=not dry_run, point=point)

    # Bulk send all points to InfluxDB using raw requests
    if influxdb_url is not None and len(data) > 0:

        lines = []
        for point in data:
            line = f"allocation_usage,facility={point['facility']},cluster={point['cluster']},qos={point['qos']},window_mins={point['window_mins']} "
            line += f"held={str(point['held']).lower()},over={str(point['over']).lower()},change={str(point['change']).lower()},percent_used={float(point['percent_used'])},purchased_nodes={float(point['purchased_nodes']) if point.get('purchased_nodes') is not None else 0.0}"
            lines.append(line)

        try:
            # Parse URL
            parsed_url = urlparse(influxdb_url)
            write_url = f"{parsed_url.scheme or 'http'}://{parsed_url.hostname or 'localhost'}:{parsed_url.port or 8086}/write"

            # Prepare auth
            auth = None
            if influxdb_username is not None and influxdb_password is not None:
                auth = (influxdb_username, influxdb_password)

            logger.debug(f"InfluxDB client initialized: {write_url}")

            # Write data
            response = requests.post(write_url, params={'db': influxdb_database}, data='\n'.join(lines), auth=auth)
            response.raise_for_status()
            logger.info(f"Successfully wrote {len(lines)} points to InfluxDB")

        except Exception as e:
            logger.error(f"Failed to send data to InfluxDB: {e}")


def toggle_job_blocking(point: OveragePoint, execute: bool = False) -> bool:
    """Enable/disable job blocking for overaged allocations."""
    template = Template("sacctmgr modify -i account name=$facility:_regular_@$cluster set GrpTRES=node=$nodes")

    # Determine node count based on blocking state
    if point['over']:
        # Blocking: set to 0
        nodes = 0
    else:
        # Unblocking: use purchased nodes or fallback to unlimited
        nodes = point.get('purchased_nodes', -1)
        if nodes is None:
            nodes = -1
            logger.warning(f"No purchased node count available for {point['facility']}@{point['cluster']}, using unlimited")
        elif nodes > 0:
            logger.info(f"Restoring {nodes} nodes for {point['facility']}@{point['cluster']}")
        else:
            logger.warning(f"Invalid node count {nodes} for {point['facility']}@{point['cluster']}, using unlimited")
            nodes = -1

    facility_usage = FacilityNodeUsage(
        facility=point['facility'],
        cluster=point['cluster'],
        nodes=nodes
    )

    logger.info(f"Job blocking toggle for {facility_usage['facility']}@{facility_usage['cluster']}: nodes={nodes} (over={point['over']}, execute={execute})")
    cmd = template.safe_substitute(**facility_usage)
    logger.info(f"Command: {cmd}")

    if execute:
        try:
            result = subprocess.check_output(cmd.split())
            for line in result.split(b"\n"):
                if line.strip():
                    logger.debug(f"sacctmgr output: {line.decode().strip()}")
        except subprocess.CalledProcessError as e:
            logger.error(f"Failed to toggle job blocking: {e}")
            return False

    return True


class FacilityUsage(GraphQlMixin):
    """Handles facility usage calculations and enforcement."""

    def __init__(self, username: str, password_file: str, windows: list, threshold: float, dry_run: bool):
        self.username = username
        self.password_file = password_file
        self.windows = windows
        self.threshold = threshold
        self.dry_run = dry_run
        self._slurm_client: SlurmrestClient | None = None

    @property
    def slurm_client(self) -> SlurmrestClient:
        """Lazily construct SlurmrestClient so missing SLURMREST_JWT fails at call time, not startup."""
        if self._slurm_client is None:
            self._slurm_client = SlurmrestClient()
        return self._slurm_client

    def get(self, date: str) -> Iterator[OveragePoint]:
        """Run the overage calculation process."""
        self.back_channel = self.connect_graph_ql(
            username=self.username,
            password_file=self.password_file,
            timeout=300
        )
        logger.debug(f"Fetching usage data for date: {date}")
        data = self.get_data()
        for point in self.overaged(data, threshold=self.threshold):
            yield point

    def get_data(self) -> dict:
        """Fetch usage data from GraphQL."""
        per_window_template = Template(
            """_$key: facilityRecentComputeUsage(pastMinutes:$minutes) { cluster: clustername, facility, percentUsed }"""
        )
        logger.trace(f"Fetching windows {self.windows}")
        all_windows = []
        for w in self.windows:
            all_windows.append(per_window_template.safe_substitute(minutes=w, key=f"{w:0>6}"))
        logger.trace(f"Window queries: {all_windows}")

        query = "query usage {"
        query += "\n".join(all_windows) + ",\n"
        query += "facilities(filter:{}) { name, computepurchases { clustername, purchased } },\n"
        query += "repos { facility, allocs: currentComputeAllocations { cluster: clustername, start, end } }"
        query += "\n}"

        logger.trace(f"GraphQL query: {query}")
        result = self.back_channel.execute(gql(query))
        logger.trace(f"GraphQL response: {result}")
        return self.format_data(result)

    def format_data(self, result: dict) -> dict:
        """Format the raw data for processing."""
        # Build purchased nodes lookup from Facility.computepurchases
        fac_purchases = {}
        for fac in result.pop("facilities", []):
            for cp in fac.get("computepurchases") or []:
                fac_purchases[(fac["name"].lower(), cp["clustername"].lower())] = cp["purchased"]

        current = {}
        for k in result["repos"]:
            f = k["facility"].lower()
            if f not in current:
                current[f] = {}
            for item in k["allocs"]:
                c = item["cluster"].lower()
                current[f][c] = {"held": None, "percentUsed": [], "purchasedNodes": fac_purchases.get((f, c))}
        del result["repos"]

        for time, array in result.items():
            logger.trace(f"Looking at time {time} with {array}")
            for a in array:
                f = a["facility"].lower()
                c = a["cluster"].lower()
                logger.trace(f"Setting {f} {c} to {a['percentUsed']}")
                current[f][c]["percentUsed"].append(int(a["percentUsed"]))

        logger.trace(f"Overages: {current}")

        list_of_assoc = []
        for f in current.keys():
            for c in current[f].keys():
                list_of_assoc.append(f"{f}:_regular_@{c}")

        logger.trace(f"Getting hold states for accounts: {list_of_assoc}")

        # Query each account individually to avoid bulk-request issues with slurmrest.
        # list_of_assoc entries are "facility:_regular_@cluster"
        for entry in list_of_assoc:
            try:
                associations_response = self.slurm_client.get_associations(account=entry)
                hold_states = self.slurm_client.extract_association_hold_states(associations_response)
                logger.debug(f"Retrieved hold state for {entry}: {hold_states}")

                for (assoc_account, assoc_cluster), state_info in hold_states.items():
                    # assoc_account is e.g. "lcls:_regular_", assoc_cluster is e.g. "ada"
                    # Strip the ":_regular_" suffix to get the facility key used in current{}
                    f = assoc_account.split(":")[0]
                    c = assoc_cluster
                    if f in current and c in current[f]:
                        current[f][c]["held"] = state_info["held"]
                        logger.trace(f"Set {f}@{c} to {state_info['held']}")

            except Exception as e:
                logger.warning(f"Failed to get hold state for {entry}: {e}")

        return current

    def overaged(self, data: dict, threshold: float = 100.0) -> Iterator[OveragePoint]:
        """Check which allocations are over threshold and yield point objects."""
        logger.trace(f"Determining overages with threshold {threshold}%...")
        for fac, d in data.items():
            logger.trace(f"Looping facility {fac}...")
            for clust, m in d.items():
                percentages = m["percentUsed"]
                purchased_nodes = m.get("purchasedNodes")
                logger.trace(f"Sublooping {clust}, {percentages}, purchased_nodes: {purchased_nodes}")
                over = False
                for p in percentages:
                    if p >= threshold:
                        over = True
                values = ",".join([f"{i:>3}" for i in percentages])
                logger.trace(f"Looking at {fac}@{clust} over: {over}, {m}")
                change = not m["held"] == over
                if m["held"] is None:
                    change = False
                if len(percentages) > 0:
                    logger.info(f"{fac:16} {clust:12} qos=regular held={m['held'] if m['held'] is not None else '-':1} over={over:1} change={change:1} nodes={purchased_nodes or 'N/A':>5}   {values}")

                    # Yield a point for each window
                    for idx, pct in enumerate(percentages):
                        window_duration = self.windows[idx] if idx < len(self.windows) else idx
                        yield OveragePoint(
                            facility=fac.lower(),
                            cluster=clust.lower(),
                            qos="regular",
                            window_mins=window_duration,
                            percentages=percentages,
                            percent_used=pct,
                            held=bool(m["held"]) if m["held"] is not None else None,
                            over=bool(over),
                            change=bool(change),
                            purchased_nodes=purchased_nodes
                        )



# For backwards compatibility, allow running this module directly
if __name__ == '__main__':
    coact(obj={})
