"""
Click-based implementation of the Coact command group.

This module provides a click.Group-based CommandManager replacement that
registers subcommands using click decorators instead of cliff's CommandManager.
"""

from loguru import logger
from typing import Any, Generator, Iterator, List, Optional, TYPE_CHECKING
from functools import wraps
from string import Template
import re
import math
import sys

import click
import json
import subprocess
from timeit import default_timer as timer

import pendulum as pdl
from gql import gql

# Import base classes from modules.base
from .base import GraphQlMixin, common_options, graphql_options, configure_logging_from_verbose
from .utils.graphql import GraphQlClient

if TYPE_CHECKING:
    from typing import IO

# get local timezone
_now = pdl.now()

# Using loguru logger

# Define context settings to support -h for help
CONTEXT_SETTINGS = dict(help_option_names=['-h', '--help'])


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

    for line in run_sacct(
        date=date,
        start_time=starttime,
        end_time=endtime,
        verbose=verbose > 0
    ):
        click.echo(line)


def run_sacct(
    sacct_bin_path: str = "sacct",
    date: str = "2023-10-12",
    start_time: str = "00:00:00",
    end_time: str = "23:59:59",
    verbose: bool = False
) -> Any:
    """Run sacct command and yield output lines."""
    commandstr = f"""SLURM_TIME_FORMAT=%s {sacct_bin_path} --allusers --duplicates --allclusters --allocations --starttime="{date}T{start_time}" --endtime="{date}T{end_time}" --truncate --parsable2 --format=JobID,User,UID,Account,Partition,QOS,Submit,Start,End,Elapsed,NCPUS,AllocNodes,AllocTRES,CPUTimeRAW,NodeList,Reservation,ReservationId,State"""

    if verbose:
        logger.info(f"cmd: {commandstr}")

    index = 0

    process = subprocess.Popen(commandstr, shell=True, stdout=subprocess.PIPE)
    for line in iter(process.stdout.readline, b""):
        fields = line.decode("utf-8").split("|")
        if index == 0 or len(fields) >= 10:
            yield line.decode("utf-8").strip()
            index += 1
        else:
            logger.warning(
                f"skipping ({len(fields)}, {int(fields[7])} < {int(fields[8])}) {line}"
            )


# ============================================================================
# SlurmRemap Command
# ============================================================================

@coact.command(name='slurmremap')
@common_options
@click.option(
    '--data',
    type=click.File('r'),
    default='-',
    help='Data to read from (default: stdin)'
)
@click.pass_context
def slurm_remap(ctx, verbose, data):
    """Remaps/patches the slurm job data to prepare for import."""
    configure_logging_from_verbose(verbose)
    ctx.obj['verbose'] = verbose

    remapper = SlurmRemapper(verbose=verbose > 0)
    remapper.run(data)


class SlurmRemapper:
    """Handles the slurm remap logic."""

    def __init__(self, verbose: bool = False):
        self.verbose = verbose

    def run(self, data):
        """Run the remap process."""
        first = True
        index = {}
        order = []
        for line in data.readlines():
            if line:
                parts = line.split("|")
                if first:
                    index = {s: idx for idx, s in enumerate(parts)}
                    order = parts
                    first = False
                    click.echo(f"{line.strip()}")
                else:
                    out = self.convert(index, parts, order)
                    if out:
                        click.echo(f"{out}")

    def convert(self, index, parts, order) -> Optional[str]:
        d = {field: parts[idx] for field, idx in index.items()}
        d = self.remap_job(d)
        if d:
            out = []
            for i in order:
                out.append(d[i])
            return "|".join(out).strip()
        return None

    def remap_job(self, d) -> Optional[dict]:
        """Remap job data to fix account info."""
        if (
            d["Account"] in ("shared", "shared:default")
            or d["Account"].startswith("shared")
            or d["User"] in ("jonl", "vanilla", "yemi", "yangw", "pav", "root", "reranna", "ppascual", "renata",)
        ):
            return None

        if "," in d["Partition"]:
            a = d["Partition"].split(",")[0]
            d["Partition"] = a

        if "@" in d["Account"]:
            d["Account"], _ = d["Account"].split("@")

        if d["QOS"] in ("Unknown",):
            d["QOS"] = "normal"

        return d

    def remap_job_pre2024(self, d):
        """deal with old jobs with wrong account info"""
        # self.logger.info(f"in: {d}")
        if d["User"] in ("lsstsvc1") and d["Account"] in ("rubin", "shared", ""):
            d["Account"] = "rubin:production"
        elif d["Account"] in ("shared", "shared:default") or d["User"] in (
            "jonl",
            "vanilla",
            "yemi",
            "yangw",
            "pav",
            "root",
            "reranna",
            "ppascual",
            "renata",
        ):
            return None
        elif d["User"] in (
            "csaunder",
            "elhoward",
            "mrawls",
            "brycek",
            "mfl",
            "digel",
            "wguan",
            "laurenma",
            "smau",
            "bos",
            "erykoff",
            "ebellm",
            "mccarthy",
            "yesw",
            "abrought",
            "shuang92",
            "aconnoll",
            "daues",
            "aheinze",
            "zhaoyu",
            "dagoret",
            "kannawad",
            "kherner",
            "eske",
            "cslater",
            "sierrav",
            "jmeyers3",
            "lskelvin",
            "jchiang",
            "yanny",
            "ktl",
            "jneveu",
            "hchiang2",
            "snyder18",
            "fred3m",
            "brycek",
            "eiger",
            "esteves",
            "mxk",
            "yusra",
            "mrabus",
            "ryczano",
            "mgower",
            "yoachim",
            "scichris",
            "jcheval",
            "richard",
            "tguillem",
        ) and d["Account"] in ("", "milano", "roma", "rubin"):
            d["Account"] = "rubin:developers"
            if d["Partition"] == "ampere":
                d["Partition"] = "milano"
        elif d["User"] == "kocevski" or (
            d["User"]
            in (
                "burnett",
                "horner",
                "mdimauro",
                "burnett",
                "laviron",
                "omodei",
                "tyrelj",
                "echarles",
                "bruel",
            )
            and d["Account"] in ("", "latba", "ligo", "repository", "burnett")
        ):
            d["Account"] = "fermi:users"
        elif d["User"] in ("glastraw",):
            d["Account"] = "fermi:l1"
        elif d["User"] in ("vossj",):
            d["Partition"] = "roma"
        elif d["User"] in (
            "dcesar",
            "jytang",
            "rafimah",
        ):
            d["Account"] = "ad:beamphysics"
        elif d["User"] in (
            "kterao",
            "kvtsang",
            "anoronyo",
            "bkroul",
            "zhulcher",
            "koh0207",
            "drielsma",
            "lkashur",
            "dcarber",
            "amogan",
            "cyifan",
            "yjwa",
            "aj14",
            "jdyer",
            "sindhuk",
            "justinjm",
            "mrmooney",
            "bearc",
            "fuhaoji",
            "sfogarty",
            "carsmith",
            "yuntse",
        ) and not d["Account"] in (
            "neutrino:ml-dev",
            "neutrino:icarus-ml",
            "neutrino:slacube",
            "neutrino:dune-ml",
        ):
            d["Account"] = "neutrino:default"
            d["Partition"] = "ampere"
        elif d["User"] in (
            "dougl215",
            "zhezhang",
        ):  # and d['Account'] in ('ampere:default',):
            # self.logger.error("HERE")
            d["Account"] = "mli:default"
            d["Account"] = "neutrino:default"
            d["Partition"] = "ampere"
        elif d["User"] in (
            "dougl215",
            "zhezhang",
        ):  # and d['Account'] in ('ampere:default',):
            # self.logger.error("HERE")
            d["Account"] = "mli:default"
        elif d["User"] in (
            "jfkern",
            "taisgork",
            "valmar",
            "tgrant",
            "arijit01",
            "mmdoyle",
            "fpoitevi",
            "ashojaei",
            "monarin",
            "claussen",
            "batyuk",
            "kevinkgu",
            "tfujit27",
            "haoyuan",
            "aliang",
            "jshenoy",
            "dorlhiac",
            "xjql",
        ):  # and d['Account'] in ('','milano', 'roma'):
            d["Account"] = "lcls:default"
        elif d["User"] in (
            "psdatmgr",
            "xiangli",
            "sachsm",
            "hekstra",
            "snelson",
            "cwang31",
            "espov",
            "thorsten",
            "wilko",
            "snelson",
            "melchior",
            "cpo",
            "wilko",
            "mshankar",
        ) and d["Account"] in (
            "",
            "lcls:xpp",
            "lcls:psmfx",
            "lcls:data",
            "ampere",
            "roma",
            "rubin",
            "lcls-xpp1234",
            "lcls:xpptut15",
            "lcls:xpptut16",
            "s3dfadmin",
        ):
            d["Account"] = "lcls:default"
        elif d["User"] in ("lsstccs", "rubinmgr"):
            d["Account"] = "rubin:commissioning"
        elif d["User"] in (
            "majernik",
            "knetsch",
        ):
            d["Account"] = "facet:default"
        elif d["User"] in ("jberger",):
            d["Account"] = "epptheory:default"
        elif d["User"] in ("tabel",):
            d["Account"] = "kipac:kipac"
        elif d["User"] in (
            "vnovati",
            "owwen",
            "melwan",
            "zatschls",
            "yanliu",
            "cartaro",
            "aditi",
            "emichiel",
        ):
            d["Account"] = "supercdms:default"

        if d["Account"] == "":
            raise Exception(f"could not determine account for {d}")

        if "," in d["Partition"]:
            a = d["Partition"].split(",")[0]
            d["Partition"] = a
        elif d["Partition"] in ("testweka",):
            return None

        if not ":" in d["Account"]:
            d["Account"] = d["Account"] + ":default"

        if d["QOS"] in ("Unknown",):
            d["QOS"] = "preemptable"
        elif d["QOS"] in ("expedite",):
            d["QOS"] = "normal"

        # self.logger.info(f"out: {d}")
        return d


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
        """Run the import process."""
        self.back_channel = self.connect_graph_ql(
            username=self.username,
            password_file=self.password_file,
            timeout=300
        )
        self.get_metadata()

        first = True
        index = {}
        buffer = []
        s = timer()

        for line in data.readlines():
            if self.verbose:
                click.echo(f"\n{line.strip()}")
            if line:
                parts = line.split("|")
                if first:
                    index = {field: idx for idx, field in enumerate(parts)}
                    first = False
                else:
                    job = self.convert(index, parts)
                    if job:
                        buffer.append(job)
                        if len(buffer) >= batch_size:
                            self.generate_output(buffer, output_format)
                            buffer = []

        if len(buffer) > 0:
            self.generate_output(buffer, output_format)

        duration = timer() - s
        logger.info(f"upload completed in {duration:,.02f}")

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

    def convert(self, index: dict, parts: list, default_facility: str = "shared", default_repo: str = "default") -> Optional[dict]:
        """Convert a line of sacct output to a job dictionary."""

        def conv(s, fx, default=None):
            try:
                return fx(s)
            except:
                return default

        def kilos_to_int(s: str) -> int:
            m = re.match(r"(^[0-9.]+)([KMG])?", s.upper())
            if m:
                mul = 1
                g = m.group(2)
                if g:
                    p = "KMG".find(g)
                    if p >= 0:
                        mul = math.pow(2, (p + 1) * 10)
                    else:
                        raise Exception("Can't handle multiplier=%s for value=%s" % (g, s))
                return int(float(m.group(1)) * mul)
            else:
                raise Exception("Can't parse %s" % s)

        def calc_resource_hours(startTs, endTs, tres: str, cluster: dict, alloc_nodes: Optional[int], ncpus: Optional[int]) -> tuple:
            elapsed_secs = (endTs - startTs).total_seconds()
            # min time
            if elapsed_secs <= 0:
                elapsed_secs = 1.0
            # determine maximal amounts
            # if a single node, then divide all metrics by the number of nodes
            used = {}
            if tres != "":
                for x in tres.split(","):
                    k, v = x.split("=")
                    if "gpu" in k:
                        k = "gpu"
                    if alloc_nodes > 0:
                        used[k] = kilos_to_int(v) * 1.0 / alloc_nodes
            # if node is exclusive
            # max % of cpu, mem or gpu's for servers
            ratios = {}
            max_ratio = 0
            for resource in ("cpu", "gpu", "mem"):
                if resource in used:
                    ratios[resource] = used[resource] / cluster[resource]
                    if ratios[resource] > max_ratio:
                        max_ratio = ratios[resource]
                    logger.debug(f"    {resource}: used {used[resource]} / {cluster[resource]} -> {ratios[resource]:.5}")
            compute_time = elapsed_secs * ncpus / 3600.0
            resource_time = elapsed_secs * alloc_nodes * max_ratio * cluster["cpu"] / 3600.0
            if self.verbose:
                click.echo(f"  calc time: {elapsed_secs}s compute_hours: {resource_time:.5} core_hours: {compute_time:.5}")
            return resource_time, elapsed_secs

        d = {field: parts[idx] for field, idx in index.items()}
        facility = default_facility
        repo = default_repo
        try:
            facility, repo = d["Account"].split(":")
        except Exception:
            logger.warning(f"could not determine facility and repo from {d['Account']}")

        startTs = parse_datetime(int(d["Start"]), force_tz=True)
        endTs = parse_datetime(int(d["End"]), force_tz=True)

        if d["Partition"] in self._clusters:
            alloc_nodes = kilos_to_int(d["AllocNodes"])
            ncpus = conv(d["NCPUS"], int, 0)
            resource_hours, elapsed_secs = calc_resource_hours(
                startTs=startTs, endTs=endTs, tres=d["AllocTRES"],
                alloc_nodes=alloc_nodes, ncpus=ncpus, cluster=self._clusters[d["Partition"]],
            )
        else:
            resource_hours = 0.0
            logger.warning(f"partition {d['Partition']} not defined in coact, ignoring job")

        if resource_hours == 0.0:
            return None

        allocId = None
        try:
            allocId = self.get_alloc_id(facility, repo, d["Partition"], startTs)
        except Exception as e:
            logger.warning(f"{e}: {d}")
            if self.exit_on_error:
                sys.exit(1)
            return None

        qos = d["QOS"]
        try:
            a = qos.split("^")
            b = a[1].split("@")
            qos = b[0]
        except:
            pass
        if qos not in ("scavenger", "preemptable", "normal"):
            logger.warning(f"could not determine appropriate qos '{d['QOS']}': line {d}")

        out = {
            "jobId": d["JobID"],
            "username": d["User"],
            "allocationId": allocId,
            "qos": qos,
            "startTs": str(startTs.in_tz("UTC")).replace("+00:00", ".000Z"),
            "endTs": str(endTs.in_tz("UTC")).replace("+00:00", ".000Z"),
            "resourceHours": resource_hours,
        }
        return out


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
@click.pass_context
def overage(ctx, date, verbose, username, password_file, windows, threshold, dry_run):
    """Recalculate the usage numbers from slurm jobs in Coact."""
    configure_logging_from_verbose(verbose)
    ctx.obj['verbose'] = verbose

    handler = OverageHandler(
        username=username,
        password_file=password_file,
        windows=list(windows),
        threshold=threshold,
        dry_run=dry_run
    )
    handler.run(date)


class OverageHandler(GraphQlMixin):
    """Handles overage calculations and enforcement."""

    def __init__(self, username: str, password_file: str, windows: list, threshold: float, dry_run: bool):
        self.username = username
        self.password_file = password_file
        self.windows = windows
        self.threshold = threshold
        self.dry_run = dry_run

    def run(self, date: str):
        """Run the overage calculation process."""
        self.back_channel = self.connect_graph_ql(
            username=self.username,
            password_file=self.password_file,
            timeout=300
        )
        logger.debug(f"Fetching usage data for date: {date}")
        data = self.get_data()
        for facility_over in self.overaged(data, threshold=self.threshold):
            self.toggle_job_blocking(execute=not self.dry_run, **facility_over)

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
        query += "repos { facility, allocs: currentComputeAllocations { cluster: clustername, start, end } }"
        query += "\n}"

        logger.trace(f"GraphQL query: {query}")
        result = self.back_channel.execute(gql(query))
        logger.trace(f"GraphQL response: {result}")
        return self.format_data(result)

    def format_data(self, result: dict) -> dict:
        """Format the raw data for processing."""
        current = {}
        for k in result["repos"]:
            f = k["facility"].lower()
            if f not in current:
                current[f] = {}
            for item in k["allocs"]:
                c = item["cluster"].lower()
                current[f][c] = {"held": None, "percentUsed": []}
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

        cmd = f"sacctmgr show assoc where account={','.join(list_of_assoc)} --noheader -P format=Account,GrpNodes,GrpJobs,MaxJobs"
        logger.trace(f"Getting hold states using '{cmd}'...")

        try:
            for l in subprocess.check_output(cmd.split()).split(b"\n"):
                this = str(l, encoding="utf-8").strip().split("|")
                try:
                    m = re.match(r"^(?P<f>\S+):(?P<r>\S+)@(?P<c>\S+)$", this[0])
                    holding = True if this[1] == "0" else False
                    if m:
                        d = m.groupdict()
                        f = d["f"]
                        c = d["c"]
                        current[f][c]["held"] = holding
                        logger.trace(f"Set {f}@{c} to {holding}")
                except Exception:
                    pass
        except subprocess.CalledProcessError as e:
            logger.warning(f"Failed to get hold states: {e}")

        return current

    def overaged(self, data: dict, threshold: float = 100.0) -> Iterator[dict]:
        """Check which allocations are over threshold."""
        logger.trace(f"Determining overages with threshold {threshold}%...")
        for fac, d in data.items():
            logger.trace(f"Looping facility {fac}...")
            for clust, m in d.items():
                percentages = m["percentUsed"]
                logger.trace(f"Sublooping {clust}, {percentages}")
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
                    logger.info(f"{fac:16} {clust:12} held={m['held'] if m['held'] is not None else '-':1} over={over:1} change={change:1}   {values}")
                if m["held"] is not None and change:
                    yield {
                        "facility": fac.lower(),
                        "cluster": clust.lower(),
                        "percentages": percentages,
                        "held": m["held"],
                        "over": over,
                        "change": change,
                    }

    def toggle_job_blocking(self, execute: bool = False, **xargs) -> bool:
        """Enable/disable job blocking for overaged allocations."""
        template = Template("sacctmgr modify -i account name=$facility:_regular_@$cluster set GrpTRES=node=$nodes")
        xargs["nodes"] = 0 if xargs["over"] else -1
        logger.trace(f"{xargs['facility']} job holding must be toggled... execute={execute}")
        cmd = template.safe_substitute(**xargs)
        logger.trace(f"Command: {cmd}")

        if execute:
            try:
                for l in subprocess.check_output(cmd.split()).split(b"\n"):
                    logger.trace(f"{l}")
            except subprocess.CalledProcessError as e:
                logger.error(f"Failed to toggle job blocking: {e}")
                return False

        return True


# For backwards compatibility, allow running this module directly
if __name__ == '__main__':
    coact(obj={})
