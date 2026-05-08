import os
import logging
from typing import TypedDict

import pendulum
from pendulum import DateTime

from openapi_client import SlurmApi, SlurmdbApi
from openapi_client import ApiClient as Client
from openapi_client import Configuration as Config
from openapi_client.models.v0042_openapi_slurmdbd_jobs_resp import V0042OpenapiSlurmdbdJobsResp
from openapi_client.models.v0042_openapi_assocs_resp import V0042OpenapiAssocsResp

logger = logging.getLogger(__name__)


class JobData(TypedDict):
    job_id: int
    user: str
    uid: int | None
    account: str
    partition: str
    qos: str
    submit_time: DateTime | None
    start_time: DateTime | None
    end_time: DateTime | None
    elapsed_seconds: int
    cpus: int
    allocated_nodes: int
    allocated_tres: str
    cpu_time_raw: int
    node_list: str
    reservation: str
    reservation_id: str
    state: str

class HoldData(TypedDict):
    held: bool
    grp_nodes: int | None
    grp_jobs: int | None
    max_jobs: int | None

# Mapping of (account, cluster) -> HoldData for association hold states
HoldStates = dict[tuple[str, str], HoldData]


class SlurmrestClient:
    """
    Client for interacting with Slurm REST API.

    Args:
        host (str, optional): The slurmrest URL. If not provided, uses
                             SLURMREST_URL environment variable or defaults
                             to http://sdf-slurmrest-dev.slac.stanford.edu.
    """

    def __init__(self, host: str | None = None):
        c = Config()

        # Set the host URL - priority: parameter > environment > default
        if host:
            c.host = host
        else:
            c.host = os.getenv("SLURMREST_URL", "http://sdf-slurmrest-dev.slac.stanford.edu")

        # Set JWT token for authentication
        c.access_token = os.getenv("SLURM_JWT")
        if not c.access_token:
            raise EnvironmentError("No SLURM_JWT set")

        self.slurm = SlurmApi(Client(c))
        self.slurmdb = SlurmdbApi(Client(c))

    def get_jobs(self, start_time: str | None = None, end_time: str | None = None, **filters):
        """Get jobs using SlurmdbApi.slurmdb_v0042_get_jobs()"""
        response = self.slurmdb.slurmdb_v0042_get_jobs(
            start_time=start_time,
            end_time=end_time,
            **filters
        )
        return response

    def get_associations(self, accounts: str | None = None):
        """Get associations using SlurmdbApi.slurmdb_v0042_get_associations()"""
        # Use the latest v0042 API - convert list parameters to comma-separated strings
        response = self.slurmdb.slurmdb_v0042_get_associations(
            account=accounts
        )
        return response
    
    def process_jobs_for_import(self, jobs_response: V0042OpenapiSlurmdbdJobsResp):
        """
        Process jobs directly for import without CLI format conversion.

        Returns a generator of job objects with all necessary data for import,
        eliminating the need for string formatting and parsing.
        """
        for job in jobs_response.jobs:
            # Extract time information from the time structure
            submit_time = None
            start_time = None
            end_time = None
            elapsed_seconds = 0

            if job.time:
                if job.time.submission:
                    submit_time = pendulum.from_timestamp(job.time.submission)
                if job.time.start:
                    start_time = pendulum.from_timestamp(job.time.start)
                if job.time.end:
                    end_time = pendulum.from_timestamp(job.time.end)
                if job.time.elapsed:
                    elapsed_seconds = job.time.elapsed

            # Extract TRES information
            allocated_tres = None
            cpus = 0
            cpu_time_raw = 0

            if job.tres and job.tres.allocated:
                # Convert TRES list to string format matching sacct output (type/name=count)
                tres_parts = []
                for tres in job.tres.allocated:
                    if tres.type and tres.count is not None:
                        if tres.type == 'cpu':
                            cpus = tres.count
                        # Include the name component (e.g. "gres/gpu") to match sacct format
                        key = f"{tres.type}/{tres.name}" if tres.name else tres.type
                        tres_parts.append(f"{key}={tres.count}")
                allocated_tres = ','.join(tres_parts)

                # Calculate CPU time raw (CPU count * elapsed seconds)
                cpu_time_raw = cpus * elapsed_seconds

            # Create a standardized job object with all needed fields
            job_data = JobData(
                job_id=job.job_id,
                user=job.user,
                uid=None,  # Unix UID not available in REST API job object - would need separate user lookup
                account=job.account,
                partition=job.partition,
                qos=job.qos,
                submit_time=submit_time,
                start_time=start_time,
                end_time=end_time,
                elapsed_seconds=elapsed_seconds,
                cpus=cpus,
                allocated_nodes=job.allocation_nodes or 0,
                allocated_tres=allocated_tres or '',
                cpu_time_raw=cpu_time_raw,
                node_list=job.nodes or '',
                reservation=getattr(job.reservation, 'name', '') if job.reservation else '',
                reservation_id=getattr(job.reservation, 'id', '') if job.reservation else '',
                state=job.state.current[0] if (job.state and job.state.current and len(job.state.current) > 0) else 'UNKNOWN'
            )
            yield job_data

    def extract_association_hold_states(self, assoc_response: V0042OpenapiAssocsResp):
        """
        Extract hold states directly from association objects.

        Returns a dict mapping (account, cluster) tuples to their hold status.
        Both keys are lower-cased to match the facility/cluster naming used in coact.

        The old sacctmgr implementation returned "account@cluster" as a single string;
        the REST API exposes these as separate fields on V0042Assoc, so no regex is needed.
        """
        hold_states: HoldStates = {}

        for assoc in assoc_response.associations:
            if assoc.account and assoc.cluster:
                # Check if account is held by looking at max TRES per job
                is_held = False
                grp_nodes = None

                # GrpTRES is the Slurm field written by toggle_job_blocking (via sacctmgr subprocess).
                # In the REST read model that field surfaces as assoc.max.tres.group.active,
                # NOT assoc.max.tres.per.job which is the per-individual-job limit (MaxTRESPerJob).
                if (assoc.max and assoc.max.tres and assoc.max.tres.group and
                    assoc.max.tres.group.active):
                    # Find the node TRES entry
                    for tres in assoc.max.tres.group.active:
                        if tres.type == 'node':
                            grp_nodes = tres.count
                            is_held = (tres.count == 0)
                            break

                # Properly handle V0042Uint32NoValStruct for job limits
                grp_jobs_value = None
                if (assoc.max and assoc.max.jobs and assoc.max.jobs.total):
                    total_struct = assoc.max.jobs.total
                    if total_struct.set:
                        if total_struct.infinite:
                            grp_jobs_value = -1  # Convention for unlimited
                        else:
                            grp_jobs_value = total_struct.number

                key = (assoc.account.lower(), assoc.cluster.lower())
                hold_states[key] = HoldData(
                    held=is_held,
                    grp_nodes=grp_nodes,
                    grp_jobs=grp_jobs_value,
                    max_jobs=None  # Not directly available in this structure
                )

        return hold_states
