import os
from datetime import datetime
import logging
from typing import TypedDict

from openapi_client import SlurmApi, SlurmdbApi
from openapi_client import ApiClient as Client
from openapi_client import Configuration as Config
from openapi_client.models.v0044_openapi_slurmdbd_jobs_resp import V0044OpenapiSlurmdbdJobsResp
from openapi_client.models.v0044_openapi_assocs_resp import V0044OpenapiAssocsResp

logger = logging.getLogger(__name__)


class JobData(TypedDict):
    job_id: int
    user: str
    uid: int | None
    account: str
    partition: str
    qos: str
    submit_time: datetime | None
    start_time: datetime | None
    end_time: datetime | None
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

class HoldStates(TypedDict):
    account: HoldData


class SlurmrestClient:
    """
    Client for interacting with Slurm REST API.

    Args:
        host (str, optional): The slurmrest URL. If not provided, uses
                             SLURMREST_URL environment variable or defaults
                             to https://sdf-slurmrest-dev.slac.stanford.edu:6820.
    """

    def __init__(self, host: str | None = None):
        c = Config()

        # Set the host URL - priority: parameter > environment > default
        if host:
            c.host = host
        else:
            c.host = os.getenv("SLURMREST_URL", "https://sdf-slurmrest-dev.slac.stanford.edu:6820")

        # Set JWT token for authentication
        c.access_token = os.getenv("SLURM_JWT")
        if not c.access_token:
            raise KeyError("No SLURM_JWT set")

        self.slurm = SlurmApi(Client(c))
        self.slurmdb = SlurmdbApi(Client(c))

    def get_jobs(self, start_time: str | None = None, end_time: str | None = None, **filters):
        """Get jobs using SlurmdbApi.slurmdb_v0044_get_jobs()"""
        response = self.slurmdb.slurmdb_v0044_get_jobs(
            start_time=start_time,
            end_time=end_time,
            **filters
        )
        return response

    def get_associations(self, accounts: str | None = None):
        """Get associations using SlurmdbApi.slurmdb_v0044_get_associations()"""
        # Use the latest v0044 API - convert list parameters to comma-separated strings
        response = self.slurmdb.slurmdb_v0044_get_associations(
            account=accounts
        )
        return response
    
    def process_jobs_for_import(self, jobs_response: V0044OpenapiSlurmdbdJobsResp):
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
                    submit_time = datetime.fromtimestamp(job.time.submission)
                if job.time.start:
                    start_time = datetime.fromtimestamp(job.time.start)
                if job.time.end:
                    end_time = datetime.fromtimestamp(job.time.end)
                if job.time.elapsed:
                    elapsed_seconds = job.time.elapsed

            # Extract TRES information
            allocated_tres = None
            cpus = 0
            cpu_time_raw = 0

            if job.tres and job.tres.allocated:
                # Convert TRES list to string format for compatibility
                tres_parts = []
                for tres in job.tres.allocated:
                    if tres.type and tres.count:
                        if tres.type == 'cpu':
                            cpus = tres.count
                        tres_parts.append(f"{tres.type}={tres.count}")
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

    def extract_association_hold_states(self, assoc_response: V0044OpenapiAssocsResp):
        """
        Extract hold states directly from association objects.

        Returns a dict mapping account names to their hold status,
        eliminating regex parsing of formatted strings.
        """
        hold_states: HoldStates = {}

        for assoc in assoc_response.associations:
            if assoc.account:
                # Check if account is held by looking at max TRES per job
                is_held = False
                grp_nodes = None

                # Navigate the API structure: assoc.max.tres.per.job
                if (assoc.max and assoc.max.tres and assoc.max.tres.per and
                    assoc.max.tres.per.job):
                    # Find the node TRES entry
                    for tres in assoc.max.tres.per.job:
                        if tres.type == 'node':
                            grp_nodes = tres.count
                            is_held = (tres.count == 0)
                            break

                # Properly handle V0044Uint32NoValStruct for job limits
                grp_jobs_value = None
                if (assoc.max and assoc.max.jobs and assoc.max.jobs.total):
                    total_struct = assoc.max.jobs.total
                    if total_struct.set:
                        if total_struct.infinite:
                            grp_jobs_value = -1  # Convention for unlimited
                        else:
                            grp_jobs_value = total_struct.number

                hold_states[assoc.account] = HoldData(
                    held=is_held,
                    grp_nodes=grp_nodes,
                    grp_jobs=grp_jobs_value,
                    max_jobs=None  # Not directly available in this structure
                )

        return hold_states
