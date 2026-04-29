import os
from datetime import datetime
import logging

from openapi_client import SlurmApi, SlurmdbApi
from openapi_client import ApiClient as Client
from openapi_client import Configuration as Config

logger = logging.getLogger(__name__)


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

    def get_jobs(self, start_time=None, end_time=None, users=None, accounts=None, clusters=None, **filters):
        """Get jobs using SlurmdbApi.slurmdb_v0044_get_jobs()"""
        # Use the latest v0044 API
        response = self.slurmdb.slurmdb_v0044_get_jobs(
            start_time=start_time,
            end_time=end_time,
            users=users,
            account=accounts,
            cluster=clusters,
            **filters
        )
        return response

    def get_associations(self, users=None, accounts=None, clusters=None):
        """Get associations using SlurmdbApi.slurmdb_v0044_get_associations()"""
        # Use the latest v0044 API - convert list parameters to comma-separated strings
        user_str = ','.join(users) if isinstance(users, list) else users
        account_str = ','.join(accounts) if isinstance(accounts, list) else accounts
        cluster_str = ','.join(clusters) if isinstance(clusters, list) else clusters

        response = self.slurmdb.slurmdb_v0044_get_associations(
            user=user_str,
            account=account_str,
            cluster=cluster_str
        )
        return response

    def get_users(self, names=None, admin_level=None, default_account=None):
        """Get users using SlurmdbApi.slurmdb_v0044_get_users()"""
        # Use the latest v0044 API
        names_str = ','.join(names) if isinstance(names, list) else names
        response = self.slurmdb.slurmdb_v0044_get_users(
            with_assocs=names_str,
            admin_level=admin_level,
            default_account=default_account
        )
        return response

    def get_accounts(self, names=None, clusters=None):
        """Get accounts using SlurmdbApi.slurmdb_v0044_get_accounts()"""
        # Use the latest v0044 API - check what parameters are actually supported
        names_str = ','.join(names) if isinstance(names, list) else names
        cluster_str = ','.join(clusters) if isinstance(clusters, list) else clusters

        # Call without unsupported parameters first, then add supported ones as needed
        response = self.slurmdb.slurmdb_v0044_get_accounts()
        return response

    def process_jobs_for_import(self, jobs_response):
        """
        Process jobs directly for import without CLI format conversion.

        Returns a generator of job objects with all necessary data for import,
        eliminating the need for string formatting and parsing.
        """
        if not hasattr(jobs_response, 'jobs') or not jobs_response.jobs:
            return

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
            job_data = {
                'job_id': job.job_id,
                'user': job.user,
                'uid': None,  # Unix UID not available in REST API job object - would need separate user lookup
                'account': job.account,
                'partition': job.partition,
                'qos': job.qos,
                'submit_time': submit_time,
                'start_time': start_time,
                'end_time': end_time,
                'elapsed_seconds': elapsed_seconds,
                'cpus': cpus,
                'allocated_nodes': job.allocation_nodes or 0,
                'allocated_tres': allocated_tres or '',
                'cpu_time_raw': cpu_time_raw,
                'node_list': job.nodes or '',
                'reservation': getattr(job.reservation, 'name', '') if job.reservation else '',
                'reservation_id': getattr(job.reservation, 'id', '') if job.reservation else '',
                'state': job.state.current[0] if (job.state and job.state.current and len(job.state.current) > 0) else 'UNKNOWN'
            }
            yield job_data

    def extract_association_hold_states(self, assoc_response):
        """
        Extract hold states directly from association objects.

        Returns a dict mapping account names to their hold status,
        eliminating regex parsing of formatted strings.
        """
        hold_states = {}

        if hasattr(assoc_response, 'associations') and assoc_response.associations:
            for assoc in assoc_response.associations:
                if assoc.account:
                    # Check if account is held by looking at max TRES per job
                    is_held = False
                    grp_nodes = None

                    # Navigate the real API structure: assoc.max.tres.per.job
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

                    hold_states[assoc.account] = {
                        'held': is_held,
                        'grp_nodes': grp_nodes,
                        'grp_jobs': grp_jobs_value,
                        'max_jobs': None  # Not directly available in this structure
                    }

        return hold_states

    def filter_and_validate_jobs(self, jobs_response, min_fields_required=10):
        """
        Direct job filtering without string conversions.

        Filters jobs based on data completeness and other criteria,
        replacing the string field counting logic.
        """
        if not hasattr(jobs_response, 'jobs') or not jobs_response.jobs:
            return []

        valid_jobs = []
        for job in jobs_response.jobs:
            # Count non-null essential fields - use correct field paths for V0044Job model
            essential_fields = [
                job.job_id, job.user, job.account, job.partition,
                job.qos, job.state,
                # Time fields are nested in job.time object
                job.time.submission if job.time else None,
                job.time.start if job.time else None,
            ]

            non_null_fields = sum(1 for field in essential_fields if field is not None)

            if non_null_fields >= min_fields_required:
                valid_jobs.append(job)
            else:
                logger.warning(f"Skipping job {job.job_id} with insufficient data ({non_null_fields} fields)")

        return valid_jobs
