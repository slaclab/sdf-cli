import os
from datetime import datetime
import re

from openapi_client import SlurmApi, SlurmdbApi
from openapi_client import ApiClient as Client
from openapi_client import Configuration as Config


class SlurmrestClient:
    """
    Client for interacting with Slurm REST API.

    Args:
        host (str, optional): The slurmrest URL. If not provided, uses
                             SLURMREST_URL environment variable or defaults
                             to http://localhost:6820
    """

    def __init__(self, host: str | None = None):
        c = Config()

        # Set the host URL - priority: parameter > environment > default
        if host:
            c.host = host
        else:
            c.host = os.getenv("SLURMREST_URL", "http://localhost:6820")

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
        # Use the latest v0044 API
        response = self.slurmdb.slurmdb_v0044_get_associations(
            user=users,
            account=accounts,
            cluster=clusters
        )
        return response

    def get_users(self, names=None, admin_level=None, default_account=None):
        """Get users using SlurmdbApi.slurmdb_v0044_get_users()"""
        # Use the latest v0044 API
        response = self.slurmdb.slurmdb_v0044_get_users(
            with_assocs=names,  # Use with_assocs parameter for filtering by user names
            admin_level=admin_level,
            default_account=default_account
        )
        return response

    def get_accounts(self, names=None, clusters=None):
        """Get accounts using SlurmdbApi.slurmdb_v0044_get_accounts()"""
        # Use the latest v0044 API
        response = self.slurmdb.slurmdb_v0044_get_accounts(
            account=names,
            cluster=clusters
        )
        return response

    def transform_jobs_to_sacct_format(self, jobs_response):
        """Transform REST API job responses to match current sacct pipe format"""
        # Expected format: JobID|User|UID|Account|Partition|QOS|Submit|Start|End|Elapsed|NCPUS|AllocNodes|AllocTRES|CPUTimeRAW|NodeList|Reservation|ReservationId|State

        lines = []
        # Add header line first
        header = "JobID|User|UID|Account|Partition|QOS|Submit|Start|End|Elapsed|NCPUS|AllocNodes|AllocTRES|CPUTimeRAW|NodeList|Reservation|ReservationId|State"
        lines.append(header)

        if hasattr(jobs_response, 'jobs') and jobs_response.jobs:
            for job in jobs_response.jobs:
                # Convert datetime objects to SLURM format (Unix timestamp)
                submit_time = str(int(job.submit_time.timestamp())) if job.submit_time else ""
                start_time = str(int(job.start_time.timestamp())) if job.start_time else ""
                end_time = str(int(job.end_time.timestamp())) if job.end_time else ""

                # Calculate elapsed time (in seconds)
                elapsed = ""
                if job.start_time and job.end_time:
                    elapsed = str(int((job.end_time - job.start_time).total_seconds()))

                # Format the line matching sacct output
                line_parts = [
                    str(job.job_id) if job.job_id else "",
                    job.user if job.user else "",
                    str(job.uid) if job.uid else "",
                    job.account if job.account else "",
                    job.partition if job.partition else "",
                    job.qos if job.qos else "",
                    submit_time,
                    start_time,
                    end_time,
                    elapsed,
                    str(job.cpus) if job.cpus else "",
                    str(job.allocated_nodes) if job.allocated_nodes else "",
                    job.allocated_tres if job.allocated_tres else "",
                    str(job.cpu_time_raw) if job.cpu_time_raw else "",
                    job.node_list if job.node_list else "",
                    job.reservation if job.reservation else "",
                    str(job.reservation_id) if job.reservation_id else "",
                    job.state if job.state else ""
                ]

                lines.append("|".join(line_parts))

        return lines

    def transform_associations_to_sacctmgr_format(self, assoc_response):
        """Transform REST API association responses to match sacctmgr show format"""
        # Expected format: Account|GrpNodes|GrpJobs|MaxJobs (pipe-separated, no header in original output)

        lines = []
        if hasattr(assoc_response, 'associations') and assoc_response.associations:
            for assoc in assoc_response.associations:
                # Extract account name and limits
                account = assoc.account if assoc.account else ""
                grp_nodes = str(assoc.max_tres_per_job.get('node', '')) if assoc.max_tres_per_job else ""
                grp_jobs = str(assoc.grp_jobs) if assoc.grp_jobs is not None else ""
                max_jobs = str(assoc.max_jobs) if assoc.max_jobs is not None else ""

                line_parts = [account, grp_nodes, grp_jobs, max_jobs]
                lines.append("|".join(line_parts))

        return lines
