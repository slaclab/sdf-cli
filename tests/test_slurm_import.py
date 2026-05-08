"""
Behavioral tests for the slurmrest migration entry points.

Tests are written at the public API level (run_sacct, FacilityUsage.format_data)
and mock only the SlurmrestClient boundary — not internal helpers.
"""

import os
from unittest.mock import patch

import pendulum
import pytest

from modules.coact import run_sacct
from modules.slurmrest import SlurmrestClient

from openapi_client.models.v0042_openapi_slurmdbd_jobs_resp import V0042OpenapiSlurmdbdJobsResp
from openapi_client.models.v0042_openapi_assocs_resp import V0042OpenapiAssocsResp
from openapi_client.models.v0042_job import V0042Job
from openapi_client.models.v0042_job_time import V0042JobTime
from openapi_client.models.v0042_job_tres import V0042JobTres
from openapi_client.models.v0042_tres import V0042Tres
from openapi_client.models.v0042_assoc import V0042Assoc
from openapi_client.models.v0042_assoc_max import V0042AssocMax
from openapi_client.models.v0042_assoc_max_tres import V0042AssocMaxTres
from openapi_client.models.v0042_assoc_max_tres_group import V0042AssocMaxTresGroup


@pytest.fixture(autouse=True)
def slurm_jwt():
    """Provide a dummy JWT so SlurmrestClient.__init__ doesn't raise."""
    os.environ["SLURM_JWT"] = "test_token"
    yield
    os.environ.pop("SLURM_JWT", None)


def make_jobs_response(mem_mb: int = 256_000, cpus: int = 128, gpus: int = 0) -> V0042OpenapiSlurmdbdJobsResp:
    """Build a minimal slurmrest jobs response with realistic TRES values."""
    tres_list = [
        V0042Tres(type="cpu", count=cpus),
        V0042Tres(type="mem", count=mem_mb),
        V0042Tres(type="node", count=4),
    ]
    if gpus:
        tres_list.append(V0042Tres(type="gres/gpu", name="a100", count=gpus))

    job = V0042Job(
        job_id=42,
        user="jdoe",
        account="lcls:default",
        partition="roma",
        qos="normal",
        time=V0042JobTime(
            start=int(pendulum.datetime(2026, 5, 1, 8).timestamp()),
            end=int(pendulum.datetime(2026, 5, 1, 9).timestamp()),
            elapsed=3600,
        ),
        tres=V0042JobTres(allocated=tres_list),
        allocation_nodes=4,
    )
    return V0042OpenapiSlurmdbdJobsResp(jobs=[job])


def make_assoc_response(grp_nodes: int, account: str = "lcls:_regular_", cluster: str = "ada") -> V0042OpenapiAssocsResp:
    """Build a minimal slurmrest associations response."""
    assoc = V0042Assoc(
        account=account,
        cluster=cluster,
        user="",
        max=V0042AssocMax(
            tres=V0042AssocMaxTres(
                group=V0042AssocMaxTresGroup(
                    active=[V0042Tres(type="node", count=grp_nodes)]
                )
            )
        )
    )
    return V0042OpenapiAssocsResp(associations=[assoc])


# ---------------------------------------------------------------------------
# run_sacct
# ---------------------------------------------------------------------------

def test_run_sacct_memory_tres_carries_m_suffix():
    """
    run_sacct must yield JobData where memory TRES is expressed with an 'M' suffix.

    slurmrest returns memory as a bare integer in megabytes. Without the suffix,
    _kilos_to_int() would treat the value as raw bytes — a ~1024x underestimate
    against cluster["mem"] which is stored in bytes.
    """
    jobs_resp = make_jobs_response(mem_mb=256_000)

    with patch("modules.coact.SlurmrestClient") as MockClient:
        instance = MockClient.return_value
        instance.get_jobs.return_value = jobs_resp
        # Delegate process_jobs_for_import to the real implementation
        real_client = SlurmrestClient.__new__(SlurmrestClient)
        instance.process_jobs_for_import.side_effect = real_client.process_jobs_for_import

        jobs = list(run_sacct(date="2026-05-01"))

    assert len(jobs) == 1
    job = jobs[0]

    # Memory must carry 'M' suffix so downstream _kilos_to_int("256000M") yields bytes
    assert "mem=256000M" in job["allocated_tres"], (
        f"Expected 'mem=256000M' in allocated_tres, got: {job['allocated_tres']!r}"
    )
    # Basic shape checks
    assert job["job_id"] == 42
    assert job["user"] == "jdoe"
    assert job["cpus"] == 128
