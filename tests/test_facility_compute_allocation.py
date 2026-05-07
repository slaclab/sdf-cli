"""
Behavioral tests for FacilityComputeAllocation handling in the RepoRegistration daemon.
"""
import pytest
from unittest.mock import MagicMock

from modules.coactd import RepoRegistration, RequestStatus


START = '2026-01-01T00:00:00Z'
END = '2031-01-01T00:00:00Z'


def make_handler():
    handler = RepoRegistration.__new__(RepoRegistration)
    handler.logger = MagicMock()
    handler.username = 'sdf-bot'
    handler.password_file = '/tmp/fake'
    handler.client_name = 'test-client'
    handler.dry_run = False
    handler.back_channel = MagicMock()
    handler.ident = 'test-req-id'
    return handler


def test_approved_request_dispatches_cascade_with_payload_fields():
    """
    An approved FacilityComputeAllocation request routes to
    do_facility_compute_allocation_cascade with facility and cluster
    extracted from the request dict.
    """
    handler = make_handler()
    handler.do_facility_compute_allocation_cascade = MagicMock(return_value=True)

    req = {
        'facilityname': 'lcls',
        'clustername': 'ada',
    }
    result = handler.do('req1', 'INSERT', 'FacilityComputeAllocation', RequestStatus.APPROVED, req, dry_run=False)

    assert result is True
    handler.do_facility_compute_allocation_cascade.assert_called_once_with(
        'lcls', 'ada', dry_run=False
    )


def test_cascade_recalculates_every_repo_allocation_by_percentage():
    """
    When purchased nodes change, every repo on that cluster receives a new
    absolute allocation of (percentOfFacility / 100) * purchased, preserving
    each repo's percentage share of the facility.

    do_repo_compute_allocation is the single delegate for each repo; it owns
    the SLURM feature-flag check, the DB upsert, the SLURM playbook call, and
    the user sync.
    """
    handler = make_handler()
    handler.do_repo_compute_allocation = MagicMock(return_value=True)

    repos = [
        {
            'Id': 'repo-a', 'name': 'alpha', 'facility': 'lcls',
            'currentComputeAllocations': [{
                'Id': 'alloc-a', 'clustername': 'ada',
                'percentOfFacility': 25.0, 'allocatedNodesCount': 25.0,
                'start': START, 'end': END,
            }],
        },
        {
            'Id': 'repo-b', 'name': 'beta', 'facility': 'lcls',
            'currentComputeAllocations': [{
                'Id': 'alloc-b', 'clustername': 'ada',
                'percentOfFacility': 50.0, 'allocatedNodesCount': 50.0,
                'start': START, 'end': END,
            }],
        },
    ]
    handler.back_channel.execute.side_effect = [
        {'facility': {'computepurchases': [{'clustername': 'ada', 'purchased': 200}]}},  # facility query
        {'repos': repos},
    ]

    result = handler.do_facility_compute_allocation_cascade(
        'lcls', 'ada', dry_run=False
    )

    assert result is True
    assert handler.do_repo_compute_allocation.call_count == 2

    # args: (repo_name, facility, cluster, percent, allocated_resource, start, end)
    by_repo = {
        c.args[0]: c.args[4]
        for c in handler.do_repo_compute_allocation.call_args_list
    }
    assert by_repo['alpha'] == 50.0   # 25% of 200
    assert by_repo['beta'] == 100.0   # 50% of 200
