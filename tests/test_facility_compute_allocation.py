"""
Behavioral tests for FacilityComputeAllocation handling in the RepoRegistration daemon.
"""
from unittest.mock import MagicMock

import pytest

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


def make_allocation(alloc_id, percent, allocated, burst_percent=0.0, burst_allocated=0.0):
    return {
        'Id': alloc_id, 'clustername': 'ada',
        'percentOfFacility': percent, 'allocatedNodesCount': allocated,
        'burstPercentOfFacility': burst_percent, 'burstAllocated': burst_allocated,
        'start': START, 'end': END,
    }


def make_repos(*named_allocations):
    return [
        {
            'Id': f'repo-{i}', 'name': name, 'facility': 'lcls',
            'currentComputeAllocations': [alloc],
        }
        for i, (name, alloc) in enumerate(named_allocations)
    ]


def set_responses(handler, purchased, repos):
    handler.back_channel.execute.side_effect = [
        {'facility': {'computepurchases': [{'clustername': 'ada', 'purchased': purchased}]}},
        {'repos': repos},
    ]


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

    set_responses(handler, 200, make_repos(
        ('alpha', make_allocation('alloc-a', 25.0, 25.0)),
        ('beta', make_allocation('alloc-b', 50.0, 50.0)),
    ))

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


def test_cascade_recalculates_burst_allocation_by_percentage():
    """
    Burst is a second percentage of the same facility purchase and must be
    recomputed alongside the base allocation. The upsert replaces the whole
    allocation document, so omitting burst would silently reset it to zero.
    """
    handler = make_handler()
    handler.do_repo_compute_allocation = MagicMock(return_value=True)

    set_responses(handler, 200, make_repos(
        ('alpha', make_allocation('alloc-a', 25.0, 25.0, burst_percent=10.0, burst_allocated=10.0)),
    ))

    handler.do_facility_compute_allocation_cascade('lcls', 'ada', dry_run=False)

    kwargs = handler.do_repo_compute_allocation.call_args.kwargs
    assert kwargs['burst_percent'] == 10.0
    assert kwargs['burst_allocated'] == 20.0  # 10% of 200


def test_cascade_keeps_burst_at_zero_when_repo_has_none():
    """A repo without burst must not acquire one, and a null field must not blow up."""
    handler = make_handler()
    handler.do_repo_compute_allocation = MagicMock(return_value=True)

    alloc = make_allocation('alloc-a', 25.0, 25.0)
    alloc['burstPercentOfFacility'] = None
    alloc['burstAllocated'] = None
    set_responses(handler, 200, make_repos(('alpha', alloc)))

    handler.do_facility_compute_allocation_cascade('lcls', 'ada', dry_run=False)

    kwargs = handler.do_repo_compute_allocation.call_args.kwargs
    assert kwargs['burst_percent'] == 0.0
    assert kwargs['burst_allocated'] == 0.0


def test_cascade_raises_when_no_purchase_record():
    """
    When the facility has no computepurchases entry for the requested cluster,
    the cascade raises so the daemon marks the request incomplete rather than
    leaving it stuck in Approved.
    """
    handler = make_handler()
    handler.do_repo_compute_allocation = MagicMock(return_value=True)

    handler.back_channel.execute.return_value = {
        'facility': {'computepurchases': [{'clustername': 'other-cluster', 'purchased': 100}]}
    }

    with pytest.raises(RuntimeError, match='No purchase record'):
        handler.do_facility_compute_allocation_cascade('lcls', 'ada', dry_run=False)

    handler.do_repo_compute_allocation.assert_not_called()


def test_cascade_raises_on_negative_purchased_nodes():
    """A negative purchase is nonsense and must not reach any repo."""
    handler = make_handler()
    handler.do_repo_compute_allocation = MagicMock(return_value=True)

    handler.back_channel.execute.return_value = {
        'facility': {'computepurchases': [{'clustername': 'ada', 'purchased': -1}]}
    }

    with pytest.raises(RuntimeError, match='Invalid purchased nodes'):
        handler.do_facility_compute_allocation_cascade('lcls', 'ada', dry_run=False)

    handler.do_repo_compute_allocation.assert_not_called()


def test_cascade_zeroes_repo_allocations_when_purchase_is_zero():
    """
    A facility relinquishing all its nodes is legitimate: repos are driven to
    zero rather than the cascade failing and leaving them allocated against
    hardware that no longer exists.
    """
    handler = make_handler()
    handler.do_repo_compute_allocation = MagicMock(return_value=True)

    set_responses(handler, 0, make_repos(
        ('alpha', make_allocation('alloc-a', 25.0, 50.0, burst_percent=10.0, burst_allocated=20.0)),
    ))

    result = handler.do_facility_compute_allocation_cascade('lcls', 'ada', dry_run=False)

    assert result is True
    call = handler.do_repo_compute_allocation.call_args
    assert call.args[4] == 0.0
    assert call.kwargs['burst_allocated'] == 0.0


def test_cascade_processes_all_repos_then_raises_on_partial_failure():
    """
    A failure on one repo must not strand the others, but the request must not be
    reported complete either - the daemon only marks Incomplete on an exception.
    """
    handler = make_handler()

    set_responses(handler, 200, make_repos(
        ('alpha', make_allocation('alloc-a', 25.0, 10.0)),
        ('beta', make_allocation('alloc-b', 50.0, 10.0)),
    ))
    handler.do_repo_compute_allocation = MagicMock(
        side_effect=[Exception("slurm playbook failed"), True]
    )

    with pytest.raises(RuntimeError, match='lcls:alpha'):
        handler.do_facility_compute_allocation_cascade('lcls', 'ada', dry_run=False)

    assert handler.do_repo_compute_allocation.call_count == 2


def test_upsert_sends_burst_and_existing_allocation_id():
    """
    repoComputeAllocationUpsert rebuilds the document, so every field it owns has to
    be sent. Passing the existing allocation id makes the API replace that row instead
    of matching on (repoid, clustername, start) and inserting a duplicate.
    """
    handler = make_handler()

    handler.upsert_repo_compute_allocation(
        'repo-a', 'ada', 25.0, 50.0, START, END,
        burst_percent=10.0, burst_allocated=20.0, allocation_id='alloc-a',
    )

    repocompute = handler.back_channel.execute.call_args.args[1]['repocompute']
    assert repocompute['Id'] == 'alloc-a'
    assert repocompute['burstPercentOfFacility'] == 10.0
    assert repocompute['burstAllocated'] == 20.0


def test_upsert_omits_allocation_id_when_creating():
    """A repo with no allocation yet on this cluster must fall back to upsert-by-key."""
    handler = make_handler()

    handler.upsert_repo_compute_allocation('repo-a', 'ada', 25.0, 50.0, START, END)

    repocompute = handler.back_channel.execute.call_args.args[1]['repocompute']
    assert 'Id' not in repocompute


def test_cascade_targets_the_allocation_row_it_read():
    """
    The cascade updates an existing allocation in place, so it must name the row.
    Matching on (repoid, clustername, start) instead risks opening a second period
    that jobs.allocationId would not follow.
    """
    handler = make_handler()
    handler.do_repo_compute_allocation = MagicMock(return_value=True)

    set_responses(handler, 200, make_repos(
        ('alpha', make_allocation('alloc-a', 25.0, 10.0)),
    ))

    handler.do_facility_compute_allocation_cascade('lcls', 'ada', dry_run=False)

    assert handler.do_repo_compute_allocation.call_args.kwargs['allocation_id'] == 'alloc-a'


def _repo_with_slurm(burst_percent=10.0, burst_allocated=20.0):
    return {
        'repo': {
            'Id': 'repo-a', 'name': 'alpha', 'facility': 'lcls', 'users': ['someone'],
            'features': [{'name': 'slurm', 'state': True, 'options': []}],
            'computerequirement': 'Normal',
            'currentComputeAllocations': [{
                'Id': 'alloc-a', 'clustername': 'ada',
                'percentOfFacility': 25.0,
                'burstPercentOfFacility': burst_percent, 'burstAllocated': burst_allocated,
                'cpus': 10, 'memory': 8, 'nodes': 50, 'gpus': 0,
                'start': START, 'end': END,
            }],
        }
    }


def test_repo_allocation_preserves_stored_burst_when_not_supplied():
    """
    A caller that knows nothing about burst must not wipe it. The upsert replaces the
    whole document, so the stored value has to be read back and re-sent.
    """
    handler = make_handler()
    handler.run_playbook = MagicMock()
    handler.upsert_repo_compute_allocation = MagicMock()
    handler.back_channel.execute.side_effect = [_repo_with_slurm(), _repo_with_slurm()]

    handler.do_repo_compute_allocation('alpha', 'lcls', 'ada', 25.0, 50.0, START, END)

    kwargs = handler.upsert_repo_compute_allocation.call_args.kwargs
    assert kwargs['burst_percent'] == 10.0
    assert kwargs['burst_allocated'] == 20.0


def test_repo_allocation_uses_supplied_burst_over_stored():
    """An explicit burst from the request or cascade must win over the stored value."""
    handler = make_handler()
    handler.run_playbook = MagicMock()
    handler.upsert_repo_compute_allocation = MagicMock()
    handler.back_channel.execute.side_effect = [_repo_with_slurm(), _repo_with_slurm()]

    handler.do_repo_compute_allocation(
        'alpha', 'lcls', 'ada', 25.0, 50.0, START, END,
        burst_percent=30.0, burst_allocated=60.0,
    )

    kwargs = handler.upsert_repo_compute_allocation.call_args.kwargs
    assert kwargs['burst_percent'] == 30.0
    assert kwargs['burst_allocated'] == 60.0


def test_repo_allocation_does_not_target_a_row_unless_told_to():
    """
    A RepoComputeAllocation request may legitimately open a new allocation period,
    so the id of the current row must not be assumed.
    """
    handler = make_handler()
    handler.run_playbook = MagicMock()
    handler.upsert_repo_compute_allocation = MagicMock()
    handler.back_channel.execute.side_effect = [_repo_with_slurm(), _repo_with_slurm()]

    handler.do_repo_compute_allocation('alpha', 'lcls', 'ada', 25.0, 50.0, START, END)

    assert handler.upsert_repo_compute_allocation.call_args.kwargs['allocation_id'] is None


def test_cascade_skips_repos_whose_allocation_is_unchanged():
    """
    Each repo update runs two Ansible playbooks serially inside the subscription
    callback, so repos already at the correct node count must be left alone.
    """
    handler = make_handler()
    handler.do_repo_compute_allocation = MagicMock(return_value=True)

    set_responses(handler, 200, make_repos(
        ('alpha', make_allocation('alloc-a', 25.0, 50.0)),   # already 25% of 200
        ('beta', make_allocation('alloc-b', 50.0, 10.0)),    # stale
    ))

    result = handler.do_facility_compute_allocation_cascade('lcls', 'ada', dry_run=False)

    assert result is True
    assert [c.args[0] for c in handler.do_repo_compute_allocation.call_args_list] == ['beta']


def test_cascade_updates_repo_whose_only_change_is_burst():
    """A stale burst allocation alone is enough to warrant an update."""
    handler = make_handler()
    handler.do_repo_compute_allocation = MagicMock(return_value=True)

    set_responses(handler, 200, make_repos(
        ('alpha', make_allocation('alloc-a', 25.0, 50.0, burst_percent=10.0, burst_allocated=5.0)),
    ))

    handler.do_facility_compute_allocation_cascade('lcls', 'ada', dry_run=False)

    assert handler.do_repo_compute_allocation.call_args.kwargs['burst_allocated'] == 20.0


def test_cascade_warns_when_percentages_exceed_100():
    """Oversubscription is not blocked, but it must be visible in the log."""
    handler = make_handler()
    handler.do_repo_compute_allocation = MagicMock(return_value=True)

    set_responses(handler, 200, make_repos(
        ('alpha', make_allocation('alloc-a', 70.0, 10.0)),
        ('beta', make_allocation('alloc-b', 50.0, 10.0)),
    ))

    handler.do_facility_compute_allocation_cascade('lcls', 'ada', dry_run=False)

    assert 'oversubscribed' in handler.logger.warning.call_args.args[0]
