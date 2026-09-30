"""
Unit tests for propagating a facility's absolute burst node headroom into the slurm
limits set by RepoRegistration.do_repo_compute_allocation.

The facility ceiling is its purchase plus its burst nodes and is the only node limit, set
on <fac>:_regular_@<part>. Every repo's raw cpus, memory and gpus are scaled by that same
ratio so the shares still add up to the ceiling; nodes is passed through unscaled.
"""

import sys
from unittest.mock import Mock, patch

import pendulum as pdl
import pytest
from graphql import print_ast

# Mock ansible_runner module to avoid all sdf-ansible dependencies
sys.modules['ansible_runner'] = Mock()

from modules.coactd import RepoRegistration


FACILITY = 'lcls'
REPO = 'testrepo'
CLUSTER = 'ada'


def repo_obj(allocated_nodes: float, slurm: bool = True):
    return {
        'Id': 'repo-1',
        'name': REPO,
        'facility': FACILITY,
        'users': ['alice', 'bob'],
        'features': [{'name': 'slurm', 'options': None, 'state': slurm}],
        'computerequirement': 'normal',
        'currentComputeAllocations': [{
            'Id': 'alloc-1',
            'clustername': CLUSTER,
            'start': '2026-01-01T00:00:00Z',
            'end': '2031-01-01T00:00:00Z',
            'percentOfFacility': 50.0,
            'cpus': 6400,
            'memory': 1000,
            'nodes': allocated_nodes,
            'gpus': 0,
        }],
    }


def facility_obj(purchased, burst_nodes, clustername=CLUSTER):
    """A facility carrying one purchase row for the cluster."""
    return {'facility': {'name': FACILITY, 'computepurchases': [
        {'clustername': clustername, 'purchased': purchased, 'burstNodes': burst_nodes}
    ]}}


def cluster_obj(nodecpucount=128, nodememgb=512, nodegpucount=0, name=CLUSTER):
    return {'clusters': [
        {'name': name, 'nodecpucount': nodecpucount, 'nodememgb': nodememgb, 'nodegpucount': nodegpucount}
    ]}


def no_purchase_facility():
    """A facility with no purchase row matching the cluster."""
    return {'facility': {'name': FACILITY, 'computepurchases': []}}


@pytest.fixture
def registration() -> RepoRegistration:
    with patch('modules.coactd.GraphQlSubscriber.__init__'), \
         patch('modules.coactd.AnsibleRunner.__init__'):
        reg = RepoRegistration(
            username='test-user',
            password_file='/tmp/test-password',
            client_name='test-client',
        )
        reg.logger = Mock()
        reg.back_channel = Mock()
        reg.run_playbook = Mock()
        return reg


def drive(registration, allocated_nodes, purchased=100, burst_nodes=0, facility=None, slurm=True, cluster=None):
    """Drive do_repo_compute_allocation; return the playbook calls it made."""
    registration.run_playbook.reset_mock()
    repo = repo_obj(allocated_nodes, slurm=slurm)
    responses = [{'repo': repo}]                                # _get_allocation_info
    if slurm:
        responses += [
            {'repoComputeAllocationUpsert': {'Id': 'repo-1'}},  # upsert
            {'repo': repo},                                     # _get_allocation_info again
            facility if facility is not None else facility_obj(purchased, burst_nodes),
            cluster if cluster is not None else cluster_obj(),      # only read with a purchase
        ]
    registration.back_channel.execute.side_effect = responses
    registration.do_repo_compute_allocation(
        repo=REPO, facility=FACILITY, cluster=CLUSTER, percent=50.0,
        allocated_resource=allocated_nodes,
        start=pdl.parse('2026-01-01T00:00:00Z'), end=None,
    )
    return registration.run_playbook.call_args_list


def ensure_repo_kwargs(calls):
    for call in calls:
        if call.args and call.args[0] == 'coact/slurm/ensure-repo.yaml':
            return call.kwargs
    raise AssertionError(f'ensure-repo.yaml was never run; got {[c.args for c in calls]}')


def run_allocation(registration, allocated_nodes, purchased, burst_nodes):
    """The common case: return the extravars handed to ensure-repo.yaml."""
    return ensure_repo_kwargs(drive(registration, allocated_nodes, purchased, burst_nodes))


class TestFacilityComputeCeiling:

    def test_returns_purchase_and_burst_for_the_cluster(self, registration):
        registration.back_channel.execute.return_value = facility_obj(256, 26)
        assert registration.facility_compute_ceiling(FACILITY, CLUSTER) == (256, 26)

    def test_matches_the_cluster_case_insensitively_on_both_sides(self, registration):
        # mixed case in the response AND in the argument, so neither .lower() can be dropped
        registration.back_channel.execute.return_value = facility_obj(256, 26, clustername='Ada')
        assert registration.facility_compute_ceiling(FACILITY, 'aDA') == (256, 26)

    def test_ignores_purchases_for_other_clusters(self, registration):
        registration.back_channel.execute.return_value = facility_obj(256, 26, clustername='milano')
        assert registration.facility_compute_ceiling(FACILITY, CLUSTER) == (None, 0.0)

    def test_returns_no_purchase_when_no_row_matches(self, registration):
        registration.back_channel.execute.return_value = no_purchase_facility()
        assert registration.facility_compute_ceiling(FACILITY, CLUSTER) == (None, 0.0)

    def test_tolerates_a_null_computepurchases_list(self, registration):
        registration.back_channel.execute.return_value = {'facility': {'name': FACILITY, 'computepurchases': None}}
        assert registration.facility_compute_ceiling(FACILITY, CLUSTER) == (None, 0.0)

    def test_treats_a_null_burst_as_zero(self, registration):
        registration.back_channel.execute.return_value = facility_obj(256, None)
        assert registration.facility_compute_ceiling(FACILITY, CLUSTER) == (256, 0.0)


class TestBurstScalesTheRepoResources:

    def test_repo_resources_and_facility_ceiling_include_the_burst(self, registration):
        # 100 purchased + 10 burst -> ratio 1.10 on the repo's 6400 cpus and 1000 GB
        extravars = run_allocation(registration, allocated_nodes=50, purchased=100, burst_nodes=10)
        assert extravars['cpus'] == 7040
        assert extravars['memory'] == 1126400
        assert extravars['gpus'] == 0
        assert extravars['facility_nodes'] == 110
        assert extravars['facility_cpus'] == 110 * 128
        assert extravars['facility_memory'] == 110 * 512 * 1024

    def test_nodes_are_passed_through_unscaled(self, registration):
        # the role only uses nodes to spot an empty allocation; leaves never get a node limit
        extravars = run_allocation(registration, allocated_nodes=0.5, purchased=100, burst_nodes=10)
        assert extravars['nodes'] == 0.5

    def test_zero_burst_changes_nothing(self, registration):
        extravars = run_allocation(registration, allocated_nodes=50, purchased=100, burst_nodes=0)
        assert (extravars['cpus'], extravars['memory']) == (6400, 1024000)
        assert extravars['facility_nodes'] == 100
        assert extravars['facility_cpus'] == 100 * 128

    def test_a_fractional_ceiling_rounds_the_node_limit_up(self, registration):
        extravars = run_allocation(registration, allocated_nodes=50, purchased=100, burst_nodes=0.5)
        assert extravars['facility_nodes'] == 101
        assert extravars['facility_cpus'] == round(100.5 * 128)

    def test_a_repo_with_no_allocation_stays_at_zero(self, registration):
        # the role turns nodes=0 into a cpu=0 hold on the regular leaf, so it must survive scaling
        extravars = run_allocation(registration, allocated_nodes=0, purchased=100, burst_nodes=10)
        assert extravars['nodes'] == 0

    def test_a_cluster_without_gpus_leaves_the_facility_gpus_unlimited(self, registration):
        extravars = run_allocation(registration, allocated_nodes=50, purchased=100, burst_nodes=10)
        assert extravars['facility_gpus'] == -1

    def test_a_gpu_cluster_caps_the_facility_gpus(self, registration):
        extravars = ensure_repo_kwargs(drive(registration, 50, purchased=100, burst_nodes=10,
                                             cluster=cluster_obj(nodegpucount=4)))
        assert extravars['facility_gpus'] == 440

    def test_an_unknown_cluster_writes_only_the_node_ceiling(self, registration):
        extravars = ensure_repo_kwargs(drive(registration, 50, purchased=100, burst_nodes=10,
                                             cluster={'clusters': []}))
        assert extravars['facility_nodes'] == 110
        assert not {'facility_cpus', 'facility_memory', 'facility_gpus'} & extravars.keys()

    @pytest.mark.parametrize('facility', [no_purchase_facility(), facility_obj(None, 10), facility_obj(-1, 10), facility_obj(0, 10)],
                             ids=['no-row', 'null', 'unlimited', 'zero'])
    def test_no_usable_purchase_means_no_burst_and_no_facility_ceiling(self, registration, facility):
        extravars = ensure_repo_kwargs(drive(registration, 50, facility=facility))
        assert (extravars['cpus'], extravars['memory'], extravars['nodes']) == (6400, 1024000, 50)
        assert not {'facility_nodes', 'facility_cpus', 'facility_memory', 'facility_gpus'} & extravars.keys()

    def test_the_account_being_configured_is_the_requested_one(self, registration):
        extravars = run_allocation(registration, allocated_nodes=50, purchased=100, burst_nodes=10)
        assert (extravars['facility'], extravars['repo'], extravars['partition']) == (FACILITY, REPO, CLUSTER)
        assert extravars['state'] == 'present'


class TestSlurmDisabled:
    """With the slurm feature off the repo is torn down; burst must not enter into it."""

    def test_the_account_is_removed_and_no_ceiling_is_written(self, registration):
        calls = drive(registration, allocated_nodes=50, slurm=False)
        extravars = ensure_repo_kwargs(calls)
        assert extravars['state'] == 'absent'
        assert 'facility_nodes' not in extravars
        assert 'nodes' not in extravars

    def test_the_facility_ceiling_is_never_queried(self, registration):
        drive(registration, allocated_nodes=50, slurm=False)
        # only _get_allocation_info runs; no upsert and no facility lookup
        assert registration.back_channel.execute.call_count == 1


class TestFacilityQuery:
    """The query text itself, which the hand-built mock responses cannot verify."""

    def test_the_facility_query_requests_burst_nodes(self, registration):
        registration.back_channel.execute.return_value = facility_obj(256, 26)
        registration.facility_compute_ceiling(FACILITY, CLUSTER)
        query = print_ast(registration.back_channel.execute.call_args[0][0])
        assert "burstNodes" in query, "do_repo_compute_allocation must ask for burst headroom"
        assert "purchased" in query
        assert "computepurchases" in query

    def test_the_cluster_query_requests_per_node_resources(self, registration):
        registration.back_channel.execute.return_value = cluster_obj()
        assert registration.cluster_node_resources(CLUSTER.upper())['nodecpucount'] == 128
        query = print_ast(registration.back_channel.execute.call_args[0][0])
        for field in ("nodecpucount", "nodememgb", "nodegpucount"):
            assert field in query
