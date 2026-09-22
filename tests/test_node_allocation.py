"""
Unit tests for node allocation functionality.
"""

import subprocess
from unittest.mock import Mock, patch

import pytest
from graphql import print_ast
from modules.coact import FacilityUsage, OveragePoint, influx_line, toggle_job_blocking


def create_graphql_response(usage_percent: float, nodes: int, burst_nodes: float = 0, windows=None):
    """Helper to create fresh GraphQL responses"""
    return {
        "repos": [
            {
                "facility": "LCLS",
                "allocs": [
                    {"cluster": "ada", "start": "2026-04-01", "end": "2026-05-01"},
                ]
            }
        ],
        "facilities": [
            {"name": "LCLS", "computepurchases": [
                {"clustername": "ada", "purchased": nodes, "burstNodes": burst_nodes}
            ]}
        ],
        **{
            f"_{w:0>6}": [{"facility": "LCLS", "cluster": "ada", "percentUsed": usage_percent}]
            for w in (windows or [60])
        }
    }


def test_facility_lifecycle_goes_over_blocks_recovers_and_restores_nodes():
    """
    A facility with 256 purchased nodes goes over quota,
    gets its jobs blocked, then recovers and is unblocked with original nodes restored.
    
    This tests the critical workflow:
    - Nodes are extracted from GraphQL (coact-api is the source of truth)
    - SLURM sacctmgr only tracks current hold state (GrpNodes value)
    - When blocking: GrpNodes set to 0
    - When unblocking: GrpNodes restored to purchased amount (from GraphQL)
    """
    facility = "lcls"
    cluster = "ada"
    purchased_nodes = 256
    
    # === PHASE 1: Facility Normal State ===
    # Initial state: facility under quota with purchased nodes
    facility_usage = FacilityUsage(
        username="test_user",
        password_file="/tmp/test",
        windows=[60],
        threshold=100.0,
        dry_run=False
    )
    
    # GraphQL response includes purchasedNodes from Facility.computepurchases
    graphql_response = {
        "repos": [
            {
                "facility": "LCLS",
                "allocs": [
                    {"cluster": "ada", "start": "2026-04-01", "end": "2026-05-01"},
                ]
            }
        ],
        "facilities": [
            {"name": "LCLS", "computepurchases": [
                {"clustername": "ada", "purchased": purchased_nodes, "burstNodes": 0}
            ]}
        ],
        "_000060": [
            {"facility": "LCLS", "cluster": "ada", "percentUsed": 85},
        ]
    }
    
    # sacctmgr shows facility has nodes available (GrpNodes != 0 means not held)
    sacctmgr_normal = b"""lcls:_regular_@ada|256|1000|1000
    """
    
    with patch('modules.coact.subprocess.check_output') as mock_subprocess:
        mock_subprocess.return_value = sacctmgr_normal
        result = facility_usage.format_data(graphql_response)
        
        # Verify initial state: facility is not held and has nodes from GraphQL
        assert result[facility][cluster]["held"] is False
        assert result[facility][cluster]["percentUsed"] == [85.0]
        assert result[facility][cluster]["purchasedNodes"] == purchased_nodes
    
    # === PHASE 2: Facility Goes Over Quota ===
    # Usage exceeds 100%, needs to block jobs
    graphql_response_over = create_graphql_response(105, purchased_nodes)
    
    # Format the over-quota data (including purchasedNodes from GraphQL)
    sacctmgr_normal = b"""lcls:_regular_@ada|256|1000|1000
    """
    with patch('modules.coact.subprocess.check_output') as mock_subprocess:
        mock_subprocess.return_value = sacctmgr_normal
        data_over = facility_usage.format_data(graphql_response_over)
    
    # Now get the OveragePoint through overage()
    overage_points = list(facility_usage.overaged(data_over, threshold=100.0))
    assert len(overage_points) == 1, "one window configured, so one point per facility/cluster"
    
    # Verify the OveragePoint from overage() has purchasedNodes populated
    overage_point = overage_points[0]
    assert overage_point['facility'] == facility
    assert overage_point['cluster'] == cluster
    assert overage_point['over'] is True
    assert overage_point['purchased_nodes'] == purchased_nodes, "OveragePoint should have purchasedNodes from format_data"
    
    # Mock sacctmgr toggle to set nodes to 0
    with patch('modules.coact.subprocess.check_output') as mock_subprocess:
        mock_subprocess.return_value = b"Modified account\n"
        result = toggle_job_blocking(overage_point, execute=True)
        
        # Verify blocking command was issued
        assert result is True
        called_args = mock_subprocess.call_args[0][0]
        assert "GrpTRES=node=0" in called_args  # Jobs blocked
        assert f"name={facility}:_regular_@{cluster}" in called_args
    
    # After blocking, sacctmgr shows GrpNodes=0 (but GraphQL still has purchasedNodes)
    sacctmgr_blocked = b"""lcls:_regular_@ada|0|1000|1000
    """
    
    # Create a fresh GraphQL response for the blocked state
    graphql_response_blocked = create_graphql_response(105, purchased_nodes)
    
    with patch('modules.coact.subprocess.check_output') as mock_subprocess:
        mock_subprocess.return_value = sacctmgr_blocked
        result = facility_usage.format_data(graphql_response_blocked)
        
        # Verify blocked state: held is True (GrpNodes=0), but purchasedNodes preserved from GraphQL
        assert result[facility][cluster]["held"] is True
        assert result[facility][cluster]["percentUsed"] == [105.0]
        assert result[facility][cluster]["purchasedNodes"] == purchased_nodes  # From GraphQL
    
    # === PHASE 3: Facility Recovers Below Quota ===
    # Usage drops back below 100%, needs to unblock
    
    # Create fresh GraphQL response for recovery
    graphql_response_recovered = create_graphql_response(95, purchased_nodes)
    
    # Format the recovered data and check held state
    sacctmgr_blocked_still = b"""lcls:_regular_@ada|0|1000|1000
    """
    with patch('modules.coact.subprocess.check_output') as mock_subprocess:
        mock_subprocess.return_value = sacctmgr_blocked_still
        data_recovered = facility_usage.format_data(graphql_response_recovered)
    
    # Get the recovery OveragePoint through overage()
    recovery_points = list(facility_usage.overaged(data_recovered, threshold=100.0))
    assert len(recovery_points) == 1, "one window configured, so one point per facility/cluster"
    
    recovery_point = recovery_points[0]
    assert recovery_point['facility'] == facility
    assert recovery_point['cluster'] == cluster
    assert recovery_point['over'] is False  # Back under quota
    assert recovery_point['held'] is True   # Still blocked
    assert recovery_point['change'] is True  # Need to unblock
    assert recovery_point['purchased_nodes'] == purchased_nodes, "OveragePoint should have purchasedNodes from coact-api"
    
    with patch('modules.coact.subprocess.check_output') as mock_subprocess:
        mock_subprocess.return_value = b"Modified account\n"
        result = toggle_job_blocking(recovery_point, execute=True)
        
        # Verify unblocking command uses original purchased nodes from coact-api
        assert result is True
        called_args = mock_subprocess.call_args[0][0]
        assert f"GrpTRES=node={purchased_nodes}" in called_args  # CRITICAL: restores 256, not -1
        assert "GrpTRES=node=-1" not in called_args  # NOT unlimited
        assert f"name={facility}:_regular_@{cluster}" in called_args
    
    # Verify final state: sacctmgr shows nodes restored
    sacctmgr_restored = b"""lcls:_regular_@ada|256|1000|1000
    """
    
    # Create fresh GraphQL response for final state
    graphql_response_final = create_graphql_response(95, purchased_nodes)
    
    with patch('modules.coact.subprocess.check_output') as mock_subprocess:
        mock_subprocess.return_value = sacctmgr_restored
        result = facility_usage.format_data(graphql_response_final)
        
        # Verify recovered state: not held and purchasedNodes from GraphQL
        assert result[facility][cluster]["held"] is False
        assert result[facility][cluster]["percentUsed"] == [95.0]
        assert result[facility][cluster]["purchasedNodes"] == purchased_nodes




# --------------------------------------------------------------------------------
# Facility burst nodes: a facility may run burst_nodes above its purchase, which
# raises the overage threshold and the node count restored when a hold is lifted.
# --------------------------------------------------------------------------------

PURCHASED = 256
BURST = 26
# 256 purchased + 26 burst = 282 nodes, which is 110.15625% of the purchase
CEILING_PCT = 110.15625


def make_usage(windows=None, threshold=100.0):
    return FacilityUsage(
        username="test_user",
        password_file="/tmp/test",
        windows=windows or [60],
        threshold=threshold,
        dry_run=False,
    )


def format_with_sacctmgr(facility_usage, response, grpnodes=b"256"):
    """Run format_data with sacctmgr reporting the given GrpNodes for the account."""
    with patch("modules.coact.subprocess.check_output") as mock_subprocess:
        mock_subprocess.return_value = b"lcls:_regular_@ada|" + grpnodes + b"|1000|1000\n"
        return facility_usage.format_data(response)


def single_point(usage_percent, burst_nodes=BURST, purchased=PURCHASED, held_grpnodes=b"256"):
    """Drive one usage figure all the way through format_data + overaged."""
    usage = make_usage()
    data = format_with_sacctmgr(
        usage,
        create_graphql_response(usage_percent, purchased, burst_nodes=burst_nodes),
        grpnodes=held_grpnodes,
    )
    points = list(usage.overaged(data, threshold=100.0))
    assert len(points) == 1
    return points[0]


def written_node_count(point, execute=True):
    """Run toggle_job_blocking and return the node count it wrote to sacctmgr."""
    with patch("modules.coact.subprocess.check_output") as mock_subprocess:
        mock_subprocess.return_value = b"Modified account\n"
        assert toggle_job_blocking(point, execute=execute) is True
        if not execute:
            mock_subprocess.assert_not_called()
            return None
        called_args = mock_subprocess.call_args[0][0]
    for arg in called_args:
        if arg.startswith("GrpTRES=node="):
            return arg.split("=")[-1]
    raise AssertionError(f"no GrpTRES=node= in {called_args}")


def make_point(**overrides):
    point = OveragePoint(
        facility="lcls", cluster="ada", qos="regular", window_mins=60,
        percentages=[95.0], percent_used=95.0, held=True, over=False, change=True,
        purchased_nodes=PURCHASED, burst_nodes=BURST, effective_threshold=CEILING_PCT,
    )
    point.update(overrides)
    return point


class TestEffectiveThreshold:
    """Burst headroom raises the bar rather than changing the measurement."""

    def test_burst_raises_the_threshold_proportionally(self):
        assert FacilityUsage.effective_threshold(100.0, PURCHASED, BURST) == pytest.approx(CEILING_PCT)

    def test_zero_burst_leaves_the_threshold_alone(self):
        assert FacilityUsage.effective_threshold(100.0, PURCHASED, 0) == 100.0

    def test_a_null_burst_is_treated_as_zero(self):
        assert FacilityUsage.effective_threshold(100.0, PURCHASED, None) == 100.0

    def test_a_fractional_purchase_still_scales(self):
        # 100.5 purchased + 10 burst -> 110.5/100.5
        assert FacilityUsage.effective_threshold(100.0, 100.5, 10) == pytest.approx(109.9502487, rel=1e-6)

    def test_burst_larger_than_the_purchase_is_not_capped(self):
        # deliberate: there is no sanity ceiling, so a 2x burst permits 300%
        assert FacilityUsage.effective_threshold(100.0, 100, 200) == 300.0

    def test_a_negative_burst_lowers_the_threshold(self):
        # the mutation rejects negatives; nothing downstream does, so pin the behaviour
        assert FacilityUsage.effective_threshold(100.0, 100, -40) == 60.0

    def test_a_non_positive_or_missing_purchase_has_nothing_to_burst_from(self):
        assert FacilityUsage.effective_threshold(100.0, None, BURST) == 100.0
        assert FacilityUsage.effective_threshold(100.0, 0, BURST) == 100.0
        assert FacilityUsage.effective_threshold(100.0, -1, BURST) == 100.0

    def test_a_non_default_base_threshold_is_scaled_too(self):
        assert FacilityUsage.effective_threshold(90.0, PURCHASED, BURST) == pytest.approx(99.140625)


class TestBurstAccommodatesSporadicOverage:

    def test_a_spike_within_the_burst_is_not_an_overage(self):
        point = single_point(105)
        assert point["over"] is False
        assert point["burst_nodes"] == BURST
        assert point["effective_threshold"] == pytest.approx(CEILING_PCT)

    def test_without_burst_the_same_spike_is_an_overage(self):
        point = single_point(105, burst_nodes=0)
        assert point["over"] is True
        assert point["effective_threshold"] == 100.0

    def test_a_spike_beyond_the_burst_is_still_an_overage(self):
        assert single_point(115)["over"] is True

    def test_usage_exactly_on_the_threshold_counts_as_over(self):
        # pins >= rather than >; nothing else in the suite sits on the boundary
        assert single_point(CEILING_PCT)["over"] is True

    def test_usage_just_under_the_threshold_is_not_over(self):
        assert single_point(CEILING_PCT - 0.01)["over"] is False

    def test_fractional_usage_above_the_threshold_is_not_truncated_away(self):
        # 110.9 sits above the 110.15625 ceiling; truncating it to 110 would let a
        # genuinely over-ceiling facility escape the hold
        assert single_point(110.9)["over"] is True
        assert single_point(110.9)["percent_used"] == pytest.approx(110.9)

    def test_the_same_verdict_is_reported_for_every_window(self):
        # `over` is computed per facility/cluster and stamped onto each window point,
        # so this pins the window labelling and that burst is not window-dependent
        windows = [5, 15, 60, 180, 1440]
        usage = make_usage(windows=windows)
        data = format_with_sacctmgr(
            usage, create_graphql_response(105, PURCHASED, burst_nodes=BURST, windows=windows)
        )
        points = list(usage.overaged(data, threshold=100.0))
        assert [p["window_mins"] for p in points] == windows
        assert all(p["over"] is False for p in points)

    def test_a_facility_with_no_purchase_falls_back_to_the_bare_threshold(self):
        usage = make_usage()
        response = create_graphql_response(105, PURCHASED, burst_nodes=BURST)
        response["facilities"] = [{"name": "LCLS", "computepurchases": []}]
        data = format_with_sacctmgr(usage, response)
        point = list(usage.overaged(data, threshold=100.0))[0]
        assert point["purchased_nodes"] is None
        assert point["effective_threshold"] == 100.0
        assert point["over"] is True

    def test_a_response_without_the_burst_field_is_treated_as_zero_burst(self):
        # rollout ordering: a new daemon against an API that predates burstNodes
        usage = make_usage()
        response = create_graphql_response(105, PURCHASED)
        for purchase in response["facilities"][0]["computepurchases"]:
            del purchase["burstNodes"]
        data = format_with_sacctmgr(usage, response)
        point = list(usage.overaged(data, threshold=100.0))[0]
        assert point["burst_nodes"] == 0
        assert point["effective_threshold"] == 100.0
        assert point["over"] is True


class TestHoldIsLiftedWhenBurstCoversTheUsage:
    """The two halves of the feature, joined: overaged() -> toggle_job_blocking()."""

    def test_a_held_facility_inside_its_burst_is_released_to_the_ceiling(self):
        # sacctmgr reports GrpNodes=0, i.e. the daemon is currently holding it
        point = single_point(105, held_grpnodes=b"0")
        assert (point["held"], point["over"], point["change"]) == (True, False, True)
        assert written_node_count(point) == "282"

    def test_a_held_facility_beyond_its_burst_stays_held(self):
        point = single_point(115, held_grpnodes=b"0")
        assert (point["held"], point["over"], point["change"]) == (True, True, False)

    def test_an_unheld_facility_beyond_its_burst_is_blocked(self):
        point = single_point(115)
        assert (point["held"], point["over"], point["change"]) == (False, True, True)
        assert written_node_count(point) == "0"


class TestReleaseRestoresTheCeiling:

    def test_release_restores_purchase_plus_burst(self):
        assert written_node_count(make_point()) == "282"

    def test_a_float_burst_is_written_as_an_integer(self):
        # burstNodes is a GraphQL Float, so production sends 26.0; without math.ceil
        # the command would read GrpTRES=node=282.0
        assert written_node_count(make_point(burst_nodes=26.0)) == "282"

    def test_a_fractional_ceiling_rounds_up(self):
        assert written_node_count(make_point(burst_nodes=26.5)) == "283"

    def test_release_without_burst_restores_the_bare_purchase(self):
        assert written_node_count(make_point(burst_nodes=0)) == "256"

    def test_release_without_a_purchase_is_unlimited(self):
        assert written_node_count(make_point(purchased_nodes=None)) == "-1"

    def test_release_with_a_zero_purchase_is_unlimited(self):
        assert written_node_count(make_point(purchased_nodes=0)) == "-1"

    def test_release_with_an_unlimited_purchase_stays_unlimited(self):
        assert written_node_count(make_point(purchased_nodes=-1)) == "-1"

    def test_a_hold_zeroes_the_account_regardless_of_burst(self):
        assert written_node_count(make_point(over=True)) == "0"

    def test_the_command_targets_the_facility_regular_account(self):
        with patch("modules.coact.subprocess.check_output") as mock_subprocess:
            mock_subprocess.return_value = b"Modified account\n"
            toggle_job_blocking(make_point(), execute=True)
            called_args = mock_subprocess.call_args[0][0]
        assert "name=lcls:_regular_@ada" in called_args

    def test_dry_run_issues_no_command(self):
        assert written_node_count(make_point(), execute=False) is None

    def test_a_failing_sacctmgr_is_reported(self):
        with patch("modules.coact.subprocess.check_output") as mock_subprocess:
            mock_subprocess.side_effect = subprocess.CalledProcessError(1, "sacctmgr")
            assert toggle_job_blocking(make_point(), execute=True) is False


class TestInfluxLine:

    def test_burst_and_threshold_are_emitted_as_float_fields(self):
        line = influx_line(make_point())
        tags, fields = line.split(" ", 1)
        assert tags == "allocation_usage,facility=lcls,cluster=ada,qos=regular,window_mins=60"
        assert "burst_nodes=26.0" in fields
        assert "effective_threshold=110.15625" in fields

    def test_a_missing_burst_is_emitted_as_zero_not_omitted(self):
        fields = influx_line(make_point(burst_nodes=None, effective_threshold=None)).split(" ", 1)[1]
        assert "burst_nodes=0.0" in fields
        assert "effective_threshold=0.0" in fields

    def test_a_missing_purchase_is_emitted_as_zero(self):
        assert "purchased_nodes=0.0" in influx_line(make_point(purchased_nodes=None))

    def test_booleans_are_lowercased_for_line_protocol(self):
        fields = influx_line(make_point(held=True, over=False, change=True)).split(" ", 1)[1]
        assert "held=true" in fields and "over=false" in fields and "change=true" in fields


class TestUsageQuery:
    """The daemon's own GraphQL query text, which the mocked-response tests cannot see."""

    def _executed_query(self, usage):
        usage.back_channel = Mock()
        usage.back_channel.execute.return_value = {
            "repos": [],
            "facilities": [],
            "_000060": [],
        }
        with patch("modules.coact.subprocess.check_output") as mock_subprocess:
            mock_subprocess.return_value = b""
            usage.get_data()
        return print_ast(usage.back_channel.execute.call_args[0][0])

    def test_the_usage_query_requests_the_facility_burst(self):
        query = self._executed_query(make_usage())
        assert "burstNodes" in query, "the overage daemon must ask the API for burst headroom"
        assert "purchased" in query

    def test_one_usage_alias_is_requested_per_configured_window(self):
        query = self._executed_query(make_usage(windows=[5, 60, 1440]))
        for minutes in (5, 60, 1440):
            assert f"pastMinutes: {minutes}" in query
