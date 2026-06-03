"""
Unit tests for node allocation functionality.
"""

from unittest.mock import patch, MagicMock

from modules.coact import toggle_job_blocking, FacilityUsage


def create_graphql_response(usage_percent: float, nodes: int):
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
            {"name": "LCLS", "computepurchases": [{"clustername": "ada", "purchased": nodes}]}
        ],
        "000060": [
            {"facility": "LCLS", "cluster": "ada", "percentUsed": usage_percent},
        ]
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
            {"name": "LCLS", "computepurchases": [{"clustername": "ada", "purchased": purchased_nodes}]}
        ],
        "000060": [
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
        assert result[facility][cluster]["percentUsed"] == [85]
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
    assert len(overage_points) > 0, "No overage points generated"
    
    # Verify the OveragePoint from overage() has purchasedNodes populated
    overage_point = overage_points[0]
    assert overage_point['facility'] == facility
    assert overage_point['cluster'] == cluster
    assert overage_point['over'] is True
    assert overage_point['purchased_nodes'] == purchased_nodes, "OveragePoint should have purchasedNodes from format_data"
    
    # Mock sacctmgr toggle to set nodes to 0
    with patch('modules.coact.AnsibleRunner.run_playbook') as mock_run:
        mock_run.return_value = MagicMock(rc=0, stats={})
        result = toggle_job_blocking(overage_point, execute=True)

        # Verify blocking command was issued
        assert result is True
        call_kwargs = mock_run.call_args.kwargs
        assert call_kwargs['nodes'] == 0
        assert call_kwargs['facility'] == facility
        assert call_kwargs['cluster'] == cluster
    
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
        assert result[facility][cluster]["percentUsed"] == [105]
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
    assert len(recovery_points) > 0, "No recovery points generated"
    
    recovery_point = recovery_points[0]
    assert recovery_point['facility'] == facility
    assert recovery_point['cluster'] == cluster
    assert recovery_point['over'] is False  # Back under quota
    assert recovery_point['held'] is True   # Still blocked
    assert recovery_point['change'] is True  # Need to unblock
    assert recovery_point['purchased_nodes'] == purchased_nodes, "OveragePoint should have purchasedNodes from coact-api"
    
    with patch('modules.coact.AnsibleRunner.run_playbook') as mock_run:
        mock_run.return_value = MagicMock(rc=0, stats={})
        result = toggle_job_blocking(recovery_point, execute=True)

        # Verify unblocking command uses original purchased nodes from coact-api
        assert result is True
        call_kwargs = mock_run.call_args.kwargs
        assert call_kwargs['nodes'] == purchased_nodes  # CRITICAL: restores 256, not -1
        assert call_kwargs['nodes'] != -1               # NOT unlimited
        assert call_kwargs['facility'] == facility
        assert call_kwargs['cluster'] == cluster
    
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
        assert result[facility][cluster]["percentUsed"] == [95]
        assert result[facility][cluster]["purchasedNodes"] == purchased_nodes


