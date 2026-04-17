"""
Unit tests for node allocation functionality.
"""

from unittest.mock import patch

from modules.coact import toggle_job_blocking, OveragePoint, FacilityUsage


def test_facility_lifecycle_goes_over_blocks_recovers_and_restores_nodes():
    """
    A facility with 256 purchased nodes goes over quota,
    gets its jobs blocked, then recovers and is unblocked with original nodes restored.
    
    This tests the critical workflow:
    - Nodes are extracted from sacctmgr
    - When blocking: GrpNodes set to 0
    - When unblocking: GrpNodes restored to purchased amount
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
    
    graphql_response = {
        "repos": [
            {
                "facility": "LCLS",
                "allocs": [
                    {"cluster": "ada", "start": "2026-04-01", "end": "2026-05-01"},
                ]
            }
        ],
        "000060": [
            {"facility": "LCLS", "cluster": "ada", "percentUsed": 85},
        ]
    }
    
    # sacctmgr shows facility has 256 nodes available
    sacctmgr_normal = b"""lcls:_regular_@ada|256|1000|1000
    """
    
    with patch('modules.coact.subprocess.check_output') as mock_subprocess:
        mock_subprocess.return_value = sacctmgr_normal
        result = facility_usage.format_data(graphql_response)
        
        # Verify initial state: facility is not held and has nodes available
        assert result[facility][cluster]["held"] is False
        assert result[facility][cluster]["percentUsed"] == [85]
        assert result[facility][cluster]["purchasedNodes"] == purchased_nodes
    
    # === PHASE 2: Facility Goes Over Quota ===
    # Usage exceeds 100%, needs to block jobs
    graphql_response_over = {
        "repos": [
            {
                "facility": "LCLS",
                "allocs": [
                    {"cluster": "ada", "start": "2026-04-01", "end": "2026-05-01"},
                ]
            }
        ],
        "000060": [
            {"facility": "LCLS", "cluster": "ada", "percentUsed": 105},
        ]
    }
    
    # Create overage point for blocking
    overage_point = OveragePoint(
        facility=facility,
        cluster=cluster,
        qos="regular",
        window_mins=60,
        percentages=[105.0],
        percent_used=105.0,
        held=False,  # Not yet blocked
        over=True,   # Over quota
        change=True  # Need to block
    )
    overage_point['purchased_nodes'] = purchased_nodes
    
    # Mock sacctmgr toggle to set nodes to 0
    with patch('modules.coact.subprocess.check_output') as mock_subprocess:
        mock_subprocess.return_value = b"Modified account\n"
        result = toggle_job_blocking(overage_point, execute=True)
        
        # Verify blocking command was issued
        assert result is True
        called_args = mock_subprocess.call_args[0][0]
        assert "GrpTRES=node=0" in called_args  # Jobs blocked
        assert f"name={facility}:_regular_@{cluster}" in called_args
    
    # After blocking, sacctmgr now shows GrpNodes=0
    sacctmgr_blocked = b"""lcls:_regular_@ada|0|1000|1000
    """
    
    with patch('modules.coact.subprocess.check_output') as mock_subprocess:
        mock_subprocess.return_value = sacctmgr_blocked
        result = facility_usage.format_data(graphql_response_over)
        
        # Verify blocked state
        assert result[facility][cluster]["held"] is True
        assert result[facility][cluster]["percentUsed"] == [105]
        # Note: purchasedNodes not set when GrpNodes=0 (only non-zero values stored)
    
    # === PHASE 3: Facility Recovers Below Quota ===
    # Usage drops back below 100%, needs to unblock
    graphql_response_recovered = {
        "repos": [
            {
                "facility": "LCLS",
                "allocs": [
                    {"cluster": "ada", "start": "2026-04-01", "end": "2026-05-01"},
                ]
            }
        ],
        "000060": [
            {"facility": "LCLS", "cluster": "ada", "percentUsed": 95},
        ]
    }
    
    # CRITICAL: Must restore with original purchased_nodes, not unlimited
    recovery_point = OveragePoint(
        facility=facility,
        cluster=cluster,
        qos="regular",
        window_mins=60,
        percentages=[95.0],
        percent_used=95.0,
        held=True,   # Currently blocked
        over=False,  # Back under quota
        change=True  # Need to unblock
    )
    recovery_point['purchased_nodes'] = purchased_nodes  # Restored from sacctmgr
    
    with patch('modules.coact.subprocess.check_output') as mock_subprocess:
        mock_subprocess.return_value = b"Modified account\n"
        result = toggle_job_blocking(recovery_point, execute=True)
        
        # Verify unblocking command uses original purchased nodes
        assert result is True
        called_args = mock_subprocess.call_args[0][0]
        assert f"GrpTRES=node={purchased_nodes}" in called_args  # CRITICAL: restores 256, not -1
        assert "GrpTRES=node=-1" not in called_args  # NOT unlimited
        assert f"name={facility}:_regular_@{cluster}" in called_args
    
    # Verify final state: sacctmgr shows nodes restored
    sacctmgr_restored = b"""lcls:_regular_@ada|256|1000|1000
    """
    
    with patch('modules.coact.subprocess.check_output') as mock_subprocess:
        mock_subprocess.return_value = sacctmgr_restored
        result = facility_usage.format_data(graphql_response_recovered)
        
        # Verify recovered state
        assert result[facility][cluster]["held"] is False
        assert result[facility][cluster]["percentUsed"] == [95]
        assert result[facility][cluster]["purchasedNodes"] == purchased_nodes


