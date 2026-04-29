"""
Unit tests for node allocation functionality.
"""

from unittest.mock import patch
import os

from modules.coact import toggle_job_blocking, FacilityUsage

# Import the OpenAPI client models for proper typing
from openapi_client.models.v0044_openapi_assocs_resp import V0044OpenapiAssocsResp
from openapi_client.models.v0044_assoc import V0044Assoc
from openapi_client.models.v0044_assoc_max import V0044AssocMax
from openapi_client.models.v0044_assoc_max_tres import V0044AssocMaxTres
from openapi_client.models.v0044_assoc_max_tres_per import V0044AssocMaxTresPer
from openapi_client.models.v0044_tres import V0044Tres
from openapi_client.models.v0044_assoc_max_jobs import V0044AssocMaxJobs
from openapi_client.models.v0044_uint32_no_val_struct import V0044Uint32NoValStruct


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
        "000060": [
            {"facility": "LCLS", "cluster": "ada", "percentUsed": usage_percent, "purchasedNodes": nodes},
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
    # Setup JWT token for REST client
    os.environ["SLURM_JWT"] = "test_token"

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

    # GraphQL response includes purchasedNodes from coact-api
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
            {"facility": "LCLS", "cluster": "ada", "percentUsed": 85, "purchasedNodes": purchased_nodes},
        ]
    }

    # Mock the REST API client to return association data that indicates not held
    def create_mock_associations_response(grp_nodes_value):
        """Create OpenAPI association response for testing."""
        mock_assoc = V0044Assoc(
            account="lcls:_regular_@ada",
            user="lcls",
            max=V0044AssocMax(
                tres=V0044AssocMaxTres(
                    per=V0044AssocMaxTresPer(
                        job=[V0044Tres(type="node", count=grp_nodes_value)]
                    )
                ),
                jobs=V0044AssocMaxJobs(
                    total=V0044Uint32NoValStruct(set=True, number=1000)
                )
            )
        )

        return V0044OpenapiAssocsResp(associations=[mock_assoc])

    with patch.object(facility_usage.slurm_client, 'get_associations') as mock_get_assoc:
        mock_get_assoc.return_value = create_mock_associations_response(256)  # Normal state
        result = facility_usage.format_data(graphql_response)
        
        # Verify initial state: facility is not held and has nodes from GraphQL
        assert result[facility][cluster]["held"] is False
        assert result[facility][cluster]["percentUsed"] == [85]
        assert result[facility][cluster]["purchasedNodes"] == purchased_nodes
    
    # === PHASE 2: Facility Goes Over Quota ===
    # Usage exceeds 100%, needs to block jobs
    graphql_response_over = create_graphql_response(105, purchased_nodes)
    
    # Format the over-quota data (including purchasedNodes from GraphQL)
    with patch.object(facility_usage.slurm_client, 'get_associations') as mock_get_assoc:
        mock_get_assoc.return_value = create_mock_associations_response(256)  # Still normal state
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
    with patch('modules.coact.subprocess.check_output') as mock_subprocess:
        mock_subprocess.return_value = b"Modified account\n"
        result = toggle_job_blocking(overage_point, execute=True)
        
        # Verify blocking command was issued
        assert result is True
        called_args = mock_subprocess.call_args[0][0]
        assert "GrpTRES=node=0" in called_args  # Jobs blocked
        assert f"name={facility}:_regular_@{cluster}" in called_args
    
    # After blocking, sacctmgr shows GrpNodes=0 (but GraphQL still has purchasedNodes)
    # Create a fresh GraphQL response for the blocked state
    graphql_response_blocked = create_graphql_response(105, purchased_nodes)

    with patch.object(facility_usage.slurm_client, 'get_associations') as mock_get_assoc:
        mock_get_assoc.return_value = create_mock_associations_response(0)  # Blocked state
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
    with patch.object(facility_usage.slurm_client, 'get_associations') as mock_get_assoc:
        mock_get_assoc.return_value = create_mock_associations_response(0)  # Still blocked
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
    # Create fresh GraphQL response for final state
    graphql_response_final = create_graphql_response(95, purchased_nodes)

    with patch.object(facility_usage.slurm_client, 'get_associations') as mock_get_assoc:
        mock_get_assoc.return_value = create_mock_associations_response(256)  # Restored state
        result = facility_usage.format_data(graphql_response_final)
        
        # Verify recovered state: not held and purchasedNodes from GraphQL
        assert result[facility][cluster]["held"] is False
        assert result[facility][cluster]["percentUsed"] == [95]
        assert result[facility][cluster]["purchasedNodes"] == purchased_nodes

    # Clean up JWT token
    if "SLURM_JWT" in os.environ:
        del os.environ["SLURM_JWT"]


