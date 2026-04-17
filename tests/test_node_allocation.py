"""
Unit tests for node allocation functionality.
Tests the core logic without requiring external dependencies.
"""

from unittest.mock import patch
import subprocess

from modules.coact import toggle_job_blocking, OveragePoint


def test_toggle_job_blocking_uses_purchased_nodes_when_unblocking():
    """Test that toggle_job_blocking uses purchased_nodes when unblocking."""
    point = OveragePoint(
        facility="test_facility",
        cluster="test_cluster",
        qos="regular",
        window_mins=5,
        percentages=[50.0],
        percent_used=50.0,
        held=True,
        over=False,  # Unblocking
        change=True,
        purchased_nodes=100  # Should use this value
    )

    # Mock the subprocess call to capture the command
    with patch('modules.coact.subprocess.check_output') as mock_subprocess:
        mock_subprocess.return_value = b"Modified account\n"

        result = toggle_job_blocking(point, execute=True)

        assert result is True
        mock_subprocess.assert_called_once()

        # Verify the command uses purchased nodes (100) instead of unlimited (-1)
        called_args = mock_subprocess.call_args[0][0]  # First positional arg (the command)
        assert "GrpTRES=node=100" in called_args
        assert "name=test_facility:_regular_@test_cluster" in called_args


def test_toggle_job_blocking_fallback_to_unlimited():
    """Test fallback to unlimited when no purchased_nodes available."""
    point = OveragePoint(
        facility="test_facility",
        cluster="test_cluster",
        qos="regular",
        window_mins=5,
        percentages=[50.0],
        percent_used=50.0,
        held=True,
        over=False,  # Unblocking
        change=True,
        purchased_nodes=None  # No purchased nodes data
    )

    with patch('modules.coact.subprocess.check_output') as mock_subprocess:
        mock_subprocess.return_value = b"Modified account\n"

        result = toggle_job_blocking(point, execute=True)

        assert result is True
        # Should fall back to unlimited (-1)
        called_args = mock_subprocess.call_args[0][0]
        assert "GrpTRES=node=-1" in called_args


def test_toggle_job_blocking_blocks_with_zero():
    """Test that blocking still sets nodes to 0 (unchanged behavior)."""
    point = OveragePoint(
        facility="test_facility",
        cluster="test_cluster",
        qos="regular",
        window_mins=5,
        percentages=[150.0],
        percent_used=150.0,
        held=False,
        over=True,  # Blocking
        change=True,
        purchased_nodes=100  # Should be ignored when blocking
    )

    with patch('modules.coact.subprocess.check_output') as mock_subprocess:
        mock_subprocess.return_value = b"Modified account\n"

        result = toggle_job_blocking(point, execute=True)

        assert result is True
        # Should use 0 for blocking, regardless of purchased_nodes
        called_args = mock_subprocess.call_args[0][0]
        assert "GrpTRES=node=0" in called_args


def test_toggle_job_blocking_dry_run_mode():
    """Test that dry run mode doesn't execute commands."""
    point = OveragePoint(
        facility="test_facility",
        cluster="test_cluster",
        qos="regular",
        window_mins=5,
        percentages=[50.0],
        percent_used=50.0,
        held=True,
        over=False,
        change=True,
        purchased_nodes=100
    )

    with patch('modules.coact.subprocess.check_output') as mock_subprocess:
        result = toggle_job_blocking(point, execute=False)

        assert result is True
        # Should not call subprocess in dry run
        mock_subprocess.assert_not_called()


def test_toggle_job_blocking_handles_invalid_purchased_nodes():
    """Test handling of invalid purchased_nodes values."""
    point = OveragePoint(
        facility="test_facility",
        cluster="test_cluster",
        qos="regular",
        window_mins=5,
        percentages=[50.0],
        percent_used=50.0,
        held=True,
        over=False,  # Unblocking
        change=True,
        purchased_nodes=0  # Invalid: zero nodes
    )

    with patch('modules.coact.subprocess.check_output') as mock_subprocess:
        mock_subprocess.return_value = b"Modified account\n"

        result = toggle_job_blocking(point, execute=True)

        assert result is True
        # Should fall back to unlimited (-1) for invalid node count
        called_args = mock_subprocess.call_args[0][0]
        assert "GrpTRES=node=-1" in called_args


def test_toggle_job_blocking_subprocess_called_process_error_returns_false():
    """Test that subprocess CalledProcessError returns False without raising."""
    point = OveragePoint(
        facility="test_facility",
        cluster="test_cluster",
        qos="regular",
        window_mins=5,
        percentages=[50.0],
        percent_used=50.0,
        held=True,
        over=False,
        change=True,
        purchased_nodes=100
    )

    with patch('modules.coact.subprocess.check_output') as mock_subprocess:
        # Simulate sacctmgr failure mode handled by implementation
        mock_subprocess.side_effect = subprocess.CalledProcessError(1, ["sacctmgr"])

        result = toggle_job_blocking(point, execute=True)

        assert result is False