"""
Unit tests for repository registration GID/group name handling.

Tests the new functionality in RepoRegistration.do_new_repo() that extracts
GID information from Ansible playbook results and creates posixgroup features
for facilities that use grouper (currently only CryoEM).
"""

import sys
from unittest.mock import Mock, patch
import pytest

# Mock ansible_runner module to avoid all sdf-ansible dependencies
sys.modules['ansible_runner'] = Mock()

from modules.coactd import RepoRegistration


class TestRepoRegistrationGID:
    """Test GID/group name handling in repository registration."""

    @pytest.fixture
    def repo_registration(self) -> RepoRegistration:
        """Create a RepoRegistration instance with mocked dependencies."""
        with patch('modules.coactd.GraphQlSubscriber.__init__'), \
             patch('modules.coactd.AnsibleRunner.__init__'):
            reg = RepoRegistration(
                username='test-user',
                password_file='/tmp/test-password',
                client_name='test-client'
            )
            reg.logger = Mock()
            reg.back_channel = Mock()
            reg.run_playbook = Mock()
            reg.playbook_task_res = Mock()
            return reg

    @pytest.fixture
    def mock_ansible_runner(self) -> Mock:
        """Mock ansible runner with realistic structure."""
        runner = Mock()
        runner.events = []
        return runner

    @pytest.fixture
    def sample_gid_facts(self) -> dict:
        """Sample ansible facts with GID information."""
        return {
            'ansible_facts': {
                'repo_gid': '12345',
                'repo_group_name': 'test-repo-group'
            }
        }

    def test_gid_extraction_success_cryoem(self, repo_registration: RepoRegistration, mock_ansible_runner: Mock, sample_gid_facts: dict):
        """Test successful GID extraction for CryoEM facility."""
        # Setup
        repo_registration.run_playbook.return_value = mock_ansible_runner
        repo_registration.playbook_task_res.return_value = sample_gid_facts
        repo_registration.back_channel.execute.side_effect = [
            {'repoUpsert': {'Id': 'repo-456'}},
            {'repoUpsertFeature': {'Id': 'feature-slurm'}},
            {'repoUpsertFeature': {'Id': 'feature-posix'}}
        ]

        # Execute
        result = repo_registration.do_new_repo(
            repo='cryo-test',
            facility='cryoem',  # Test case-insensitive matching
            principal='cryo-user'
        )

        # Verify
        assert result is True
        repo_registration.logger.info.assert_any_call(
            "Retrieved repo GID for cryoem:cryo-test: 12345"
        )

    def test_gid_extraction_empty_facts(self, repo_registration: RepoRegistration, mock_ansible_runner: Mock):
        """Test handling when ansible facts are empty (triggers KeyError)."""
        # Setup
        repo_registration.run_playbook.return_value = mock_ansible_runner
        repo_registration.playbook_task_res.return_value = {'ansible_facts': {}}  # Empty facts
        repo_registration.back_channel.execute.side_effect = [
            {'repoUpsert': {'Id': 'repo-123'}},
            {'repoUpsertFeature': {'Id': 'feature-slurm'}}
        ]

        # Execute
        result = repo_registration.do_new_repo(
            repo='test-repo',
            facility='cryoem',
            principal='test-user'
        )

        # Verify
        assert result is True

        # Should log warning about extraction failure (KeyError when accessing repo_gid)
        repo_registration.logger.warning.assert_called_with(
            "Failed to extract repo GID for cryoem:test-repo: 'repo_gid'"
        )

    def test_non_grouper_facility_skips_gid(self, repo_registration, mock_ansible_runner):
        """Test that non-grouper facilities skip GID extraction."""
        # Setup
        repo_registration.run_playbook.return_value = mock_ansible_runner
        repo_registration.back_channel.execute.side_effect = [
            {'repoUpsert': {'Id': 'repo-123'}},
            {'repoUpsertFeature': {'Id': 'feature-slurm'}}
        ]

        # Execute
        result = repo_registration.do_new_repo(
            repo='test-repo',
            facility='OTHER',  # Not CryoEM
            principal='test-user'
        )

        # Verify
        assert result is True

        # playbook_task_res should NOT be called
        repo_registration.playbook_task_res.assert_not_called()

        # Should only create slurm feature
        assert repo_registration.back_channel.execute.call_count == 2
