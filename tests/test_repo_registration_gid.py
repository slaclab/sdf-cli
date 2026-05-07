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
                client_name='test-client',
                grouper_password_file='/tmp/test-grouper-password'
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
        """Sample ansible facts returned by grouper.yml 'Export grouper params' task."""
        return {
            'ansible_facts': {
                'gid': '12345',
            }
        }

    def test_gid_extraction_success_cryoem(self, repo_registration: RepoRegistration, mock_ansible_runner: Mock, sample_gid_facts: dict):
        """Test successful GID extraction for CryoEM facility (ct* repo triggers grouper)."""
        # Setup — run_playbook called twice: add_repo.yaml then grouper.yml
        repo_registration.run_playbook.return_value = mock_ansible_runner
        repo_registration.playbook_task_res.return_value = sample_gid_facts
        repo_registration.back_channel.execute.side_effect = [
            {'repoUpsert': {'Id': 'repo-456'}},
            {'repoUpsertFeature': {'Id': 'feature-slurm'}},
            {'repoUpsertFeature': {'Id': 'feature-posix'}}
        ]

        # Execute — repo starts with 'ct' to satisfy grouper condition
        result = repo_registration.do_new_repo(
            repo='ct-test',
            facility='cryoem',
            principal='cryo-user'
        )

        # Verify
        assert result is True
        # grouper.yml should have been invoked
        repo_registration.run_playbook.assert_any_call(
            'coact/grouper.yml',
            grouper_name='sdf-cryoem-ct-test',
            state='present',
            grouper_description='POSIX group for cryoem ct-test repository access',
            grouper_password_file='/tmp/test-grouper-password',
        )
        repo_registration.logger.info.assert_any_call(
            "Retrieved repo GID for cryoem:ct-test: 12345"
        )

    def test_gid_extraction_empty_facts(self, repo_registration: RepoRegistration, mock_ansible_runner: Mock):
        """Test handling when grouper playbook returns empty ansible_facts (triggers KeyError)."""
        # Setup
        repo_registration.run_playbook.return_value = mock_ansible_runner
        repo_registration.playbook_task_res.return_value = {'ansible_facts': {}}  # Empty facts — KeyError on 'gid'
        repo_registration.back_channel.execute.side_effect = [
            {'repoUpsert': {'Id': 'repo-123'}},
            {'repoUpsertFeature': {'Id': 'feature-slurm'}}
        ]

        # Execute — repo starts with 'ct' to trigger grouper
        result = repo_registration.do_new_repo(
            repo='ct-repo',
            facility='cryoem',
            principal='test-user'
        )

        # Verify
        assert result is True

        # KeyError on 'gid' is caught and logged as warning
        repo_registration.logger.warning.assert_called_with(
            "Failed to create grouper POSIX group for cryoem:ct-repo: 'gid'"
        )

    def test_non_grouper_facility_skips_gid(self, repo_registration, mock_ansible_runner):
        """Test that non-cryoem facilities skip grouper entirely."""
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

        # Only add_repo.yaml should run — grouper.yml should NOT be called
        repo_registration.run_playbook.assert_called_once_with(
            'coact/add_repo.yaml', facility='OTHER', repo='test-repo'
        )
        repo_registration.playbook_task_res.assert_not_called()

        # Should only create slurm feature (2 back_channel calls: repoUpsert + feature)
        assert repo_registration.back_channel.execute.call_count == 2

    def test_cryoem_non_ct_ce_repo_skips_grouper(self, repo_registration, mock_ansible_runner):
        """Test that cryoem repos not starting with ct/ce skip grouper."""
        # Setup
        repo_registration.run_playbook.return_value = mock_ansible_runner
        repo_registration.back_channel.execute.side_effect = [
            {'repoUpsert': {'Id': 'repo-123'}},
            {'repoUpsertFeature': {'Id': 'feature-slurm'}}
        ]

        # Execute — repo does not start with ct or ce
        result = repo_registration.do_new_repo(
            repo='other-repo',
            facility='cryoem',
            principal='test-user'
        )

        # Verify
        assert result is True
        repo_registration.run_playbook.assert_called_once_with(
            'coact/add_repo.yaml', facility='cryoem', repo='other-repo'
        )
        repo_registration.playbook_task_res.assert_not_called()
