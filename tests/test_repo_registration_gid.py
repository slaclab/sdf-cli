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
        # Mock extract_grouper_values to return the GID
        repo_registration.extract_grouper_values = Mock(return_value=('12345', 'sdf-cryoem-ct-test'))
        repo_registration.back_channel.execute.side_effect = [
            {'repo': None},  # Query returns null (repo not found)
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
        """Test handling when grouper playbook returns empty ansible_facts (no GID found)."""
        # Setup
        repo_registration.run_playbook.return_value = mock_ansible_runner
        # Mock extract_grouper_values to return None for GID (empty facts scenario)
        repo_registration.extract_grouper_values = Mock(return_value=(None, 'sdf-cryoem-ct-repo'))
        repo_registration.back_channel.execute.side_effect = [
            {'repoUpsert': {'Id': 'repo-123'}},
            {'repoUpsertFeature': {'Id': 'feature-slurm'}}
        ]

        # Execute — repo starts with 'ct' to trigger grouper
        # This should now raise a RuntimeError when GID is None
        with pytest.raises(RuntimeError, match="Unable to fetch gid from grouper"):
            repo_registration.do_new_repo(
                repo='ct-repo',
                facility='cryoem',
                principal='test-user'
            )

        # Verify the exception was logged by the outer exception handler
        repo_registration.logger.warning.assert_called_with(
            "Failed to create grouper POSIX group for cryoem:ct-repo: Unable to fetch gid from grouper."
        )

    def test_non_grouper_facility_skips_gid(self, repo_registration, mock_ansible_runner):
        """Test that non-cryoem facilities skip grouper entirely."""
        # Setup
        repo_registration.run_playbook.return_value = mock_ansible_runner
        repo_registration.back_channel.execute.side_effect = [
            {'repo': None},  # Query returns null (repo not found)
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
        # The new implementation passes repo_principal, repo_users, gidNumber=None, and groupName=''
        repo_registration.run_playbook.assert_called_once_with(
            'coact/add_repo.yaml',
            facility='OTHER',
            repo='test-repo',
            repo_principal='test-user',
            repo_users=['test-user'],  # New parameter added by idempotency fix
            gidNumber=None,
            groupName=''
        )
        # extract_grouper_values should not be called for non-grouper facilities
        if hasattr(repo_registration, 'extract_grouper_values') and isinstance(repo_registration.extract_grouper_values, Mock):
            repo_registration.extract_grouper_values.assert_not_called()

        # Should only create slurm feature (3 back_channel calls: user query + repoUpsert + feature)
        assert repo_registration.back_channel.execute.call_count == 3

    def test_all_cryoem_repos_use_grouper(self, repo_registration, mock_ansible_runner):
        """Test that ALL cryoem repos (not just ct/ce) unconditionally use grouper."""
        # Setup
        repo_registration.run_playbook.return_value = mock_ansible_runner
        # Mock extract_grouper_values to return a valid GID for any CryoEM repo
        repo_registration.extract_grouper_values = Mock(return_value=('54321', 'sdf-cryoem-other-repo'))
        repo_registration.back_channel.execute.side_effect = [
            {'repo': None},  # Query returns null (repo not found)
            {'repoUpsert': {'Id': 'repo-123'}},
            {'repoUpsertFeature': {'Id': 'feature-slurm'}},
            {'repoUpsertFeature': {'Id': 'feature-posix'}}
        ]

        # Execute — repo does not start with ct or ce, but should still use grouper
        result = repo_registration.do_new_repo(
            repo='other-repo',
            facility='cryoem',
            principal='test-user'
        )

        # Verify
        assert result is True
        # grouper.yml should be called for ALL CryoEM repos
        repo_registration.run_playbook.assert_any_call(
            'coact/grouper.yml',
            grouper_name='sdf-cryoem-other-repo',
            state='present',
            grouper_description='POSIX group for cryoem other-repo repository access',
            grouper_password_file='/tmp/test-grouper-password',
        )
        # extract_grouper_values SHOULD be called for all CryoEM repos
        repo_registration.extract_grouper_values.assert_called_once()
        # Verify GID was logged
        repo_registration.logger.info.assert_any_call(
            "Retrieved repo GID for cryoem:other-repo: 54321"
        )
        # Should create both slurm and posixgroup features (4 back_channel calls: user query + repoUpsert + 2 features)
        assert repo_registration.back_channel.execute.call_count == 4


class TestRepoIdempotency:
    """Test idempotency fixes for NewRepo request workflow."""

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
            reg.extract_grouper_values = Mock(return_value=('12345', 'sdf-cryoem-test-repo'))
            return reg

    @pytest.fixture
    def mock_ansible_runner(self) -> Mock:
        """Mock ansible runner with realistic structure."""
        runner = Mock()
        runner.events = []
        return runner

    def test_fresh_newrepo_uses_principal_only(self, repo_registration: RepoRegistration, mock_ansible_runner: Mock):
        """
        When creating a brand new repo that doesn't exist in the database,
        the playbook should receive only the principal user.
        """
        # Setup - repo doesn't exist in database
        repo_registration.run_playbook.return_value = mock_ansible_runner
        repo_registration.back_channel.execute.side_effect = [
            {'repo': None},  # Normal "not found"
            {'repoUpsert': {'Id': 'repo-123'}},  # Repo creation succeeds
            {'repoUpsertFeature': {'Id': 'feature-slurm'}},
            {'repoUpsertFeature': {'Id': 'feature-posix'}}
        ]

        # Execute
        result = repo_registration.do_new_repo(
            repo='test-new-1',
            facility='cryoem',
            principal='user1'
        )

        # Verify
        assert result is True

        # Check that the playbook was called with only the principal
        playbook_call = [c for c in repo_registration.run_playbook.call_args_list
                         if c[0][0] == 'coact/add_repo.yaml'][0]
        assert playbook_call[1]['repo_users'] == ['user1']
        assert playbook_call[1]['repo_principal'] == 'user1'

        # Verify logging indicates repo not found
        log_messages = [str(c) for c in repo_registration.logger.info.call_args_list]
        assert any('not found in database' in msg for msg in log_messages)

    def test_rerun_newrepo_preserves_existing_users_and_leaders(self, repo_registration: RepoRegistration, mock_ansible_runner: Mock):
        """
        Test complete idempotency: re-running NewRepo on an existing repo must preserve
        both users and leaders lists, not reset them to just the principal.
        """
        # Setup - repo exists with 3 users and 2 leaders
        repo_registration.run_playbook.return_value = mock_ansible_runner
        repo_registration.back_channel.execute.side_effect = [
            {
                'repo': {
                    'users': ['user1', 'user2', 'user3'],
                    'leaders': ['user1', 'leader2']  # Multiple leaders
                }
            },
            {'repoUpsert': {'Id': 'repo-123'}},
            {'repoUpsertFeature': {'Id': 'feature-slurm'}},
            {'repoUpsertFeature': {'Id': 'feature-posix'}}
        ]

        # Execute - re-run NewRepo for existing repo
        result = repo_registration.do_new_repo(
            repo='test-existing',
            facility='cryoem',
            principal='user1'
        )

        # Verify result
        assert result is True

        # Verify Ansible playbook received ALL existing users (for LDAP sync)
        playbook_call = [c for c in repo_registration.run_playbook.call_args_list
                         if c[0][0] == 'coact/add_repo.yaml'][0]
        assert set(playbook_call[1]['repo_users']) == {'user1', 'user2', 'user3'}, \
            "Ansible playbook must receive all users for LDAP sync"

        # The second back_channel.execute call is the repoUpsert mutation
        upsert_call = repo_registration.back_channel.execute.call_args_list[1]
        upsert_data = upsert_call[0][1]  # Second argument is the variables

        assert set(upsert_data['repo']['users']) == {'user1', 'user2', 'user3'}, \
            f"Database write must preserve all users, not reset to principal! Got: {upsert_data['repo']['users']}"

        assert set(upsert_data['repo']['leaders']) == {'user1', 'leader2'}, \
            f"Database write must preserve all leaders, not reset to principal! Got: {upsert_data['repo']['leaders']}"

        # Verify logging indicates existing data was found
        log_messages = [str(c) for c in repo_registration.logger.info.call_args_list]
        assert any('Found existing repo' in msg and '3 users' in msg for msg in log_messages), \
            "Should log that existing users were found"
        assert any('2 leaders' in msg for msg in log_messages), \
            "Should log that existing leaders were found"
