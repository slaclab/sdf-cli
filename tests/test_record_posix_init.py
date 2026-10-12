"""
Tests for Registration.record_posix_init and its call in UserRegistration.do_new_user: after the user is upserted
into coact, coactd asks coact-api to initialise the user's posix data from user-lookup. It must never raise
(registration still completes) and must run before repo membership changes can $addToSet on top of it.
"""

import sys
from unittest.mock import Mock

import pytest

sys.modules['ansible_runner'] = Mock()

from modules.coactd import Registration, UserRegistration  # noqa: E402


def make_registration():
    r = Registration.__new__(Registration)
    r.logger = Mock()
    r.back_channel = Mock()
    return r


class TestRecordPosixInit:

    def test_sends_username(self):
        r = make_registration()
        r.back_channel.execute.return_value = {'userPosixInit': {'primaryGid': 1000, 'secondaryGidNumbers': [], 'syncedAt': 'now'}}
        r.record_posix_init('alice')
        assert r.back_channel.execute.call_count == 1
        assert r.back_channel.execute.call_args.args[0] is Registration.USER_POSIX_INIT_GQL
        assert r.back_channel.execute.call_args.args[1] == {'username': 'alice'}
        r.logger.warning.assert_not_called()

    def test_backend_error_never_raises(self):
        r = make_registration()
        r.back_channel.execute.side_effect = RuntimeError("user-lookup returned no primary gid")
        r.record_posix_init('alice')
        r.logger.warning.assert_called_once()

    def test_unexpected_response_never_raises(self):
        r = make_registration()
        r.back_channel.execute.return_value = {}
        r.record_posix_init('alice')
        r.logger.warning.assert_called_once()


LDAP_FACTS = {
    'ansible_facts': {
        'ldap_user_default_shell': '/bin/bash',
        'ldap_user_uidNumber': '12345',
        'ldap_user_gecos': 'Alice Example',
        'ldap_user_homedir': '/home/alice',
    }
}


def make_user_registration(dry_run=False):
    r = UserRegistration.__new__(UserRegistration)
    r.logger = Mock()
    r.back_channel = Mock()
    r.dry_run = dry_run
    r.run_playbook = Mock()
    r.playbook_task_res = Mock(return_value=LDAP_FACTS)
    return r


def executed_mutations(r):
    return [c.args[0] for c in r.back_channel.execute.call_args_list]


class TestDoNewUserPosixInit:

    def test_init_runs_right_after_upsert_and_before_repo_membership(self):
        r = make_user_registration()
        assert r.do_new_user('alice', 'alice@example.org', 'somefacility') is True
        assert executed_mutations(r) == [
            UserRegistration.USER_UPSERT_GQL,
            UserRegistration.USER_POSIX_INIT_GQL,
            UserRegistration.USER_STORAGE_GQL,
            UserRegistration.REPO_ADD_USER_GQL,
        ]
        init_call = r.back_channel.execute.call_args_list[1]
        assert init_call.args[1] == {'username': 'alice'}

    def test_dry_run_skips_init(self):
        r = make_user_registration(dry_run=True)
        r.do_new_user('alice', 'alice@example.org', 'somefacility')
        assert UserRegistration.USER_POSIX_INIT_GQL not in executed_mutations(r)

    def test_init_failure_does_not_block_registration(self):
        r = make_user_registration()

        def execute(query, variables):
            if query is UserRegistration.USER_POSIX_INIT_GQL:
                raise RuntimeError("coact-api down")
            return {}

        r.back_channel.execute.side_effect = execute
        assert r.do_new_user('alice', 'alice@example.org', 'somefacility') is True
        assert executed_mutations(r)[-2:] == [UserRegistration.USER_STORAGE_GQL, UserRegistration.REPO_ADD_USER_GQL]
        r.logger.warning.assert_called()

    def test_upsert_failure_skips_init(self):
        r = make_user_registration()
        r.back_channel.execute.side_effect = RuntimeError("userUpsert failed")
        with pytest.raises(RuntimeError):
            r.do_new_user('alice', 'alice@example.org', 'somefacility')
        assert executed_mutations(r) == [UserRegistration.USER_UPSERT_GQL]
