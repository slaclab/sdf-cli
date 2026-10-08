"""
Tests for Registration.record_posix_group_change: after the posixGroup playbook changes LDAP, coactd
tells coact-api which gid was added/removed for the user. It must pass the exact delta and never raise.
"""

import sys
from unittest.mock import Mock

sys.modules['ansible_runner'] = Mock()

from modules.coactd import Registration  # noqa: E402


def make_registration():
    r = Registration.__new__(Registration)
    r.logger = Mock()
    r.back_channel = Mock()
    return r


class TestRecordPosixGroupChange:

    def test_present_sends_user_gid_and_flag(self):
        r = make_registration()
        r.back_channel.execute.return_value = {'userPosixGroupUpdate': {'secondaryGidNumbers': [3049]}}
        r.record_posix_group_change('alice', '3049', present=True)
        assert r.back_channel.execute.call_count == 1
        assert r.back_channel.execute.call_args.args[1] == {'username': 'alice', 'gidnumber': 3049, 'present': True}

    def test_absent_sends_false(self):
        r = make_registration()
        r.back_channel.execute.return_value = {'userPosixGroupUpdate': {'secondaryGidNumbers': []}}
        r.record_posix_group_change('alice', 3049, present=False)
        assert r.back_channel.execute.call_args.args[1]['present'] is False

    def test_backend_error_never_raises(self):
        r = make_registration()
        r.back_channel.execute.side_effect = RuntimeError("coact-api down")
        r.record_posix_group_change('alice', 3049, present=True)
        r.logger.warning.assert_called()
