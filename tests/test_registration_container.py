"""
The pieces the containerized registration daemons depend on: the Ansible target
extravar, writable runner directories, the password from COACT_PASSWORD, a dry
run that leaves requests alone, reconnecting when the websocket drops, and
finishing the current request on SIGTERM.
"""

import asyncio
import sys
from unittest.mock import Mock, patch

import pytest
from click.testing import CliRunner
from gql.transport.exceptions import TransportClosed

sys.modules['ansible_runner'] = Mock()

from modules import coactd  # noqa: E402
from gql import gql  # noqa: E402

from modules.coactd import Registration, UserRegistration  # noqa: E402
from modules.utils.graphql import GraphQlSubscriber  # noqa: E402

TARGET = 'sdfmgr002.sdf.slac.stanford.edu'
RUNNER_ENV = ('COACT_ANSIBLE_RUNNER_PATH', 'COACT_ANSIBLE_ARTIFACT_DIR', 'COACT_ANSIBLE_ROTATE_ARTIFACTS')


@pytest.fixture
def ansible_run(monkeypatch):
    for var in RUNNER_ENV:
        monkeypatch.delenv(var, raising=False)
    run = Mock()
    run.return_value.rc = 0
    monkeypatch.setattr(coactd.ansible_runner, 'run', run)
    return run


def make_user_registration(**kwargs):
    r = UserRegistration(username='sdf-bot', client_name='test', password='pw', **kwargs)
    r.logger = Mock()
    r.back_channel = Mock()
    r.ident = 'req1'
    return r


# ---------------------------------------------------------------------------
# run_playbook
# ---------------------------------------------------------------------------

class TestRunPlaybook:

    def test_target_added_to_extravars(self, ansible_run):
        r = make_user_registration(ansible_target=TARGET)
        r.run_playbook('set_user_shell.yaml', user='alice', user_login_shell='/bin/zsh')
        assert ansible_run.call_args.kwargs['extravars'] == {
            'user': 'alice', 'user_login_shell': '/bin/zsh', 'coact_ansible_target': TARGET,
        }

    def test_no_target_leaves_extravars_unchanged(self, ansible_run):
        r = make_user_registration()
        r.run_playbook('set_user_shell.yaml', user='alice', user_login_shell='/bin/zsh')
        assert ansible_run.call_args.kwargs['extravars'] == {'user': 'alice', 'user_login_shell': '/bin/zsh'}

    def test_empty_target_is_no_target(self, ansible_run):
        r = make_user_registration(ansible_target='')
        r.run_playbook('set_user_shell.yaml', user='alice')
        assert 'coact_ansible_target' not in ansible_run.call_args.kwargs['extravars']

    def test_bare_metal_runner_dirs_by_default(self, ansible_run):
        make_user_registration().run_playbook('set_user_shell.yaml')
        kw = ansible_run.call_args.kwargs
        assert kw['private_data_dir'] == './ansible-runner/'
        assert kw['artifact_dir'] is None
        assert kw['rotate_artifacts'] == 0

    def test_runner_dirs_from_env(self, ansible_run, monkeypatch):
        monkeypatch.setenv('COACT_ANSIBLE_RUNNER_PATH', '/app/ansible-runner')
        monkeypatch.setenv('COACT_ANSIBLE_ARTIFACT_DIR', '/var/lib/coact/artifacts')
        monkeypatch.setenv('COACT_ANSIBLE_ROTATE_ARTIFACTS', '50')
        make_user_registration().run_playbook('set_user_shell.yaml')
        kw = ansible_run.call_args.kwargs
        assert kw['private_data_dir'] == '/app/ansible-runner'
        assert kw['artifact_dir'] == '/var/lib/coact/artifacts'
        assert kw['rotate_artifacts'] == 50

    def test_handler_dry_run_wins_over_caller(self, ansible_run):
        r = make_user_registration(dry_run=True)
        assert r.run_playbook('coact/add_user.yaml', dry_run=False, user='alice') is None
        ansible_run.assert_not_called()

    def test_failed_playbook_raises(self, ansible_run):
        ansible_run.return_value.rc = 2
        with pytest.raises(Exception, match='AnsibleRunner failed'):
            make_user_registration().run_playbook('set_user_shell.yaml')


# ---------------------------------------------------------------------------
# dry run
# ---------------------------------------------------------------------------

ACCOUNT_REQ = {'Id': 'req1', 'reqtype': 'UserAccount', 'approvalstatus': 'Approved',
               'preferredUserName': 'alice', 'facilityname': 'lcls', 'eppn': 'alice@slac.stanford.edu'}
SHELL_REQ = {'Id': 'req2', 'reqtype': 'UserChangeShell', 'approvalstatus': 'Approved',
             'username': 'alice', 'shell': '/bin/zsh'}


def process(r, req):
    r.markCompleteRequest = Mock()
    r.markIncompleteRequest = Mock()
    r.process(req['Id'], 'update', req['reqtype'], req['approvalstatus'], req)


class TestDryRun:

    @pytest.mark.parametrize('req', [ACCOUNT_REQ, SHELL_REQ], ids=['UserAccount', 'UserChangeShell'])
    def test_dry_run_runs_nothing_writes_nothing_marks_nothing(self, ansible_run, req):
        r = make_user_registration(dry_run=True)
        process(r, req)
        ansible_run.assert_not_called()
        r.back_channel.execute.assert_not_called()
        r.markCompleteRequest.assert_not_called()
        r.markIncompleteRequest.assert_not_called()

    def test_dry_run_failure_does_not_mark_incomplete(self, ansible_run):
        r = make_user_registration(dry_run=True)
        process(r, dict(ACCOUNT_REQ, facilityname=None))  # fails the do_new_user assert
        r.markIncompleteRequest.assert_not_called()
        r.logger.exception.assert_called_once()

    def test_real_run_marks_complete(self, ansible_run):
        r = make_user_registration()
        process(r, SHELL_REQ)
        r.markCompleteRequest.assert_called_once()
        r.markIncompleteRequest.assert_not_called()

    def test_real_run_failure_marks_incomplete(self, ansible_run):
        ansible_run.return_value.rc = 2
        r = make_user_registration()
        process(r, SHELL_REQ)
        r.markIncompleteRequest.assert_called_once()
        r.markCompleteRequest.assert_not_called()


# ---------------------------------------------------------------------------
# password
# ---------------------------------------------------------------------------

class TestPassword:

    def invoke(self, args, env=None):
        with patch.object(coactd, 'run_registration') as run:
            result = CliRunner().invoke(coactd.coactd, ['userregistration', *args], env=env or {}, obj={})
        return result, run

    @pytest.mark.parametrize('command', ['userregistration', 'reporegistration'])
    def test_password_from_env(self, command):
        with patch.object(coactd, 'run_registration') as run:
            result = CliRunner().invoke(coactd.coactd, [command],
                                        env={'COACT_PASSWORD': 'from-env\n', 'COACT_ANSIBLE_TARGET': TARGET}, obj={})
        assert result.exit_code == 0, result.output
        handler = run.call_args.args[0]
        assert handler.resolve_password() == 'from-env'
        assert handler.ansible_target == TARGET

    def test_password_from_command_line(self):
        result, run = self.invoke(['--password', 'from-cli'])
        assert result.exit_code == 0, result.output
        assert run.call_args.args[0].resolve_password() == 'from-cli'
        assert run.call_args.args[0].ansible_target is None

    def test_missing_password_is_an_error(self):
        result, run = self.invoke([])
        assert result.exit_code == 2
        assert '--password' in result.output
        run.assert_not_called()

    @pytest.mark.parametrize('command', ['userregistration', 'reporegistration'])
    def test_there_is_no_password_file_option(self, tmp_path, command):
        pw = tmp_path / 'password'
        pw.write_text('from-file')
        with patch.object(coactd, 'run_registration') as run:
            result = CliRunner().invoke(coactd.coactd, [command, '--password-file', str(pw)],
                                        env={'COACT_PASSWORD': 'from-env'}, obj={})
        assert result.exit_code == 2
        assert 'No such option' in result.output
        run.assert_not_called()

    def test_run_passes_the_env_password_to_both_connections(self):
        r = make_user_registration()
        r.connect_graph_ql = Mock()
        r.connect_subscriber = Mock()
        r.subscribe = Mock(return_value=iter([]))
        r.run()
        assert r.connect_graph_ql.call_args.kwargs['password'] == 'pw'
        assert r.connect_subscriber.call_args.kwargs['password'] == 'pw'
        assert 'password_file' not in r.connect_graph_ql.call_args.kwargs

    def test_crash_does_not_log_the_env_password(self, capsys):
        import re
        import sdf_click
        secret = 'hunter2-do-not-log'
        with patch('modules.coactd.UserRegistration.connect_graph_ql', side_effect=RuntimeError('boom')), \
                patch.object(coactd.signal, 'signal'), \
                patch('sys.argv', ['sdf_click.py', 'coactd', 'userregistration', '-vv']), \
                patch.dict('os.environ', {'COACT_PASSWORD': secret}):
            assert sdf_click.main() == 1
        err = re.sub(r'\x1b\[[0-9;]*m', '', capsys.readouterr().err)
        assert 'RuntimeError: boom' in err
        assert secret not in err


# ---------------------------------------------------------------------------
# reconnect and SIGTERM
# ---------------------------------------------------------------------------

class TestRunForever:

    def handler(self, runs):
        r = make_user_registration()
        r.run = Mock(side_effect=runs)
        return r

    def test_zero_reconnects_exits_on_first_drop(self, monkeypatch):
        monkeypatch.setattr(coactd.time, 'sleep', Mock())
        r = self.handler([TransportClosed('gone')])
        with pytest.raises(SystemExit) as e:
            r.run_forever(max_reconnects=0)
        assert e.value.code == 1
        assert r.run.call_count == 1

    def test_reconnects_then_gives_up(self, monkeypatch):
        monkeypatch.setattr(coactd.time, 'sleep', Mock())
        r = self.handler([TransportClosed('gone'), OSError('refused'), TransportClosed('gone')])
        with pytest.raises(SystemExit):
            r.run_forever(max_reconnects=2)
        assert r.run.call_count == 3

    def test_long_lived_connection_resets_the_count(self, monkeypatch):
        monkeypatch.setattr(coactd.time, 'sleep', Mock())
        clock = iter([0, 1, 1, 1000, 1000, 1001])  # first run short, second long, third short
        monkeypatch.setattr(coactd.time, 'monotonic', lambda: next(clock))
        r = self.handler([TransportClosed('a'), TransportClosed('b'), TransportClosed('c')])
        with pytest.raises(SystemExit):
            r.run_forever(max_reconnects=1, stable_after=60)
        assert r.run.call_count == 3

    def test_request_errors_are_not_swallowed(self):
        r = self.handler([ValueError('a bug')])
        with pytest.raises(ValueError):
            r.run_forever(max_reconnects=5)

    def test_stopping_returns_instead_of_reconnecting(self):
        r = self.handler([TransportClosed('gone')])
        r.stopping = True
        r.run_forever(max_reconnects=5)
        assert r.run.call_count == 1


class FakeSubscriptionClient:
    """Stands in for gql's Client: subscribe_async yields the given events, then waits forever."""

    def __init__(self, events, before_yield=lambda: None):
        self.events = events
        self.before_yield = before_yield
        self.closed = False

    async def subscribe_async(self, document, variable_values=None):
        try:
            for event in self.events:
                self.before_yield()
                yield event
            await asyncio.sleep(3600)
        finally:
            self.closed = True


def request_event(req_id):
    return {'operationType': 'update', 'requests': {'theRequest': dict(SHELL_REQ, Id=req_id)}}


class TestSubscribeStop:

    def subscriber(self, client):
        s = GraphQlSubscriber()
        s.subscription_client = client
        return s

    def test_idle_stop_returns_and_closes(self):
        client = FakeSubscriptionClient([])
        stop = {'now': False}
        polls = []

        def should_stop():
            polls.append(1)
            if len(polls) >= 3:
                stop['now'] = True
            return stop['now']

        got = list(self.subscriber(client).subscribe('subscription { x }', should_stop=should_stop, poll_interval=0.01))
        assert got == []
        assert client.closed

    def test_stop_in_the_window_after_an_event_never_drops_it(self):
        # The stop flag goes up after the server has handed over the event
        # (the old window before busy=True): the event must still come out.
        stop = {'now': False}
        client = FakeSubscriptionClient([request_event('req1')],
                                        before_yield=lambda: stop.update(now=True))
        got = list(self.subscriber(client).subscribe('subscription { x }', should_stop=lambda: stop['now'],
                                                     poll_interval=0.01))
        assert [g[0] for g in got] == ['req1']
        assert client.closed

    def test_events_flow_until_stopped(self):
        client = FakeSubscriptionClient([request_event('a'), {'operationType': 'delete', 'requests': {}},
                                         request_event('b')])
        sub = self.subscriber(client).subscribe('subscription { x }', should_stop=lambda: False, poll_interval=0.01)
        assert [next(sub)[0], next(sub)[0]] == ['a', 'b']  # the delete is filtered out
        sub.close()
        assert client.closed


class TestSigterm:

    @pytest.mark.parametrize('busy', [False, True])
    def test_sigterm_only_sets_the_flag(self, busy):
        r = make_user_registration()
        r.busy = busy
        r.handle_sigterm(15, None)  # never raises, idle or busy
        assert r.stopping

    def test_run_stops_the_subscription_via_the_flag(self):
        r = make_user_registration()
        r.connect_graph_ql = Mock()
        r.connect_subscriber = Mock()
        r.subscribe = Mock(return_value=iter([]))
        r.run()
        should_stop = r.subscribe.call_args.kwargs['should_stop']
        assert should_stop() is False
        r.handle_sigterm(15, None)
        assert should_stop() is True

    def test_sigterm_during_reconnect_sleep_stops(self, monkeypatch):
        r = make_user_registration()
        r.run = Mock(side_effect=[TransportClosed('gone')])
        monkeypatch.setattr(coactd.time, 'sleep', lambda s: r.handle_sigterm(15, None))
        r.run_forever(max_reconnects=5)
        assert r.run.call_count == 1

    def test_busy_sigterm_finishes_the_request_then_stops(self):
        r = make_user_registration()
        r.connect_graph_ql = Mock()
        r.connect_subscriber = Mock()
        events = [('req1', 'update', 'UserChangeShell', 'Approved', SHELL_REQ),
                  ('req3', 'update', 'UserChangeShell', 'Approved', dict(SHELL_REQ, Id='req3'))]
        r.subscribe = Mock(return_value=iter(events))
        processed = []

        def process(req_id, *args):
            r.handle_sigterm(15, None)  # arrives mid-request: must not raise
            processed.append(req_id)

        r.process = process
        r.run()
        assert processed == ['req1']
        assert r.stopping and not r.busy

    def test_run_registration_installs_the_handler(self):
        r = make_user_registration()
        r.run_forever = Mock()
        with patch.object(coactd.signal, 'signal') as sig:
            coactd.run_registration(r, max_reconnects=3)
        sig.assert_called_once_with(coactd.signal.SIGTERM, r.handle_sigterm)
        r.run_forever.assert_called_once_with(max_reconnects=3)


# ---------------------------------------------------------------------------
# dry-run back channel (every handler)
# ---------------------------------------------------------------------------

class TestDryRunBackChannel:

    def test_mutations_are_skipped_queries_pass_through(self):
        client = Mock()
        client.execute.return_value = {'repo': {'users': []}}
        log = Mock()
        bc = coactd.DryRunBackChannel(client, log)
        assert bc.execute(UserRegistration.USER_UPSERT_GQL, {'user': {'username': 'alice'}}) is None
        client.execute.assert_not_called()
        assert 'would send mutation userUpsert' in log.warning.call_args.args[0]
        query = gql('query getRepoUsers($repo: RepoInput!) { repo(filter: $repo) { users } }')
        assert bc.execute(query, {'repo': {}}) == {'repo': {'users': []}}
        client.execute.assert_called_once()

    def test_dry_run_wraps_the_back_channel(self):
        r = coactd.RepoRegistration(username='sdf-bot', client_name='t', password='pw', dry_run=True)
        r.connect_graph_ql = Mock()
        r.connect_subscriber = Mock()
        r.subscribe = Mock(return_value=iter([]))
        r.run()
        assert isinstance(r.back_channel, coactd.DryRunBackChannel)

    def test_repo_dry_run_sends_no_mutation(self):
        r = coactd.RepoRegistration(username='sdf-bot', client_name='t', password='pw', dry_run=True)
        r.logger = Mock()
        client = Mock()
        client.execute.return_value = {'repo': {'clusters': [], 'features': [], 'users': ['alice']}}
        r.back_channel = coactd.DryRunBackChannel(client, r.logger)
        assert r.do_repo_membership(user='bob', repo='r', facility='f', action='present', dry_run=True) is True
        sent = [c.args[0] for c in client.execute.call_args_list]
        assert sent and not any(coactd.is_mutation(d) for d in sent)

    def test_real_run_does_not_wrap(self):
        r = make_user_registration()
        r.connect_graph_ql = Mock(return_value='client')
        r.connect_subscriber = Mock()
        r.subscribe = Mock(return_value=iter([]))
        r.run()
        assert r.back_channel == 'client'
