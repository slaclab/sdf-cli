"""
Subprocess-like API for executing bash commands on remote hosts using ansible_runner.

This module provides a simple interface for running ad-hoc shell commands on
delegated hosts using ansible_runner, with an API similar to subprocess.
"""

from typing import Any, NamedTuple
import ansible_runner
from loguru import logger


class CompletedRemoteCommand(NamedTuple):
    """Result of a completed remote command execution.

    Similar to subprocess.CompletedProcess but for remote commands.
    """
    command: str
    returncode: int
    stdout: str
    stderr: str
    host: str

    def check_returncode(self):
        """Raise exception if the command returned non-zero exit status."""
        if self.returncode != 0:
            raise RemoteCommandError(
                f"Command '{self.command}' on host '{self.host}' "
                f"returned non-zero exit status {self.returncode}",
                self
            )


class RemoteCommandError(Exception):
    """Exception raised when a remote command fails.

    Similar to subprocess.CalledProcessError.
    """
    def __init__(self, message: str, completed_command: CompletedRemoteCommand):
        super().__init__(message)
        self.completed_command = completed_command
        self.returncode = completed_command.returncode
        self.stdout = completed_command.stdout
        self.stderr = completed_command.stderr
        self.command = completed_command.command
        self.host = completed_command.host


class RemoteShell:
    """Execute bash commands on remote hosts using ansible_runner.

    This class provides a subprocess-like interface for running commands
    on delegated hosts through Ansible's ad-hoc command execution.

    Example:
        >>> shell = RemoteShell(host='compute-node-01.example.com')
        >>> result = shell.run('ls -la /tmp')
        >>> print(result.stdout)
        >>> print(result.returncode)

        # With check=True to raise on non-zero exit
        >>> result = shell.run('some-command', check=True)

        # With custom inventory
        >>> shell = RemoteShell(
        ...     host='compute-node',
        ...     inventory={'all': {'hosts': {'compute-node': {'ansible_host': '10.0.0.1'}}}}
        ... )
    """

    def __init__(
        self,
        host: str,
        private_data_dir: str | None = None,
        inventory: dict[str, Any] | None = None,
        ssh_user: str | None = None,
        ssh_key: str | None = None,
        become: bool = False,
        become_user: str | None = None,
        **ansible_kwargs
    ):
        """Initialize RemoteShell for a specific host.

        Args:
            host: Target hostname or IP address
            private_data_dir: Directory for ansible-runner artifacts (default: ./ansible-runner/)
            inventory: Ansible inventory dict (default: simple host-only inventory)
            ssh_user: SSH username (default: current user)
            ssh_key: Path to SSH private key
            become: Enable privilege escalation (sudo)
            become_user: User to become (default: root)
            **ansible_kwargs: Additional arguments passed to ansible_runner.run()
        """
        self.host = host
        self.private_data_dir = private_data_dir or './ansible-runner/'
        self.ssh_user = ssh_user
        self.ssh_key = ssh_key
        self.become = become
        self.become_user = become_user
        self.ansible_kwargs = ansible_kwargs

        # Default simple inventory if not provided
        self.inventory = inventory or {
            'all': {
                'hosts': {
                    host: {}
                }
            }
        }

    def run(
        self,
        command: str,
        check: bool = False,
        timeout: int | None = None,
        **extra_vars
    ) -> CompletedRemoteCommand:
        """Execute a bash command on the remote host.

        Args:
            command: Bash command string to execute
            check: If True, raise RemoteCommandError on non-zero exit
            timeout: Command timeout in seconds
            **extra_vars: Additional variables passed to ansible

        Returns:
            CompletedRemoteCommand with stdout, stderr, and return code

        Raises:
            RemoteCommandError: If check=True and command returns non-zero
        """
        # Build extravars
        extravars = {
            'target_host': self.host,
            'shell_command': command,
        }
        if timeout:
            extravars['async_timeout'] = timeout
        extravars.update(extra_vars)

        # Build runner kwargs
        runner_kwargs = {
            'private_data_dir': self.private_data_dir,
            'module': 'shell',
            'module_args': command,
            'host_pattern': self.host,
            'inventory': self.inventory,
            'extravars': extravars,
            'suppress_env_files': True,
        }

        # Add optional SSH configuration
        if self.ssh_user:
            if 'cmdline' not in runner_kwargs:
                runner_kwargs['cmdline'] = ''
            runner_kwargs['cmdline'] += f' -u {self.ssh_user}'

        if self.ssh_key:
            if 'cmdline' not in runner_kwargs:
                runner_kwargs['cmdline'] = ''
            runner_kwargs['cmdline'] += f' --private-key={self.ssh_key}'

        # Add privilege escalation
        if self.become:
            runner_kwargs['cmdline'] = runner_kwargs.get('cmdline', '') + ' --become'
            if self.become_user:
                runner_kwargs['cmdline'] += f' --become-user={self.become_user}'

        # Merge with additional ansible kwargs
        runner_kwargs.update(self.ansible_kwargs)

        # Execute via ansible_runner
        logger.debug(f"Executing remote command on {self.host}: {command}")
        runner = ansible_runner.run(**runner_kwargs)

        # Extract results
        stdout, stderr, returncode = self._extract_results(runner)

        result = CompletedRemoteCommand(
            command=command,
            returncode=returncode,
            stdout=stdout,
            stderr=stderr,
            host=self.host
        )

        if check:
            result.check_returncode()

        return result

    def _extract_results(self, runner: ansible_runner.Runner) -> tuple[str, str, int]:
        """Extract stdout, stderr, and return code from ansible_runner results.

        Args:
            runner: Completed ansible_runner.Runner instance

        Returns:
            Tuple of (stdout, stderr, returncode)
        """
        stdout = ""
        stderr = ""
        returncode = runner.rc if runner.rc is not None else -1

        # Parse events to extract command output
        for event in runner.events:
            if event.get('event') == 'runner_on_ok' or event.get('event') == 'runner_on_failed':
                event_data = event.get('event_data', {})
                res = event_data.get('res', {})

                # Extract stdout and stderr from the module result
                if 'stdout' in res:
                    stdout = res['stdout']
                if 'stderr' in res:
                    stderr = res['stderr']

                # Get the actual command return code
                if 'rc' in res:
                    returncode = res['rc']

                # Also check for module failure messages
                if 'msg' in res and not stdout and not stderr:
                    stderr = res['msg']

        return stdout, stderr, returncode


def run(
    command: str,
    host: str,
    check: bool = False,
    timeout: int | None = None,
    private_data_dir: str | None = None,
    **kwargs
) -> CompletedRemoteCommand:
    """Execute a bash command on a remote host (convenience function).

    This is a simple function-based interface similar to subprocess.run().
    For multiple commands on the same host, use the RemoteShell class instead.

    Args:
        command: Bash command string to execute
        host: Target hostname or IP address
        check: If True, raise RemoteCommandError on non-zero exit
        timeout: Command timeout in seconds
        private_data_dir: Directory for ansible-runner artifacts
        **kwargs: Additional arguments passed to RemoteShell()

    Returns:
        CompletedRemoteCommand with stdout, stderr, and return code

    Example:
        >>> result = run('whoami', host='server.example.com')
        >>> print(result.stdout)
        root
        >>> print(result.returncode)
        0
    """
    shell = RemoteShell(
        host=host,
        private_data_dir=private_data_dir,
        **kwargs
    )
    return shell.run(command, check=check, timeout=timeout)
