"""
Base classes for click-based command managers and mixins.

This module provides reusable base classes that can be used by all
subcommand modules in the SDF CLI.
"""

import sys
from enum import Enum
from pathlib import Path
from typing import Optional

import click
from loguru import logger
import ansible_runner

from .utils.graphql import GraphQlClient

# Define context settings to support -h for help across all commands
CONTEXT_SETTINGS = dict(help_option_names=['-h', '--help'])

COACT_ANSIBLE_RUNNER_PATH = './ansible-runner/'


class AnsibleRunner:
    """Mixin class for running Ansible playbooks."""
    # Using loguru logger
    ident = None

    def run_playbook(
        self,
        playbook: str,
        private_data_dir: str = COACT_ANSIBLE_RUNNER_PATH,
        tags: str = 'all',
        dry_run: bool = False,
        **kwargs
    ) -> Optional[ansible_runner.runner.Runner]:
        name = Path(playbook).name
        if not dry_run:
            r = ansible_runner.run(
                private_data_dir=private_data_dir,
                playbook=playbook,
                tags=tags,
                extravars=kwargs,
                suppress_env_files=True,
                ident=f'{self.ident}_{name}:{tags}',
                cancel_callback=lambda: None
            )
            self.logger.debug(r.stats)
            if not r.rc == 0:
                raise Exception("AnsibleRunner failed")
            return r
        else:
            self.logger.warning(f"not running playbook {playbook}")
            return None

    def playbook_events(self, runner: ansible_runner.runner.Runner) -> dict:
        for e in runner.events:
            if 'event_data' in e:
                yield e['event_data']

    def playbook_task_res(self, runner: ansible_runner.runner.Runner, play: str, task: str) -> dict:
        for e in self.playbook_events(runner):
            if 'play' in e and play == e['play'] and 'task' in e and task == e['task'] and 'res' in e:
                return e['res']

class GraphQlMixin:
    """
    Mixin class providing GraphQL client functionality for click commands.
    
    This mixin provides helper methods for connecting to GraphQL services
    and managing authentication. It can be used with command handler classes.
    
    Example usage:
        class MyHandler(GraphQlMixin):
            def run(self):
                self.back_channel = self.connect_graph_ql(
                    username='user',
                    password_file='/path/to/password'
                )
                # Use self.back_channel for GraphQL queries
    """
    
    back_channel = None
    
    def connect_graph_ql(self, username: str, password_file: str, timeout: int = 60):
        """
        Connect to the GraphQL service.
        
        Args:
            username: The username for basic auth
            password_file: Path to file containing the password
            timeout: Connection timeout in seconds
            
        Returns:
            A connected GraphQL client
        """
        # Reuse the existing GraphQlClient connection logic
        client = GraphQlClient()
        return client.connect_graph_ql(
            username=username,
            password_file=password_file,
            timeout=timeout
        )
    
    @staticmethod
    def get_password(password_file: str) -> str:
        """
        Read password from file.
        
        Args:
            password_file: Path to the password file
            
        Returns:
            The password string with whitespace stripped
        """
        with open(password_file, 'r') as f:
            return f.read().strip()


def common_options(f):
    """
    Decorator for common options shared across commands.
    
    Adds -v/--verbose flag to commands with counting support:
    - No flag: normal output (WARNING level)
    - -v: verbose output (INFO level)
    - -vv: debug output (DEBUG level)
    - -vvv: trace output (TRACE level)
    """
    f = click.option(
        '-v', '--verbose',
        count=True,
        help='Verbose output (-v for info, -vv for debug, -vvv for trace)'
    )(f)
    return f


def configure_logging_from_verbose(verbose: int) -> None:
    """
    Configure loguru logging level based on verbose count.
    
    Args:
        verbose: The verbosity level (0=WARNING, 1=INFO, 2=DEBUG, 3+=TRACE)
    """
    # Remove default handler
    logger.remove()
    
    # Determine level based on verbosity
    if verbose >= 3:
        level = "TRACE"
    elif verbose >= 2:
        level = "DEBUG"
    elif verbose >= 1:
        level = "INFO"
    else:
        level = "WARNING"
    
    # Add handler with appropriate level and format
    logger.add(
        sys.stderr,
        level=level,
        format="<green>{time:YYYY-MM-DD HH:mm:ss}</green> | <level>{level: <8}</level> | <cyan>{name}</cyan>:<cyan>{function}</cyan>:<cyan>{line}</cyan> - <level>{message}</level>",
        colorize=True,
    )
    
    logger.debug(f"Logging configured with level: {level}")


def graphql_options(f):
    """
    Decorator for GraphQL authentication options.
    
    Adds --username and --password-file options to commands.
    """
    f = click.option(
        '--username',
        default='sdf-bot',
        help='Basic auth username for graphql service'
    )(f)
    f = click.option(
        '--password-file',
        required=True,
        type=click.Path(exists=True),
        help='Basic auth password for graphql service'
    )(f)
    return f