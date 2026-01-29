"""
Base classes for click-based command managers and mixins.

This module provides reusable base classes that can be used by all
subcommand modules in the SDF CLI.
"""

import sys

import click
from loguru import logger

from .utils.graphql import GraphQlClient

# Define context settings to support -h for help across all commands
CONTEXT_SETTINGS = dict(help_option_names=['-h', '--help'])


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