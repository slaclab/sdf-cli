#!/usr/bin/env python3
"""
Click-based implementation of the SDF Command Line Tools.

This module provides a click-based replacement for the cliff-based sdf.py,
using click groups instead of cliff's App and CommandManager pattern.
"""

import sys

import click
from loguru import logger

# Import base classes from modules package
from modules.base import (
    GraphQlMixin,
    common_options,
    graphql_options,
    configure_logging_from_verbose,
    CONTEXT_SETTINGS,
)


# Re-export base classes for backwards compatibility
__all__ = [
    'GraphQlMixin',
    'common_options',
    'graphql_options',
    'configure_logging_from_verbose',
    'MultiGroup',
    'cli',
    'main',
]


# =============================================================================
# Multi-Group CLI Support
# =============================================================================

class MultiGroup(click.Group):
    """
    A custom click Group that manages multiple command groups.

    This is analogous to the MultiApp class in the cliff-based implementation,
    providing a way to organize multiple command namespaces under a single CLI.
    """

    def __init__(self, name=None, commands=None, **attrs):
        super().__init__(name, commands, **attrs)
        self.command_managers = {}

    def add_command_manager(self, name: str, group: click.Group):
        """
        Add a command group as a subcommand.

        Args:
            name: The name to use for the command group
            group: A click.Group instance containing subcommands
        """
        self.command_managers[name] = group
        self.add_command(group, name=name)

    def list_commands(self, ctx):
        """List all available commands including command managers."""
        rv = super().list_commands(ctx)
        return rv

    def get_command(self, ctx, cmd_name):
        """Get a command by name, checking command managers first."""
        # First check if it's a registered command manager
        if cmd_name in self.command_managers:
            return self.command_managers[cmd_name]
        # Fall back to normal command lookup
        return super().get_command(ctx, cmd_name)


@click.group(cls=MultiGroup, invoke_without_command=True, context_settings=CONTEXT_SETTINGS)
@click.option('--debug', is_flag=True, help='Enable debug logging')
@click.option('--quiet', is_flag=True, help='Suppress non-error output')
@click.version_option(version='1.0', prog_name='sdf')
@click.pass_context
def cli(ctx, debug, quiet):
    """S3DF Command Line Tools

    A collection of utilities for managing S3DF resources including
    users, repositories, and compute allocations.
    """
    ctx.ensure_object(dict)
    ctx.obj['debug'] = debug
    ctx.obj['quiet'] = quiet

    # Configure loguru based on flags
    logger.remove()  # Remove default handler
    
    if debug:
        logger.add(
            sys.stderr,
            level="DEBUG",
            format="<green>{time:YYYY-MM-DD HH:mm:ss}</green> | <level>{level: <8}</level> | <cyan>{name}</cyan>:<cyan>{function}</cyan>:<cyan>{line}</cyan> - <level>{message}</level>",
            colorize=True,
        )
    elif quiet:
        logger.add(
            sys.stderr,
            level="ERROR",
            format="<green>{time:YYYY-MM-DD HH:mm:ss}</green> | <level>{level: <8}</level> | <level>{message}</level>",
            colorize=True,
        )
    else:
        logger.add(
            sys.stderr,
            level="INFO",
            format="<green>{time:YYYY-MM-DD HH:mm:ss}</green> | <level>{level: <8}</level> | <level>{message}</level>",
            colorize=True,
        )

    # If no subcommand is provided, show help
    if ctx.invoked_subcommand is None:
        click.echo(ctx.get_help())


# =============================================================================
# Import and register command groups from modules
# =============================================================================

# Import the coact command group (slurm job management and accounting)
from modules.coact import coact
cli.add_command(coact)

# Import the coactd command group (daemon/workflow processing)
from modules.coactd import coactd
cli.add_command(coactd)


# =============================================================================
# Placeholder groups for other command managers (to be migrated)
# =============================================================================

@cli.group(context_settings=CONTEXT_SETTINGS)
@click.pass_context
def user(ctx):
    """Manage Users"""
    pass


@user.command(name='list')
@click.pass_context
def user_list(ctx):
    """Show all users"""
    click.echo("User list command - to be implemented")


@user.command(name='add')
@click.option('--uid', '-u', required=True, help='uid/username')
@click.option('--uidnumber', '-n', help='uid number')
@click.option('--eppns', multiple=True, help='authenticated eppns/email addresses')
@click.pass_context
def user_add(ctx, uid, uidnumber, eppns):
    """Add a User"""
    click.echo(f"Adding user {uid} - to be implemented")


@user.command(name='delete')
@click.option('--uid', '-u', required=True, help='uid/username')
@click.pass_context
def user_delete(ctx, uid):
    """Delete a User"""
    click.echo(f"Deleting user {uid} - to be implemented")


@user.command(name='update')
@click.option('--uid', '-u', required=True, help='uid/username')
@click.option('--uidnumber', '-n', help='uid number')
@click.option('--eppns', multiple=True, help='authenticated eppns/email addresses')
@click.pass_context
def user_update(ctx, uid, uidnumber, eppns):
    """Modify a User record"""
    click.echo(f"Updating user {uid} - to be implemented")


@cli.group(context_settings=CONTEXT_SETTINGS)
@click.pass_context
def repo(ctx):
    """Manage Repositories"""
    pass


@repo.command(name='list')
@click.pass_context
def repo_list(ctx):
    """Show all repositories"""
    click.echo("Repo list command - to be implemented")


@cli.group(context_settings=CONTEXT_SETTINGS)
@click.pass_context
def menu(ctx):
    """Menu-based interface"""
    pass


# =============================================================================
# Main entry point
# =============================================================================

def main(argv=None):
    """Main entry point for the CLI."""
    try:
        cli(obj={})
    except Exception as e:
        logger.exception(f"Error running CLI: {e}")
        return 1
    return 0


if __name__ == '__main__':
    sys.exit(main(sys.argv[1:]))