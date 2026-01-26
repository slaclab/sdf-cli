"""
LDAP utilities for the SDF CLI.

This module provides LDAP client functionality for connecting to and
querying LDAP servers.
"""

from os.path import exists

import bonsai
from loguru import logger


def get_password(filepath):
    """Read password from a file.
    
    Args:
        filepath: Path to the password file
        
    Returns:
        The password string with whitespace stripped, or None if no filepath
        
    Raises:
        FileNotFoundError: If the password file does not exist
    """
    password = None
    if filepath:
        if not exists(filepath):
            raise FileNotFoundError(f'BindPW file {filepath} not found.')
        with open(filepath) as f:
            password = f.read().strip()
    return password


def client(server, binddn=None, password_file=None, cert_policy='never'):
    """Create and connect an LDAP client.
    
    Args:
        server: LDAP server URI
        binddn: Bind DN for authentication
        password_file: Path to file containing the bind password
        cert_policy: Certificate policy ('never', 'allow', 'try', 'demand')
        
    Returns:
        A connected LDAP client
    """
    client = bonsai.LDAPClient(server)
    logger.debug(f"Connecting to {server} with {binddn}")
    password = get_password(password_file)
    if binddn and password is not None:
        client.set_credentials('SIMPLE', binddn, password)
    client.set_cert_policy(cert_policy)
    return client.connect()