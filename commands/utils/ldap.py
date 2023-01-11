from os.path import exists
import bonsai

import logging

def get_password( filepath ):
    password = None
    if filepath:
        if not exists(filepath):
            raise FileNotFoundError(f'BindPW file {filepath} not found.') 
        with open(filepath) as f:
          password = f.read().strip()
    return password


def client( server, binddn=None, password_file=None, cert_policy='never' ):

    client = bonsai.LDAPClient( server )
    logging.warning(f"connecting to {server} with {binddn}")
    password = get_password( password_file )
    if binddn and not password == None:
        client.set_credentials( 'SIMPLE', binddn, password )
    client.set_cert_policy(cert_policy)
    return client.connect()

