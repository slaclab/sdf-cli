from cliff.command import Command
from cliff.commandmanager import CommandManager

import logging

from .utils.graphql import GraphQlLister


class List(GraphQlLister):
    "show list of Repo's available"

    def take_action(self, parsed_args):


        # TODO: add filtering

        res = self.query("""
        { repos { name gid users } }
        """)

        # filter example
        # { repos(filters:{ name: "bd" } ) { name gid users }}
        
        return (
            ('Name', 'GID', 'Users'),
            # ((r['name'], r['gid'], ','.join(r['users'])) for r in res['repos'])
            ((r['name'], r['gid'], ','.join(r['users'][0:10])) for r in res['repos'])
        )


class GetReposWithUser(GraphQlLister):
    "list the Repos a User is associated with"


class ListMine(GraphQlLister):
    "show list of Repos that you have Principal or Leader roles on"

class AddUser(GraphQlLister):
    "add a User to a Repo (you must have Principal or Leader role on the Repo)"


class SyncToLDAP(GraphQlLister):
    "produce LDIFs of Repo information suitable for LDAP groups"
    def get_parser(self, prog_name):
        parser = super(SyncToLDAP, self).get_parser(prog_name)
        parser.add_argument('dn', default='ou=Group,dc=reg,o=slac')
        return parser

    def take_action(self, parsed_args):
        self.LOG.warn(f"{parsed_args}")
        res = self.query("""
        { repos { name gid users } }
        """)




class Repo(CommandManager):
    "Manage Repo's"

    LOG = logging.getLogger(__name__)

    def __init__(self, namespace, convert_underscores=True):
        super(Repo,self).__init__(namespace, convert_underscores=convert_underscores)
        for cmd in [ List, SyncToLDAP ]:
            self.add_command( cmd.__name__.lower(), cmd )


