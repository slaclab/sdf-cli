from cliff.command import Command
from cliff.commandmanager import CommandManager

import logging

from .utils.graphql import GraphQlLister


class List(GraphQlLister):
    "show list of Repo's available"

    LOG = logging.getLogger(__name__)

    def take_action(self, parsed_args):
        res = self.query("""
        { repos { name gid users } }
        """)
        self.LOG.warning(f"OUTPUT {res}")

        # filter example
        # { repos(filters:{ name: "bd" } ) { name gid users }}
        
        return (
            ('Name', 'GID', 'Users'),
            # ((r['name'], r['gid'], ','.join(r['users'])) for r in res['repos'])
            ((r['name'], r['gid'], ','.join(r['users'][0:10])) for r in res['repos'])
        )

class Repo(CommandManager):
    "Manage Repo's"

    LOG = logging.getLogger(__name__)

    def __init__(self, namespace, convert_underscores=True):
        super(Repo,self).__init__(namespace, convert_underscores=convert_underscores)
        for cmd in [ List, ]:
            self.add_command( cmd.__name__.lower(), cmd )


