from cliff.command import Command
from cliff.commandmanager import CommandManager

import logging

from .utils.graphql import GraphQlLister

def comma_separate( array, limit=10 ):
    postfix = ''
    if len(array) > limit:
        postfix = ',...'
    return ','.join( array[:limit] ) + postfix

class List(GraphQlLister):
    "show list of Repo's available"

    def take_action(self, parsed_args):

        # TODO: add filtering

        res = self.query("""
        { repos( filter: {} ) { facility name state accessGroups users principal leaders users} }
        """)

        # filter example
        # { repos(filters:{ name: "bd" } ) { name gid users }}
        
        return (
            ('Facility', 'Name', 'State', 'Access Groups', 'Principal', 'Leaders', 'Users'),
            # ((r['name'], r['gid'], ','.join(r['users'])) for r in res['repos'])
            (( r['facility'], r['name'], r['state'], comma_separate(r['accessGroups']), r['principal'], comma_separate(r['leaders']), comma_separate(r['users']) ) for r in res['repos'] )
        )


class GetReposWithUser(GraphQlLister):
    "list the Repos a User is associated with"


class ListMine(GraphQlLister):
    "show list of Repos that you have Principal or Leader roles on"

class AddUser(GraphQlLister):
    "add a User to a Repo (you must have Principal or Leader role on the Repo)"



class Repo(CommandManager):
    "Manage Repo's"

    LOG = logging.getLogger(__name__)

    def __init__(self, namespace, convert_underscores=True):
        super(Repo,self).__init__(namespace, convert_underscores=convert_underscores)
        for cmd in [ List, ]:
            self.add_command( cmd.__name__.lower(), cmd )


