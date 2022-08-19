import os
import sys

from cliff.command import Command
from cliff.commandmanager import CommandManager

from .utils.graphql import GraphQlLister

import logging

class Get(GraphQlLister):
    'display details of journal with given title'

    log = logging.getLogger(__name__)

    def take_action(self, parsed_args):
        parser = argparse.ArgumentParser(description="Load sample data from SLURM into the jobs collection. This is mainly for testing")
        parser.add_argument("-v", "--verbose", action='store_true', help="Turn on verbose logging")
        parser.add_argument("-u", "--url", help="The URL to the CoAct GraphQL API", default="wss://coact-dev.slac.stanford.edu/graphql")
        args = parser.parse_args()
        logging.basicConfig(level=logging.DEBUG if args.verbose else logging.INFO)

        transport = WebsocketsTransport(url=args.url)

        client = Client(
            transport=transport,
            fetch_schema_from_transport=False,
        )

        # Provide a GraphQL query
        query = gql(
            """
            subscription {
            requests {
                theRequest {
                reqtype
                eppn
                preferredUserName
                }
            }
            }
        """
        )

        for result in client.subscribe(query):
            print (result)

class Coactd(CommandManager):
    "A Manager class to register sub commands"
    LOG = logging.getLogger(__name__)

    def __init__(self, namespace, convert_underscores=True):
        super(Menu,self).__init__(namespace, convert_underscores=convert_underscores)
        for cmd in [ Get, ]:
            self.add_command( cmd.__name__.lower(), cmd )


