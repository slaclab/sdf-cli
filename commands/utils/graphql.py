from os import getenv

from gql import gql, Client
from gql.transport.aiohttp import AIOHTTPTransport

from cliff.lister import Lister

import logging


SDF_IRIS_URI=getenv("SDF_IRIS_URI", "http://localhost:8000/graphql")

class GraphQlLister(Lister):
    "Example of how to fetch data from graphql and dump output as a table"
    LOG = logging.getLogger(__name__)
    transport = None
    client = None

    def __init__(self, app, app_args, cmd_name=None):
        super(GraphQlLister, self).__init__(app, app_args, cmd_name=cmd_name)
        self.connect( SDF_IRIS_URI )

    def connect(self, graphql_uri ):
        self.transport = AIOHTTPTransport(url=graphql_uri)
        self.client = Client(transport=self.transport, fetch_schema_from_transport=True)

    def query(self, query ):
        return self.client.execute( gql(query) )


