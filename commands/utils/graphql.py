from os import getenv

from gql import gql, Client
from gql.transport.aiohttp import AIOHTTPTransport

from cliff.lister import Lister

import logging
from gql.transport.requests import log as requests_logger
requests_logger.setLevel(logging.ERROR)
from gql.transport.websockets import log as websockets_logger
websockets_logger.setLevel(logging.ERROR)


SDF_IRIS_URI=getenv("SDF_IRIS_URI", "http://localhost:8000/graphql")

class GraphQlLister(Lister):
    "Example of how to fetch data from graphql and dump output as a table"
    LOG = logging.getLogger(__name__)
    transport = None
    client = None

    def __init__(self, app, app_args, cmd_name=None):
        super(GraphQlLister, self).__init__(app, app_args, cmd_name=cmd_name)
        self.connect( SDF_IRIS_URI )

    def connect(self, graphql_uri, get_schema=False ):
        self.transport = AIOHTTPTransport(url=graphql_uri)
        self.client = Client(transport=self.transport, fetch_schema_from_transport=get_schema)

    def query(self, query, var={} ):
        return self.client.execute( gql(query), variable_values=var )


