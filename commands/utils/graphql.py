from os import getenv

from gql import gql, Client
from gql.transport.aiohttp import AIOHTTPTransport
from gql.transport.websockets import WebsocketsTransport

from cliff.lister import Lister

import logging
from gql.transport.requests import log as requests_logger
requests_logger.setLevel(logging.ERROR)
from gql.transport.websockets import log as websockets_logger
websockets_logger.setLevel(logging.ERROR)


SDF_COACT_URI=getenv("SDF_COACT_URI", "http://localhost:8000/graphql")


class GraphQlClient:

    transport = None
    client = None

    def connectGraphQl(self, graphql_uri=SDF_COACT_URI, get_schema=False ):
        self.transport = AIOHTTPTransport(url=graphql_uri)
        self.client = Client(transport=self.transport, fetch_schema_from_transport=get_schema)
        # lets reduce the logging from gql
        for name in logging.root.manager.loggerDict:
            if name.startswith('gql'):
                logger = logging.getLogger(name) 
                logger.setLevel(logging.WARNING)

    def query(self, query, var={} ):
        return self.client.execute( gql(query), variable_values=var )


class GraphQlSubscriber:

    LOG = logging.getLogger(__name__)

    transport = None
    client = None

    def connectGraphQl(self, graphql_uri=SDF_COACT_URI, get_schema=False ):
        self.LOG.info(f"connecting to {graphql_uri}")
        self.transport = WebsocketsTransport(url=graphql_uri)
        self.client = Client(transport=self.transport, fetch_schema_from_transport=get_schema)
        # lets reduce the logging from gql
        for name in logging.root.manager.loggerDict:
            if name.startswith('gql'):
                logger = logging.getLogger(name) 
                logger.setLevel(logging.WARNING)

    
    def subscribe(self, query, var={} ):
        self.connectGraphQl()
        return self.client.subscribe( gql(query), variable_values=var )




class GraphQlLister(Lister, GraphQlClient):
    "Example of how to fetch data from graphql and dump output as a table"
    LOG = logging.getLogger(__name__)

    def __init__(self, app, app_args, cmd_name=None):
        super(GraphQlLister, self).__init__(app, app_args, cmd_name=cmd_name)
        self.connectGraphQl()
        


