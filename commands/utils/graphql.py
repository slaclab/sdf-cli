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

import base64

SDF_COACT_URI=getenv("SDF_COACT_URI", "coact-dev.slac.stanford.edu:443/graphql-service")


class GraphQlClient:

    transport = None
    client = None

    def get_password( self, password_file=None ):
        password = None
        with open(password_file,'r') as f:
          password = f.read()
        return password

    def get_basic_auth_headers( self, username=None, password=None ):
        headers = {}
        if username and password:
          mux = f'{username}:{password}'.encode("ascii")
          headers = { 'Authorization': f'Basic {base64.b64encode(mux).decode("ascii")}' }
        return headers

    def connect_graph_ql(self, graphql_uri='https://'+SDF_COACT_URI, get_schema=False, username=None, password_file=None, password=None ):
        self.LOG.info(f"connecting to {graphql_uri}")
        if password_file:
            password = self.get_password( password_file=password_file )
        self.transport = AIOHTTPTransport(url=graphql_uri, headers=self.get_basic_auth_headers( username=username, password=password ))
        self.client = Client(transport=self.transport, fetch_schema_from_transport=get_schema)
        # lets reduce the logging from gql
        for name in logging.root.manager.loggerDict:
            if name.startswith('gql'):
                logger = logging.getLogger(name) 
                logger.setLevel(logging.WARNING)

    def query(self, query, var={} ):
        return self.client.execute( gql(query), variable_values=var )


class GraphQlSubscriber( GraphQlClient ):

    LOG = logging.getLogger(__name__)

    transport = None
    client = None

    def connect_subscriber(self, graphql_uri='wss://'+SDF_COACT_URI, get_schema=False, username=None, password=None ):
        self.LOG.info(f"connecting to {graphql_uri}")
        self.transport = WebsocketsTransport(url=graphql_uri, headers=self.get_basic_auth_headers( username=username, password=password ))
        self.client = Client(transport=self.transport, fetch_schema_from_transport=get_schema)
        # lets reduce the logging from gql
        for name in logging.root.manager.loggerDict:
            if name.startswith('gql'):
                logger = logging.getLogger(name) 
                logger.setLevel(logging.WARNING)

    def subscribe(self, query, var={}, username=None, password_file=None, password=None ):
        if password_file:
            password = self.get_password( password_file=password_file )
        self.connect_subscriber( username=username, password=password )
        for item in self.client.subscribe( gql(query), variable_values=var ):
            req = item['requests'].get('theRequest', {})
            optype = item['requests'].get("operationType", None )
            reqtype = req.get('reqtype', None)
            approval = req.get("approvalstatus", None)
            yield optype, reqtype, approval, req
            




class GraphQlLister(Lister, GraphQlClient):
    "Example of how to fetch data from graphql and dump output as a table"
    LOG = logging.getLogger(__name__)

    def __init__(self, app, app_args, cmd_name=None):
        super(GraphQlLister, self).__init__(app, app_args, cmd_name=cmd_name)
        self.connectGraphQl()
        


