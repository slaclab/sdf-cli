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
from timeit import default_timer as timer

SDF_COACT_URI=getenv("SDF_COACT_URI", "coact-dev.slac.stanford.edu:443/graphql-service")

REQUEST_COMPLETE_MUTATION = gql('''mutation requestComplete( $Id: String!, $notes: String! ) { requestComplete( id: $Id, notes: $notes ) }''')
REQUEST_INCOMPLETE_MUTATION = gql('''mutation requestIncomplete( $Id: String!, $notes: String! ) { requestIncomplete( id: $Id, notes: $notes ) }''')

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

    def connect_graph_ql(self, graphql_uri='https://'+SDF_COACT_URI, get_schema=False, username=None, password_file=None, password=None , timeout=30):
        self.LOG.debug(f"connecting to {graphql_uri}")
        if password_file:
            password = self.get_password( password_file=password_file )
        self.transport = AIOHTTPTransport(url=graphql_uri, headers=self.get_basic_auth_headers( username=username, password=password ))
        self.client = Client(transport=self.transport, fetch_schema_from_transport=get_schema, execute_timeout=timeout)
        # lets reduce the logging from gql
        for name in logging.root.manager.loggerDict:
            if name.startswith('gql'):
                logger = logging.getLogger(name) 
                logger.setLevel(logging.WARNING)
        return self.client

    def query(self, query, var={} ):
        s = timer()
        self.LOG.debug(f"gql querying: {query}, vars: {var}")
        res = self.client.execute( gql(query), variable_values=var )
        e = timer()
        duration = e - s
        self.LOG.debug(f"gql query took s = timer()") 
        return res
    def mutate(self, query, var={} ):
        return self.query( query, var=var )

    def markCompleteRequest( self, req, notes ):
        return self.client.execute( REQUEST_COMPLETE_MUTATION, variable_values={ 'Id': req['Id'], 'notes': notes } )

    def markIncompleteRequest( self, req, notes ):
        return self.client.execute( REQUEST_INCOMPLETE_MUTATION, variable_values={ 'Id': req['Id'], 'notes': notes } )


class GraphQlSubscriber( GraphQlClient ):

    LOG = logging.getLogger(__name__)

    subscription_transport = None
    subscription_client = None

    def connect_subscriber(self, graphql_uri='wss://'+SDF_COACT_URI, get_schema=False, username=None, password_file=None, password=None, ping_interval=120, pong_timeout=60 ):
        self.LOG.info(f"connecting to {graphql_uri}")
        if password_file:
            password = self.get_password( password_file=password_file )
        self.subscription_transport = WebsocketsTransport(url=graphql_uri, headers=self.get_basic_auth_headers( username=username, password=password ), ping_interval=ping_interval, pong_timeout=pong_timeout)
        self.subscription_client = Client(transport=self.subscription_transport, fetch_schema_from_transport=get_schema)
        # lets reduce the logging from gql
        for name in logging.root.manager.loggerDict:
            if name.startswith('gql'):
                logger = logging.getLogger(name) 
                logger.setLevel(logging.WARNING)
        return self.subscription_client

    def subscribe(self, query, var={}):
        for item in self.subscription_client.subscribe( gql(query), variable_values=var ):
            #self.LOG.info(f"GOT {item}")
            optype = item.get("operationType", None )
            if not optype in [ 'delete', ]:
                this = item.get('requests',{})
                req = this.get('theRequest', {})
                if req:
                    req_id = req.get('Id', None)
                    reqtype = req.get('reqtype', None)
                    approval = req.get("approvalstatus", None)
                    yield req_id, optype, reqtype, approval, req
        return None, None, None, None, {}
            




class GraphQlLister(Lister, GraphQlClient):
    "Example of how to fetch data from graphql and dump output as a table"
    LOG = logging.getLogger(__name__)

    def __init__(self, app, app_args, cmd_name=None):
        super(GraphQlLister, self).__init__(app, app_args, cmd_name=cmd_name)
        self.connectGraphQl()
        


