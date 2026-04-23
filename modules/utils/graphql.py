"""
GraphQL client utilities for the SDF CLI.

This module provides GraphQL client and subscriber classes for connecting
to the Coact GraphQL service.
"""

from os import getenv
import logging
import base64
from timeit import default_timer as timer

from gql import gql, Client
from gql.transport.aiohttp import AIOHTTPTransport
from gql.transport.websockets import WebsocketsTransport

from loguru import logger

# Suppress noisy gql loggers (they use standard logging)
from gql.transport.requests import log as requests_logger
requests_logger.setLevel(logging.ERROR)

websockets_logger = logging.getLogger('gql.transport.websockets')
websockets_logger.setLevel(logging.WARNING)

SDF_COACT_URI = getenv("SDF_COACT_URI", "coact-dev.slac.stanford.edu:443/graphql-service")

REQUEST_COMPLETE_MUTATION = gql('''mutation requestComplete( $Id: String!, $notes: String! ) { requestComplete( id: $Id, notes: $notes ) }''')
REQUEST_INCOMPLETE_MUTATION = gql('''mutation requestIncomplete( $Id: String!, $notes: String! ) { requestIncomplete( id: $Id, notes: $notes ) }''')


class GraphQlClient:
    """GraphQL client for connecting to and querying the Coact GraphQL service."""

    transport = None
    client = None

    def get_password(self, password_file=None):
        password = None
        with open(password_file, 'r') as f:
            password = f.read()
        return password

    def get_basic_auth_headers(self, username=None, password=None):
        headers = {}
        if username and password:
            mux = f'{username}:{password}'.encode("ascii")
            headers = {'Authorization': f'Basic {base64.b64encode(mux).decode("ascii")}'}
        return headers

    def connect_graph_ql(self, graphql_uri='https://'+SDF_COACT_URI, get_schema=False, username=None, password_file=None, password=None, timeout=30):
        logger.trace(f"GraphQL connect: uri={graphql_uri}, username={username}, timeout={timeout}s")
        logger.trace(f"GraphQL connect: password_file={password_file}, get_schema={get_schema}")
        if password_file:
            password = self.get_password(password_file=password_file)
            logger.trace(f"GraphQL connect: loaded password from file (length={len(password.strip()) if password else 0})")
        logger.trace(f"GraphQL connect: creating AIOHTTPTransport to {graphql_uri}")
        self.transport = AIOHTTPTransport(url=graphql_uri, headers=self.get_basic_auth_headers(username=username, password=password))
        logger.trace(f"GraphQL connect: creating Client with execute_timeout={timeout}")
        self.client = Client(transport=self.transport, fetch_schema_from_transport=get_schema, execute_timeout=timeout)
        logger.trace(f"GraphQL connect: successfully connected to {graphql_uri}")
        # Suppress gql library logging
        for name in logging.root.manager.loggerDict:
            if name.startswith('gql'):
                logging.getLogger(name).setLevel(logging.WARNING)
        return self.client

    def query(self, query, var={}):
        s = timer()
        logger.trace(f"GraphQL query: {query}")
        logger.trace(f"GraphQL query vars: {var}")
        res = self.client.execute(gql(query), variable_values=var)
        e = timer()
        duration = e - s
        logger.trace(f"GraphQL query completed in {duration:.3f}s")
        logger.trace(f"GraphQL query result: {res}")
        return res

    def mutate(self, query, var={}):
        return self.query(query, var=var)

    def markCompleteRequest(self, req, notes):
        logger.trace(f"GraphQL markCompleteRequest: Id={req['Id']}, notes={notes}")
        result = self.client.execute(REQUEST_COMPLETE_MUTATION, variable_values={'Id': req['Id'], 'notes': notes})
        logger.trace(f"GraphQL markCompleteRequest result: {result}")
        return result

    def markIncompleteRequest(self, req, notes):
        logger.trace(f"GraphQL markIncompleteRequest: Id={req['Id']}, notes={notes}")
        result = self.client.execute(REQUEST_INCOMPLETE_MUTATION, variable_values={'Id': req['Id'], 'notes': notes})
        logger.trace(f"GraphQL markIncompleteRequest result: {result}")
        return result


class GraphQlSubscriber(GraphQlClient):
    """GraphQL subscriber for WebSocket-based subscriptions."""

    subscription_transport = None
    subscription_client = None

    def connect_subscriber(self, graphql_uri='wss://'+SDF_COACT_URI, get_schema=False, username=None, password_file=None, password=None, ping_interval=120, pong_timeout=60):
        logger.trace(f"GraphQL subscriber connect: uri={graphql_uri}, username={username}")
        logger.trace(f"GraphQL subscriber connect: ping_interval={ping_interval}, pong_timeout={pong_timeout}")
        if password_file:
            password = self.get_password(password_file=password_file)
            logger.trace("GraphQL subscriber connect: loaded password from file")
        logger.trace(f"GraphQL subscriber connect: creating WebsocketsTransport to {graphql_uri}")
        self.subscription_transport = WebsocketsTransport(
            url=graphql_uri,
            headers=self.get_basic_auth_headers(username=username, password=password),
            ping_interval=ping_interval,
            pong_timeout=pong_timeout
        )
        logger.trace("GraphQL subscriber connect: creating Client")
        self.subscription_client = Client(transport=self.subscription_transport, fetch_schema_from_transport=get_schema)
        logger.trace(f"GraphQL subscriber connect: successfully connected to {graphql_uri}")
        # Suppress gql library logging
        for name in logging.root.manager.loggerDict:
            if name.startswith('gql'):
                logging.getLogger(name).setLevel(logging.WARNING)
        return self.subscription_client

    def subscribe(self, query, var={}):
        for item in self.subscription_client.subscribe(gql(query), variable_values=var):
            optype = item.get("operationType", None)
            if optype not in ['delete']:
                this = item.get('requests', {})
                req = this.get('theRequest', {})
                if req:
                    req_id = req.get('Id', None)
                    reqtype = req.get('reqtype', None)
                    approval = req.get("approvalstatus", None)
                    yield req_id, optype, reqtype, approval, req
        return None, None, None, None, {}
