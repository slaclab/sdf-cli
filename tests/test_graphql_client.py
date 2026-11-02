"""
Tests for GraphQlClient.connect_graph_ql: the HTTP transport must verify TLS certificates, because it carries
the sdf-bot basic-auth password. gql 3.x leaves AIOHTTPTransport unverified unless ssl= is passed explicitly.
"""

import warnings

from modules.utils.graphql import GraphQlClient


class TestConnectGraphQlTls:

    def test_transport_verifies_certificates(self):
        c = GraphQlClient()
        c.connect_graph_ql(graphql_uri='https://coact-dev.slac.stanford.edu/graphql-service', username='u', password='p')
        assert c.transport.ssl is True

    def test_no_unverified_ssl_warning(self):
        with warnings.catch_warnings():
            warnings.simplefilter('error')
            GraphQlClient().connect_graph_ql(graphql_uri='https://coact-dev.slac.stanford.edu/graphql-service')
