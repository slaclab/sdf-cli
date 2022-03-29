from cliff.command import Command
from cliff.commandmanager import CommandManager

from .utils.graphql import GraphQlLister

from functools import reduce
from operator import ior

import json
import re

import logging

class List(GraphQlLister):
    "show all users"
    def get_parser(self, prog_name):
        parser = super(List, self).get_parser(prog_name)
        #parser.add_argument('dn', default='ou=Group,dc=reg,o=slac')
        return parser

    def take_action(self, parsed_args):

        # TODO: add filtering

        res = self.query("""
        { users { uid uidNumber eppns } }
        """)

        # filter example
        # { repos(filters:{ name: "bd" } ) { name gid users }}
        
        return (
            ('UID', 'UIDNumber', 'EPPNs'),
            ((r['uid'], r['uidNumber'], ','.join(r['eppns'])) for r in res['users'])
        )


class ListMine(GraphQlLister):
    "show list of Repos that you have Principal or Leader roles on"

class Add(GraphQlLister):
    "add a User"
    def get_parser(self, prog_name):
        parser = super(Add, self).get_parser(prog_name)
        parser.add_argument('--uid', '-u', required=True, help="uid/username")
        parser.add_argument('--uidNumber', '-n', help="uid number")
        parser.add_argument('--eppns', nargs="*", help="authenticated eppns/email addresses")
        return parser

    def take_action(self, parsed_args):
        res = self.query("""
            mutation{
              createUser( data: {
                uid: "%s",
                uidNumber: %s,
                eppns: [%s]
              }) {
                user {
                  uid eppns uidNumber
                }
              }
            }
        """ % ( parsed_args.uid, parsed_args.uidNumber, ','.join([ '"'+e+'"' for e in parsed_args.eppns ]) ) )
        self.LOG.error(f"RES: {res}")
        r = res['createUser']['user']
        return (
            ('UID', 'UIDNumber', 'EPPNs'),
            ((r['uid'], r['uidNumber'], ','.join(r['eppns'])),)
        )

class Delete(GraphQlLister):
    "delete a User"
    def get_parser(self, prog_name):
        parser = super(Delete, self).get_parser(prog_name)
        parser.add_argument('--uid', '-u', required=True, help="uid/username")
        return parser

    def take_action(self, parsed_args):
        res = self.query("""
            mutation{
              deleteUser( data: {
                uid: "%s"
              }) {
                user {
                  uid eppns uidNumber
                }
              }
            }
        """ % ( parsed_args.uid, ) )
        self.LOG.error(f"RES: {res}")
        r = res['deleteUser']['user']
        return (
            ('UID', 'UIDNumber', 'EPPNs'),
            ((r['uid'], r['uidNumber'], ','.join(r['eppns'])),)
        )
         

class Update(GraphQlLister):
    "modify a User record"
    def get_parser(self, prog_name):
        parser = super(Update, self).get_parser(prog_name)
        parser.add_argument('--uid', '-u', required=True, help="uid/username")
        parser.add_argument('--uidNumber', '-n', help="uid number")
        parser.add_argument('--eppns', nargs="*", help="authenticated eppns/email addresses")
        return parser

    def take_action(self, parsed_args):
        user = self.query("""
            {
              users( filters: { uid: "%s" } ) {
                id uid uidNumber eppns
              }
            }
        """ % (parsed_args.uid) )
        if len( user['users'] ) == 1:
            remote = user['users'][0]
            d = {}
            for k,v in vars( parsed_args ).items():
                if k in ( 'uid', 'uidNumber', 'eppns' ) and not v == None:
                    d[k] = v
            l = [ remote, d ]
            merged = reduce(ior, l, {})
            #self.LOG.warning(f"REMOTE: {remote}, LOCAL: {d} -> MEGED {merged}")
            if 'uidNumber' in merged:
                merged['uidNumber'] = int(merged['uidNumber'])
            json_string = json.dumps(merged)
            # strip out " in keys
            r = r'\s*\"(\w+)\":\s+'
            query_string = re.sub( r, r"\1: ", json_string)
            #self.LOG.warning(f"QUERY: {query_string}") 
            q = """mutation {
              updateUser( data: %s ) {
                 user { id uid uidNumber eppns }
              }
            }
            """ % (query_string,)
            res = self.query(q)
            r = res['updateUser']['user']
            #self.LOG.warning(f"r {r}")
            return (
                ('UID', 'UIDNumber', 'EPPNs'),
                ((r['uid'], r['uidNumber'], ','.join(r['eppns'])),)
            )
        

#        res = self.query("""
#            mutation{
#              deleteUser( data: {
#                uid: "%s"
#              }) {
#                user {
#                  uid eppns uidNumber
#                }
#              }
#            }
#        """ % ( parsed_args.uid, ) )

class User(CommandManager):
    "Manage Users"

    LOG = logging.getLogger(__name__)

    def __init__(self, namespace, convert_underscores=True):
        super(User,self).__init__(namespace, convert_underscores=convert_underscores)
        for cmd in [ List, Add, Delete, Update ]:
            self.add_command( cmd.__name__.lower(), cmd )


