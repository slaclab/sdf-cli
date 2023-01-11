import os
import sys

from cliff.command import Command
from cliff.commandmanager import CommandManager

from .utils.graphql import GraphQlSubscriber
from .utils.ldap import client as ldap_client

import smtplib
from email.message import EmailMessage


import logging


class EmailNotifications(Command,GraphQlSubscriber):
    'sends email notifications from requests'
    LOG = logging.getLogger(__name__)

    def get_parser(self, prog_name):
        parser = super(EmailNotifications, self).get_parser(prog_name)
        parser.add_argument('--verbose', help='verbose output', required=False)
        parser.add_argument('--smtp-server', help='smtp relay address', default='smtp.slac.stanford.edu')
        return parser

    def send_email(self, receiver, body, sender='sdf-help@slac.stanford.edu', subject=None ):
        msg = EmailMessage()
        msg['Subject'] = subject
        msg['From'] = sender
        msg['To'] = receiver
        msg.set_content(body)
        s = smtplib.SMTP(self.smtp_server)
        self.LOG.warning(f"sending email {msg}")
        s.send_message(msg)
        return s.quit()

    def take_action(self, parsed_args):
        # set global values
        self.smtp_server = parsed_args.smtp_server

        # Provide a GraphQL query
        res = self.subscribe("""
            subscription {
                requests {
                    theRequest {
                    reqtype
                    eppn
                    preferredUserName
                    }
                }
            }
        """)
        for result in res:
            self.LOG.warning(f"GOT {result}")
            self.send_email( 
                "ytl@slac.stanford.edu",
                f"{result}",
                subject='test email',
            )
  

class UserRegistration(Command,GraphQlSubscriber):
    'workflow for user creation'
    LOG = logging.getLogger(__name__)

    def get_parser(self, prog_name):
        parser = super(UserRegistration, self).get_parser(prog_name)
        parser.add_argument('--verbose', help='verbose output', required=False)
        parser.add_argument('--ldap-server', help='ldaps://sdfldap001.slac.stanford.edu')
        parser.add_argument('--ldap-binddn', help='ldap admin user dn', default='uid=ldap-coact,ou=Admin,dc=sdf,dc=slac,dc=stanford,dc=edu')
        parser.add_argument('--ldap-bindpw-file', help='filepath containing ldap admin password', required=True)
        return parser

    def take_action(self, parsed_args):

        self.args = parsed_args

        res = self.subscribe("""
            subscription {
                requests {
                    theRequest {
                    reqtype
                    eppn
                    preferredUserName
                    }
                }
            }
        """)
        for result in res:
            self.LOG.warning(f"GOT {result}")

            # idempotent workflow
            this_username = 'ytl'


            # make sure home directory exists
            self.ensure_home_directories( this_username )

            # add to general access for s3df
            self.ensure_ldap_memberOf( this_username, 'sdf-users') # TODO full dn

            # for each group the user is in, add to posixGroup
            for g in groups:
                self.ensure_ldap_memberOf( this_username, g )


    def ensure_home_directories(self, username):

        # 0. check the user is valid (running id)

        # 1. ensure that the directory exists

        # 2. setup quota

        return

    def ensure_ldap_memberOf( self, username, group ):

        client = ldap_client( self.args.ldap_server, binddn=self.args.ldap_binddn, password_file=self.args.ldap_bindpw_file )
        with client as ldap:
            res = ldap.search(group)
            for r in res:
                self.LOG.warn( f'FOUND {r}' )

            # check if user is in group

            # append to list if not

            # save

        return

    def remove_ldap_memberOf( self, usermame, group ):
        return



class Get(Command,GraphQlSubscriber):
    'just streams output from requests subscription'
    LOG = logging.getLogger(__name__)

    def get_parser(self, prog_name):
        parser = super(Get, self).get_parser(prog_name)
        parser.add_argument('--verbose', help='verbose output', required=False)
        return parser

    def take_action(self, parsed_args):
        # Provide a GraphQL query
        res = self.subscribe("""
            subscription {
                requests {
                    theRequest {
                    reqtype
                    eppn
                    preferredUserName
                    }
                }
            }
        """)
        for result in res:
            print (result)




class Coactd(CommandManager):
    "A Manager class to register sub commands"
    LOG = logging.getLogger(__name__)

    def __init__(self, namespace, convert_underscores=True):
        super(Coactd,self).__init__(namespace, convert_underscores=convert_underscores)
        for cmd in [ EmailNotifications, UserRegistration, Get, ]:
            self.add_command( cmd.__name__.lower(), cmd )


