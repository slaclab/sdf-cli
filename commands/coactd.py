import os
import sys

from cliff.command import Command
from cliff.commandmanager import CommandManager

from .utils.graphql import GraphQlSubscriber
#from .utils.ldap import client as ldap_client

import smtplib
from email.message import EmailMessage

import ansible_runner

import logging

COACT_ANSIBLE_RUNNER_PATH = './ansible-runner/'

class AnsibleRunner():
    LOG = logging.getLogger(__name__) 
    def run_playbook(self, playbook, private_data_dir=COACT_ANSIBLE_RUNNER_PATH, **kwargs):
        r = ansible_runner.run( private_data_dir=private_data_dir, playbook=playbook, extravars=kwargs )
        self.LOG.info(r.stats)
        if len(r.stats['failures']) > 0:
            raise Exception(f"playbook run failed: {r.stats}")


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
  



class UserRegistration(Command,GraphQlSubscriber,AnsibleRunner):
    'workflow for user creation'
    LOG = logging.getLogger(__name__)

    def get_parser(self, prog_name):
        parser = super(UserRegistration, self).get_parser(prog_name)
        parser.add_argument('--verbose', help='verbose output', required=False)
#        parser.add_argument('--ldap-server', help='ldaps://sdfldap001.slac.stanford.edu')
#        parser.add_argument('--ldap-binddn', help='ldap admin user dn', default='uid=ldap-coact,ou=Admin,dc=sdf,dc=slac,dc=stanford,dc=edu')
#        parser.add_argument('--ldap-bindpw-file', help='filepath containing ldap admin password', required=True)
        parser.add_argument('--username', help='basic auth username for graphql service', default='sdf-cli', required=True)
        parser.add_argument('--password-file', help='basic auth password for graphql service', required=True)
        return parser

    def take_action(self, parsed_args):

        self.back_channel = self.connect_graph_ql( username=parsed_args.username, password_file=parsed_args.password_file )

        sub = self.subscribe( """
            subscription {
                requests {
                    theRequest {
                        reqtype
                        approvalstatus
                        eppn
                        preferredUserName
                        reponame
                        facilityname
                        principal
                        username
                        actedat
                        actedby
                        requestedby
                        timeofrequest
                    }
                    operationType
                }
            }
          """,
          username=parsed_args.username, password_file=parsed_args.password_file
        )

        for op_type, req_type, approval, req in sub:
            try:
                self.LOG.warning(f"GOT {op_type} {req_type} - {approval}: {req}")
                user = req.get('preferredUserName', None)
                facility = req.get('facilityname', None)
                if not user or not facility:
                    raise Exception('No valid username or user_facility present in request')
                if approval == 'Approved':
                    self.run_playbook( 'add_user.yaml', user='pav', user_facility='rubin' )
            except Exception as e:
                self.LOG.error( f'{e}' )



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


