import os
import sys
import inspect
from enum import Enum

from cliff.command import Command
from cliff.commandmanager import CommandManager

from .utils.graphql import GraphQlSubscriber
#from .utils.ldap import client as ldap_client

import jinja2
import smtplib
from email.message import EmailMessage

from gql import gql

import ansible_runner

import logging

COACT_ANSIBLE_RUNNER_PATH = './ansible-runner/'

# order of class inherietence important: https://stackoverflow.com/questions/58608361/string-based-enum-in-python
class RequestStatus(str,Enum):
  APPROVED = 'Approved'
  NOT_ACTED_ON = 'NotActedOn'
  REJECTED = 'Rejected'
  COMPLETED = 'Complete'
  INCOMPLETE = 'Incomplete'


class AnsibleRunner():
    LOG = logging.getLogger(__name__) 
    def run_playbook(self, playbook, private_data_dir=COACT_ANSIBLE_RUNNER_PATH, tags='', **kwargs):
        r = ansible_runner.run( private_data_dir=private_data_dir, playbook=playbook, tags=tags, extravars=kwargs )
        self.LOG.info(r.stats)
        if len(r.stats['failures']) > 0:
            raise Exception(f"playbook run failed: {r.stats}")
        return r

class EmailRunner():
    LOG = logging.getLogger(__name__)
    smtp_server = None
    subject_prefix = '[Coact] '
    j2 = jinja2.Environment()
    
    def send_email(self, receiver, body, sender='s3df-help@slac.stanford.edu', subject=None, smtp_server=None, vars={} ):
        msg = EmailMessage()
        msg['Subject'] = self.subject_prefix + str(subject)
        msg['From'] = sender
        msg['To'] = receiver
        #msg.set_content( render_fstring(body) )
        t = self.j2.from_string( body )
        msg.set_content( t.render(**vars) )
        server = smtp_server if smtp_server else self.smtp_server
        if not server:
            raise Exception("No smtp server configured")
        s = smtplib.SMTP(server)
        self.LOG.info(f"sending email {msg}")
        s.send_message(msg)
        return s.quit()



class UserRegistration(Command,GraphQlSubscriber,AnsibleRunner,EmailRunner):
    'workflow for user creation'
    LOG = logging.getLogger(__name__)

    def get_parser(self, prog_name):
        parser = super(UserRegistration, self).get_parser(prog_name)
        parser.add_argument('--verbose', help='verbose output', required=False)
        parser.add_argument('--username', help='basic auth username for graphql service', default='sdf-cli')
        parser.add_argument('--password-file', help='basic auth password for graphql service', required=True)
        parser.add_argument('--smtp-server', help='smtp relay address', default='smtp.slac.stanford.edu')
        return parser

    def take_action(self, parsed_args):

        # connect
        back_channel = self.connect_graph_ql( username=parsed_args.username, password_file=parsed_args.password_file )
        q = """
            subscription {
                requests {
                    theRequest {
                        Id
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
        """

        sub = self.connect_subscriber( username=parsed_args.username, password=self.get_password(parsed_args.password_file ) )
        for req_id, op_type, req_type, approval, req in self.subscribe( q ):
            self.LOG.info(f"Processing {req_id}: {op_type} {req_type} - {approval}: {req}")

            v = {}

            if req_type == 'UserAccount':

                user = req.get('preferredUserName', None)
                facility = req.get('facilityname', None)

                if not user or not facility:
                    raise Exception('No valid username or user_facility present in request')

                # if the Request is valid, then run the ansible playbook, mark the request complete/failed, and send
                # email to all parties that its completed
                # make sure this is idempotent
                elif approval in [ RequestStatus.APPROVED ]:

                    try:
                        playbook = 'add_user.yaml'

                        ansible_output = self.run_playbook( playbook, user=user, user_facility=facility, tags='ldap' )
                        ansible_output = self.run_playbook( playbook, user=user, user_facility=facility, tags='home' )
                        ansible_output = self.run_playbook( playbook, user=user, user_facility=facility, tags='facility' )
                        self.LOG.info(f"Marking request {req_id} complete")
                        self.markCompleteRequest( req, 'AnsibleRunner completed' )

                    except Exception as e:
                        self.LOG.error( f'Request {req_id} failed to complete: {e}' )
                        self.markIncompleteRequest( req, 'AnsibleRunner did not complete' )

                else:
                    self.LOG.warn(f"Ingoring {approval} state request")

            else:
                self.LOG.warn(f"Ignoring request type {req_type}")

            self.LOG.warning(f"Done processing {req_id}")



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
        for cmd in [ UserRegistration, Get, ]:
            self.add_command( cmd.__name__.lower(), cmd )


