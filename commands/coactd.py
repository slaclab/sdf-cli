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



NEW_USER_REQUEST_EMAIL = """
Dear {facility} Czar,

User {user} ({eppn}) has requested membership of your facility. In order to proceed, you must approve or deny their request at

https://coact-dev.slac.stanford.edu/requests

Thanks,

Team S3DF
"""

NEW_USER_COMPLETE_CZAR_EMAIL = """
Dear {facility} Czar,

User {user}'s S3DF account registration has been completed. They may access S3DF immediately.

Please note that access to storage and batch resources will require users to be assigned to Repo's in Coact. More details at...

Thanks,

Team S3DF
"""

NEW_USER_COMPLETE_USER_EMAIL = """
Dear S3DF user,

Your account registration is complete. 

Connection information is available at https://s3df.slac.stanford.edu/public/doc/#/accounts-and-access?id=how-to-connect.

Questions and issues may be directed to s3df-help@slac.stanford.edu.

Thanks,

Team S3DF
"""

class render_fstring:
    def __init__(self, payload):
        self.payload = payload
    def __str__(self):
        vars = inspect.currentframe().f_back.f_globals.copy()
        vars.update(inspect.currentframe().f_back.f_locals)
        return self.payload.format(**vars)


class AnsibleRunner():
    LOG = logging.getLogger(__name__) 
    def run_playbook(self, playbook, private_data_dir=COACT_ANSIBLE_RUNNER_PATH, **kwargs):
        r = ansible_runner.run( private_data_dir=private_data_dir, playbook=playbook, extravars=kwargs )
        self.LOG.info(r.stats)
        if len(r.stats['failures']) > 0:
            raise Exception(f"playbook run failed: {r.stats}")
        return r

class EmailRunner():
    LOG = logging.getLogger(__name__)
    smtp_server = None
    subject_prefix = '[Coact] '
    
    def send_email(self, receiver, body, sender='s3df-help@slac.stanford.edu', subject=None, smtp_server=None ):
        msg = EmailMessage()
        msg['Subject'] = self.subject_prefix + str(subject)
        msg['From'] = sender
        msg['To'] = receiver
        msg.set_content( render_fstring(body) )
        server = smtp_server if smpt_server else self.smtp_server
        if not server:
            raise Exception("No smtp server configured")
        s = smtplib.SMTP(server)
        self.LOG.warning(f"sending email {msg}")
        s.send_message(msg)
        return s.quit()


class EmailNotifications(Command,GraphQlSubscriber,EmailRunner):
    'sends email notifications from requests'
    LOG = logging.getLogger(__name__)

    def get_parser(self, prog_name):
        parser = super(EmailNotifications, self).get_parser(prog_name)
        parser.add_argument('--verbose', help='verbose output', required=False)
        parser.add_argument('--smtp-server', help='smtp relay address', default='smtp.slac.stanford.edu')
        return parser

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
            self.send_email( 
                "ytl@slac.stanford.edu",
                f"{result}",
                subject='test email',
            )
  



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

        # config email
        self.smtp_server = parsed_args.smtp_server

        # connect
        self.connect_graph_ql( username=parsed_args.username, password_file=parsed_args.password_file )
        sub = self.subscribe( """
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
          """,
          username=parsed_args.username, password_file=parsed_args.password_file
        )

        for req_id, op_type, req_type, approval, req in sub:
            self.LOG.info(f"Processing {req_id}: {op_type} {req_type} - {approval}: {req}")
            try:

                if req_type == 'UserAccount':

                    user = req.get('preferredUserName', None)
                    facility = req.get('facilityname', None)
                    if not user or not facility:
                        raise Exception('No valid username or user_facility present in request')

                    # determine the czars
                    # TODO
                    czars = [ 'ytl@slac.stanford.edu', ]
                    eppn = 'ytl@slac.stanford.edu'

                    if approval in [ 'NotActedOn', ]:
                        self.LOG.info("Sending email to czars about new user request")
                        self.send_email( czars, NEW_USER_REQUEST_EMAIL, subject=f'New User Request for {user}' )

                    # if the Request is valid, then run the ansible playbook, mark the request complete/failed, and send
                    # email to all parties that its completed
                    elif approval in [ 'Approved', 'Incomplete', 'Complete' ]:
                        ansible_output = self.run_playbook( 'add_user.yaml', user='pav', user_facility='rubin' )
                        self.LOG.info(f"Marking request {req_id} complete")
                        self.markCompleteRequest( req, 'AnsibleRunner completed' )
                        self.send_email( czars, NEW_USER_COMPLETE_CZAR_EMAIL, subject=f'User {user} registration complete' ) 
                        self.send_email( eppn, NEW_USER_COMPLETE__USER_EMAIL, subject=f'Your S3DF account registration is complete' ) 


                    else:
                        self.LOG.warn(f"dunno what to do with {approval} state")

                else:
                    self.LOG.warn(f"Not acting on req_type {req_type}")

            except Exception as e:
                self.LOG.error( f'Request {req_id} failed to complete: {e}' )
                self.markIncompleteRequest( req, 'AnsibleRunner did not complete' )

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
        for cmd in [ EmailNotifications, UserRegistration, Get, ]:
            self.add_command( cmd.__name__.lower(), cmd )


