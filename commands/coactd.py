import os
import sys
import inspect
from enum import Enum
from typing import Any

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
    def run_playbook(self, playbook: str, private_data_dir: str = COACT_ANSIBLE_RUNNER_PATH, tags: str = '', **kwargs) -> ansible_runner.runner.Runner:
        r = ansible_runner.run( private_data_dir=private_data_dir, playbook=playbook, tags=tags, extravars=kwargs )
        self.LOG.debug(r.stats)
        if len(r.stats['failures']) > 0:
            raise Exception(f"playbook run failed: {r.stats}")
        return r
    def playbook_events(self,runner: ansible_runner.runner.Runner) -> dict:
        for e in runner.events:
            if 'event_data' in e:
                yield e['event_data']
    def playbook_task_res(self, runner: ansible_runner.runner.Runner, play: str, task: str) -> dict:
        for e in self.playbook_events(runner):
            #self.LOG.info(f"looking for {play} / {task}: {e}")
            if 'play' in e and play == e['play'] and 'task' in e and task == e['task'] and 'res' in e:
                return e['res']

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


class Registration(Command, GraphQlSubscriber, AnsibleRunner):
    'Base class for servicing Coact Requests'
    LOG = logging.getLogger(__name__)
    back_channel = None
    request_types = []

    def get_parser(self, prog_name):
        parser = super(Registration, self).get_parser(prog_name)
        parser.add_argument('--verbose', help='verbose output', required=False)
        parser.add_argument('--username', help='basic auth username for graphql service', default='sdf-bot')
        parser.add_argument('--password-file', help='basic auth password for graphql service', required=True)
        return parser

    def take_action(self, parsed_args):
        # connect
        self.back_channel = self.connect_graph_ql( username=parsed_args.username, password_file=parsed_args.password_file )
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
            try:

                if req_type in self.request_types:

                    result = self.do( req_id, op_type, req_type, approval, req )
                    if result:
                        self.LOG.info(f"Marking request {req_id} complete")
                        self.markCompleteRequest( req, 'AnsibleRunner completed' )
                        self.LOG.info(f"Done processing {req_id}")
                        
                else:
                    self.LOG.info(f"Ignoring {req_id}")

            except Exception as e:
                self.markIncompleteRequest( req, f'AnsibleRunner did not complete: {e}' )
                self.LOG.error(f"Error processing {req_id}: {e}")
        

    def do(self, req_id: str, op_type: Any, req_type: Any, approval: str, req: dict) -> bool:
        raise NotImplementedError('do() is abstract')
        return False


class UserRegistration(Registration):
    'workflow for user creation'
    request_types = [ 'UserAccount', ]

    USER_UPSERT_GQL = gql("""
        mutation userUpsert($user: UserInput!) {
            userUpsert(user: $user) {
                Id
            }
        }
        """)

    USER_STORAGE_GQL = gql("""
        mutation userStorageAllocationUpsert($user: UserInput!, $userstorage: UserStorageInput!) {
            userStorageAllocationUpsert(user: $user, userstorage: $userstorage) {
                Id
            }
        }
        """)

    REPO_ADD_USER_GQL = gql("""
        mutation repoAddUser($repo: RepoInput!, $user: UserInput!) {
            repoAddUser(repo: $repo, user: $user) { Id }
        }
        """)

    def do(self, req_id, op_type, req_type, approval, req):

        user = req.get('preferredUserName', None)
        facility = req.get('facilityname', None)
        eppn = req.get('eppn', None )
        assert user and facility and eppn

        # if the Request is valid, then run the ansible playbook, mark the request complete/failed, and send
        # email to all parties that its completed
        # make sure this is idempotent
        if approval in [ RequestStatus.APPROVED ]:

            playbook = 'add_user.yaml'

            self.LOG.info(f"Initiating {req_type} request for {user} at facility {facility} using {playbook}")

            # enable ldap
            runner = self.run_playbook( playbook, user=user, user_facility=facility, tags='ldap' )
            ldap_facts = self.playbook_task_res( runner, 'Create user', 'gather user ldap facts' )['ansible_facts']
            shell = self.run_playbook( playbook, user=user, user_facility=facility, tags='shell' )
            self.LOG.debug(f"ldap facts: {ldap_facts}")

            user_create_req = {
                'user': {
                    'username': user,
                    'eppns': [ eppn, ],
                    'shell': ldap_facts['ldap_user_default_shell'],
                    'preferredemail': eppn,
                    'uidnumber': int(ldap_facts['ldap_user_uidNumber']),
                    'fullname': ldap_facts['ldap_user_gecos'],
                }
            }
            self.LOG.debug(f"upserting user record {user_create_req}")
            user_id = self.back_channel.execute( self.USER_UPSERT_GQL, user_create_req ) 
            self.LOG.debug(f"upserted user {user_id}")

            # sshkeys
            runner = self.run_playbook( playbook, user=user, user_facility=facility, tags='sshkey' )

            # configure home directory; need force_copy_skel incase they already belong to another facility
            runner = self.run_playbook( playbook, user=user, user_facility=facility, tags='home', force_copy_skel=True )
            # TODO determine the storage paths and amount
            user_storage_req = {
                'user' : {
                    'username': user,
                },
                'userstorage': {
                    'username': user,
                    'purpose': "home",
                    'gigabytes': 25,
                    'storagename': "sdfhome",
                    'rootfolder': ldap_facts['ldap_user_homedir'],
                }
            }
            self.LOG.debug(f"upserting user storage record {user_storage_req}")
            self.back_channel.execute( self.USER_STORAGE_GQL, user_storage_req )

            # do any facility specific tasks
            runner = self.run_playbook( playbook, user=user, user_facility=facility, tags='facility' )

            # always register user with the facility's `default` Repo
            # Id references don't work in mutation for some reason
            add_user_req = {
                'repo': { 'facility': facility, 'name': 'default' },
                'user': { 'username': user },
            }
            self.LOG.debug(f"add to default repo {add_user_req}")
            self.back_channel.execute( self.REPO_ADD_USER_GQL, add_user_req )

            return True

        else:
            self.LOG.info(f"Ingoring {approval} state request")
            return None



class RepoRegistration(Registration):
    'workflow for repo creation'
    request_types = [ 'NewRepo', 'RepoMembership' ]

    REPO_USERS_GQL = gql("""
      query getRepoUsers ( $repo: RepoInput! ) {
        repo( filter: $repo ) {
            users
        }
      }""")

    REPO_UPSERT_GQL = gql("""
        mutation repoUpsert($repo: RepoInput! ) {
            repoUpsert(repo: $repo) {
                Id
            }
        }
        """)

    def do(self, req_id, op_type, req_type, approval, req):

        user = req.get('username', None)
        repo = req.get('reponame', None)
        facility = req.get('facilityname', None)
        principal = req.get('principal', None )

        if req_type == 'NewRepo':
            assert name and facility and principal
            if approval in [ RequestStatus.APPROVED ]:
                return self.do_new_repo( repo, facility, principal )

        elif req_type == 'RepoMembership':
            assert user and repo and facility
            if approval in [ RequestStatus.APPROVED ]:
                return self.do_repo_membership( user, repo, facility )

        return None

    def do_new_repo( self, repo: str, facility: str, principal: str, playbook: str="add_repo.yaml" ) -> bool:

        # add facility specific Repos
        runner = self.run_playbook( playbook, facility=facility, repo=repo )
        self.LOG.warn(f"add_repo.yaml: {runner}")

        # deal with storage

        # write back to coact the repo information
        repo_create_req = {
            'repo': {
                'name': name,
                'facility': facility,
                'principal': principal,
                'leaders': [ principal, ],
                'users': [ principal, ],
            }
        }
        self.LOG.info(f"upserting repo record {repo_create_req}")
        self.back_channel.execute( self.REPO_UPSERT_GQL, repo_create_req )

        # add compute record for repo

        return True

    def do_repo_membership( self, user: str, repo: str, facility: str, playbook: str="slurm-users.yaml" ) -> bool:

        # determine slurm account name; facility:repo
        account_name = f'{facility}:{repo}'.lower()

        # fetch for the list of all users for the repo
        runner = self.back_channel.execute( self.REPO_USERS_GQL, { 'repo': {'facility': facility, 'name': repo }} )
        users = runner['repo']['users']
        assert user in users

        users_str = ','.join(users)
        self.LOG.info(f"setting account {account_name} with users {users_str}")

        # run playbook to sync users to the slurm account
        runner = self.run_playbook( playbook, users=users_str, user_account=account_name )
        self.LOG.info(f"{playbook} output: {runner}")

        # ensure requested user has been added and the runner output defines user
        
        # deal with qoses for slurm account

        #self.back_channel.execute( self.REPO_UPSERT_GQL, repo_create_req )

        return True

    


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
        for cmd in [ UserRegistration, RepoRegistration, Get, ]:
            self.add_command( cmd.__name__.lower(), cmd )


