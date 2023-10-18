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
import datetime
from dateutil import parser
from dateutil.relativedelta import relativedelta

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
    ident = None # use this to create a directory for the ansible run output of the same name (eg the coact request id)
    def run_playbook(self, playbook: str, private_data_dir: str = COACT_ANSIBLE_RUNNER_PATH, tags: str = 'all', **kwargs) -> ansible_runner.runner.Runner:
        r = ansible_runner.run( 
            private_data_dir=private_data_dir,
            playbook=playbook, 
            tags=tags, 
            extravars=kwargs,
            ident=f'{self.ident}_{tags}',
            cancel_callback=lambda: None
        )
        self.LOG.debug(r.stats)
        if not r.rc == 0:
            raise Exception(f"AnsibleRunner failed")
        return r
    def playbook_events(self,runner: ansible_runner.runner.Runner) -> dict:
        for e in runner.events:
            if 'event_data' in e:
                yield e['event_data']
    def playbook_task_res(self, runner: ansible_runner.runner.Runner, play: str, task: str) -> dict:
        for e in self.playbook_events(runner):
            # self.LOG.info(f"looking for {play} / {task}: {e}")
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

    SUBSCRIPTION_STR = """
        subscription( $clientName: String ) {
            requests( clientName: $clientName ) {
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
                    shell
                    clustername
                    start
                    end
                }
                operationType
            }
        }
    """

    def get_parser(self, prog_name):
        parser = super(Registration, self).get_parser(prog_name)
        parser.add_argument('--verbose', help='verbose output', required=False)
        parser.add_argument('--username', help='basic auth username for graphql service', default='sdf-bot')
        parser.add_argument('--password-file', help='basic auth password for graphql service', required=True)
        parser.add_argument('--client-name', help='subscriber queue name to connect to', default=f'sdf-bot-{self.__class__.__name__}')
        return parser

    def take_action(self, parsed_args):
        # connect
        self.back_channel = self.connect_graph_ql( username=parsed_args.username, password_file=parsed_args.password_file )
        sub = self.connect_subscriber( username=parsed_args.username, password=self.get_password(parsed_args.password_file ) )
        for req_id, op_type, req_type, approval, req in self.subscribe( self.SUBSCRIPTION_STR, var={"clientName": parsed_args.client_name} ):
            self.LOG.info(f"Processing {req_id}: {op_type} {req_type} - {approval}: {req}")
            self.ident = req_id # set the request id for ansible runner
            try:

                if req_type in self.request_types:

                    result = self.do( req_id, op_type, req_type, approval, req )
                    if result:
                        self.LOG.info(f"Marking request {req_id} complete")
                        self.markCompleteRequest( req, f'Request {self.ident} completed' )
                        self.LOG.info(f"Done processing {req_id}")
                    else:
                        self.LOG.warning(f"Unknown return for {req_id}")
                        
                else:
                    self.LOG.info(f"Ignoring {req_id}")

            except Exception as e:
                self.markIncompleteRequest( req, f'Request {self.ident} did not complete: {e}' )
                self.LOG.error(f"Error processing {req_id}: {e}")
        

    def do(self, req_id: str, op_type: Any, req_type: Any, approval: str, req: dict) -> bool:
        raise NotImplementedError('do() is abstract')
        return False


class UserRegistration(Registration):
    'workflow for user creation'
    request_types = [ 'UserAccount', 'UserChangeShell', ]

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

    USER_CHANGE_SHELL_GQL = gql("""
        mutation userUpdate($user: UserInput!) {
            userUpdate(user: $user) { Id }
        }
        """)

    def do(self, req_id, op_type, req_type, approval, req):

        user = req.get('preferredUserName', None)
        facility = req.get('facilityname', None)
        eppn = req.get('eppn', None)

        if approval in [ RequestStatus.APPROVED ]:

            if req_type == 'UserAccount':
                assert user and facility and eppn
                return self.do_new_user( user, eppn, facility )
            elif req_type == 'UserChangeShell':
                user = req.get('username', None)
                shell = req.get('shell', None)
                assert user and shell 
                return self.do_change_shell(user, shell)

        else:
            self.LOG.info(f"Ingoring {approval} state request")
            return None

    def do_change_shell(self, user: str, shell: str, playbook: str="set_user_shell.yaml") -> bool:
        self.LOG.info(f"Changing shell for user {user} using {playbook}")
        runner = self.run_playbook( playbook, user=user, user_login_shell=shell )
        user_id = self.back_channel.execute( self.USER_CHANGE_SHELL_GQL, {'user':{"username": user, "shell": shell}} )

        return True

    def do_new_user( self, user: str, eppn: str, facility: str, playbook: str="add_user.yaml" ) -> bool:

        self.LOG.info(f"Creating user {user} at facility {facility} using {playbook}")

        # enable ldap
        runner = self.run_playbook( playbook, user=user, user_facility=facility, tags='ldap' )
        self.LOG.error(f"FACTS: {self.playbook_task_res( runner, 'Create user', 'gather user ldap facts' )}")
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

        # configure home directory; need force_copy_skel incase they already belong to another facility
        runner = self.run_playbook( playbook, user=user, user_facility=facility, tags='home', force_copy_skel=False ) #True )
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

        # sshkeys
        runner = self.run_playbook( playbook, user=user, user_facility=facility, tags='sshkey' )

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



class RepoRegistration(Registration):
    'workflow for repo maintenance'
    request_types = [ 'NewRepo', 'RepoMembership', 'RepoRemoveUser', 'RepoChangeComputeRequirement', 'RepoComputeAllocation' ]

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

    COMPUTE_ALLOCATION_UPSERT_GQL = gql("""
        mutation repoComputeAllocationUpsert($repo: RepoInput!, $repocompute: RepoComputeAllocationInput!, $qosinputs: [QosInput!]!) {
            repoComputeAllocationUpsert(repo: $repo, repocompute: $repocompute, qosinputs: $qosinputs) {
                Id
            } 
        }
        """)

    REPO_APPEND_USER_GQL = gql("""
        mutation repoAppendMember($repo: RepoInput!, $user: UserInput!) {
            repoAppendMember(repo: $repo, user: $user) {
                Id
            }
        }
        """)

    REPO_CURRENT_COMPUTE_REQUIREMENT_GQL = gql("""
        query repo( $repo: RepoInput! ) {
          repo(filter: $repo) {
            Id
            name
            facility
            users
            computerequirement
            currentComputeAllocations {
              Id
              clustername
              start
              end
              percentOfFacility
            }
          }
        }
        """)

    FACILITY_CURRENT_COMPUTE_CGL = gql("""
        query facility( $facility: String! ) {
          facility(filter: {name: $facility}) {
            name
            computeallocations {
              clustername
            }
            computepurchases {
              clustername
              purchased
            }
          }
        }
    """)

    REPO_CURRENT_COMPUTE_ALLOCATIONS_CGL = gql("""
        query repo( $facility: String!, $repo: String! ) {
          repo(filter: {facility: $facility, name: $repo}) {
            facility
            name
            currentComputeAllocations {
              clustername
              end
              start
            }
          }
        }
        """)

    REPO_COMPUTE_ALLOCATION_UPSERT_GQL = gql("""
        mutation repo( $repo: RepoInput!, $repocompute: RepoComputeAllocationInput! ) {
          repoComputeAllocationUpsert( repo: $repo, repocompute: $repocompute ){
            Id 
            currentComputeAllocations {
              Id
              clustername
              end
              start
            }
          }
        }
    """)

    def do(self, req_id, op_type, req_type, approval, req):

        user = req.get('username', None)
        repo = req.get('reponame', None)
        facility = req.get('facilityname', None)
        principal = req.get('principal', None )

        if approval in [ RequestStatus.APPROVED ]:

            if req_type == 'NewRepo':
                assert repo and facility and principal
                return self.do_new_repo( repo, facility, principal )

            elif req_type in ('RepoMembership','RepoRemoveUser'):
                add_user = True if req_type == 'RepoMembership' else False
                assert user and repo and facility
                return self.do_repo_membership( user, repo, facility, add_user )

            elif req_type == 'RepoComputeAllocation':
                clustername = req.get('clustername', None)
                #assert facility and repo and clustername and percent
                percent = req.get('percentOfFacility', 10.)
                start = req.get('start', None)
                end = req.get('end', None)
                return self.do_repo_compute_allocation( repo, facility, clustername, percent, start, end )

            elif req_type == 'RepoChangeComputeRequirement':
                requirement = req.get('computerequirement', None)
                assert repo and facility and requirement
                return self.do_compute_requirement( repo, facility, requirement )

        return None

    def do_new_repo( self, repo: str, facility: str, principal: str, playbook: str="coact/add_repo.yaml", default_repo_allocation_percent=100. ) -> bool:

        # add facility specific Repos
        runner = self.run_playbook( playbook, facility=facility, repo=repo )
        self.LOG.warn(f"add_repo.yaml: {runner}")

        # write back to coact the repo information
        repo_create_req = {
            'repo': {
                'name': repo,
                'facility': facility,
                'principal': principal,
                'leaders': [ principal, ],
                'users': [ principal, ],
            }
        }
        self.LOG.info(f"upserting repo record {repo_create_req}")
        repo_upserted = self.back_channel.execute( self.REPO_UPSERT_GQL, repo_create_req )
        repo_id = repo_upserted['repoUpsert']['Id']

        # for each facilities' clusters, create a compute allocation record
        # could/shoudl probably merge this into single call with above gql mutation
        fac_res = self.back_channel.execute( self.FACILITY_CURRENT_COMPUTE_CGL, { 'facility': facility } )
        assert fac_res['facility']['name'] == facility
        start = datetime.datetime( 2023, 6, 1).isoformat()
        end = datetime.datetime( 2024, 7, 1).isoformat()

        clusters = fac_res['facility']['computepurchases']
        #self.LOG.info(f'clusters: {clusters}')
        for cluster in clusters:
            #self.LOG.info(f'cluster {cluster}')
            partition = cluster['clustername']
            # determine absolute resource count
            purchased = float(cluster['purchased'])
            resource = purchased * default_repo_allocation_percent / 100.
            self.upsert_repo_compute_allocation( repo_id, partition, default_repo_allocation_percent, start, end )


        # TODO: deal with storage

        return True

    def upsert_repo_compute_allocation( self, repo_id: str, cluster: str, percent: float, start: str, end: str, default_end_delta=relativedelta(years=3) ):
        # TODO search for existing cluster

        # must have an end
        if not end or end == '':
            end = parser.parse(start) + default_end_delta
            end = end.isoformat()

        compute_allocation_req = {
            'repo': { 'Id': repo_id },
            'repocompute': {
                'repoid': repo_id, 'clustername': cluster,
                'percentOfFacility': percent,
                'start': start, 'end': end
            },
        }
        self.LOG.info(f'upserting {compute_allocation_req}')
        resp = self.back_channel.execute( self.REPO_COMPUTE_ALLOCATION_UPSERT_GQL, compute_allocation_req )
        self.LOG.info(f'modified {resp}')
        return resp

    def do_repo_compute_allocation( self, repo: str, facility: str, cluster: str, percent: float, start: str, end: str ):
        
        self.LOG.info(f"set repo compute allocation {facility}:{repo} at {cluster} to {percent} between {start} - {end}")
        repo_req = { 'repo': { 'facility': facility, 'name': repo } }
        resp = self.back_channel.execute( self.REPO_CURRENT_COMPUTE_REQUIREMENT_GQL, repo_req )
        self.LOG.info(f'got {resp}')
        repo_obj = resp['repo']
        assert facility == repo_obj['facility'] and repo == repo_obj['name'] 
        self.LOG.info(f"found repo {repo_obj}")

        resp = self.upsert_repo_compute_allocation( repo_obj['Id'], cluster, percent, start, end )

        return True

    def get_account_name( self, facility: str, repo: str ) -> str:
        account_name = f'{facility}:{repo}'.lower()
        if account_name.endswith( ':default' ):
            account_name = f'{facility}'.lower()
        return account_name

    def do_repo_membership( self, user: str, repo: str, facility: str, add_user: bool=False, playbook: str="coact/slurm-users-partition.yaml" ) -> bool:

        # fetch for the list of all users for the repo
        runner = self.back_channel.execute( self.REPO_USERS_GQL, { 'repo': {'facility': facility, 'name': repo }} )
        users = runner['repo']['users']

        users_str = ','.join(users)
        self.LOG.info(f"setting account {facility}:{repo} with users {users_str}")

        # run playbook to add this user and existsing repo users to the slurm account
        # rubin submits jobs to multiple partitions, so we need to treat that differently for now due to 
        # the lack of support in slurm of this with multiple partitions
        if facility.lower() in ( 'rubin' ):
            self.LOG.warn("Exceptional code branch for rubin facility and multi partition usage!")
            runner = self.run_playbook( 'coact/slurm-users.yaml', user=user, users=users_str, account=f'{facility.lower()}:{repo.lower()}', defaultqos="normal", qos="normal,preemptable" )

        else:

            # determine which clusters are defined
            runner = self.back_channel.execute( self.FACILITY_CURRENT_COMPUTE_CGL, { 'facility': facility } )
            assert runner['facility']['name'] == facility

            clusters = runner['facility']['computepurchases']
             # determine total shares for facility
            facility_shares = 1
            for c in clusters:
              # set the slurm shares equal to teh number of cores
              # TODO: for gpus perhaps set to the number of gpus
              facility_shares += int(c['purchased'])
            # configure
            runner = self.run_playbook( 'coact/slurm-account.yaml', facility=facility, repo=repo, shares=facility_shares )
            #self.LOG.info(f"{playbook} output: {runner}")

            # set permissions and partitions
            for cluster in clusters:

              # if its the default repo, do not allow normal qos jobs
              qos = 'preemptable'
              default_qos = 'preemptable'

              partition = cluster['clustername']
              purchased = int(cluster['purchased'])
              if purchased > 0:
                qos = 'normal,preemptable'
                default_qos = 'normal'

              # run
              runner = self.run_playbook( playbook, user=user, users=users_str, facility=facility, repo=repo, partition=partition, defaultqos=default_qos, qos=qos, add_user=add_user )
              #self.LOG.info(f"{playbook} output: {runner}")


            # TODO purge removed clusters

        # add user into repo back in coact
        add_user_req = {
            "repo": { "name": repo, "facility": facility },
            "user": { "username": user }
        }
        self.back_channel.execute( self.REPO_APPEND_USER_GQL, add_user_req ) 

        return True

    def do_compute_requirement( self, repo: str, facility: str, requirement: str, playbook: str="coact/repo_change_compute_requirement.yaml" ) -> bool:
        # get current compute requirement
        repo = self.back_channel.execute( self.REPO_CURRENT_COMPUTE_REQUIREMENT_GQL, { 'repo': {'facility': facility, 'name': repo }} )
        self.LOG.info(f"repo: {repo}")
        current = repo['repo']['computerequirement']
        users = repo['repo']['users']
        users_str = ','.join(users)
        self.LOG.info(f"setting account {account_name} with users {users_str}")

        self.LOG.info(f"change {facility} {repo}'s compute requirement from {current} to {requirement} for users {users_str}")


        self.QOS_ENUMS = {
            'offshift': 'high',
            'onshift': 'expedite',
            'normal': 'normal',
            'preemptable': 'preemptable',
        }
        allowed_qos = [ self.QOS_ENUMS['normal'], self.QOS_ENUMS['preemptable'] ]
        default_qos = self.QOS_ENUMS[requirement]

        # 1) promote from normal to offshift
        # add qos to slurm account, set defaultqos to offshift
        if requirement == 'offshift':
            allowed_qos.append( self.QOS_ENUMS['offshift'] )

        # 2) promote from offshit to onshift
        # keep offshift qos, add onshift qos, set default qos to onshift
        elif requirement == 'onshift':
            allowed_qos.append( self.QOS_ENUMS['offshift'], self.QOS_ENUMS['onshift'] )

        # 3) promote from normal to onshift
        # add both onshift and offshift qos, set default to onshift

        # 4) demote from onshift to offshift
        # remove onshift qos, set default to offshift

        # 5) demote from offshift to normal
        # remove both onshift and offshift qos, set default to normal

        # 6) demote from onshift to normal
        # remove both onshift and offshift qos, set default to normal


        raise NotImplementedError()

        account_name = self.get_account_name( facility, repo )

        # setup the permissions to the qos
        runner = self.run_playbook( playbook, users=users_str, account=account_name, partition='milano', defaultqos=default_qos, qos=','.join(allowed_qos) )


        # reconfigure all jobs for this account
        
        # 1) onshift to offshift
        # 


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


