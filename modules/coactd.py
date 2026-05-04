"""
Click-based implementation of the Coactd command group.

This module provides a click.Group-based CommandManager replacement that
registers subcommands using click decorators instead of cliff's CommandManager.

The Coactd commands handle:
- User registration workflows (account creation, shell changes)
- Repository maintenance workflows (creation, membership, compute allocations)
- Request subscription streaming
"""

import json
from loguru import logger
from enum import Enum
from typing import Any, Optional, List
from math import ceil
from timeit import default_timer as timer
from pathlib import Path

import click
import pendulum as pdl
from gql import gql

import jinja2
import smtplib
from email.message import EmailMessage

import ansible_runner

# Import base classes from modules.base
from .base import GraphQlMixin, common_options, configure_logging_from_verbose
from .utils.graphql import GraphQlSubscriber

# Using loguru logger

# Define context settings to support -h for help
CONTEXT_SETTINGS = dict(help_option_names=['-h', '--help'])

COACT_ANSIBLE_RUNNER_PATH = './ansible-runner/'


# Order of class inheritance important
class RequestStatus(str, Enum):
    APPROVED = 'Approved'
    NOT_ACTED_ON = 'NotActedOn'
    REJECTED = 'Rejected'
    COMPLETED = 'Complete'
    INCOMPLETE = 'Incomplete'


class AnsibleRunner:
    """Mixin class for running Ansible playbooks."""
    # Using loguru logger
    ident = None

    def run_playbook(
        self,
        playbook: str,
        private_data_dir: str = COACT_ANSIBLE_RUNNER_PATH,
        tags: str = 'all',
        dry_run: bool = False,
        **kwargs
    ) -> Optional[ansible_runner.runner.Runner]:
        name = Path(playbook).name
        if not dry_run:
            r = ansible_runner.run(
                private_data_dir=private_data_dir,
                playbook=playbook,
                tags=tags,
                extravars=kwargs,
                suppress_env_files=True,
                ident=f'{self.ident}_{name}:{tags}',
                cancel_callback=lambda: None
            )
            self.logger.debug(r.stats)
            if not r.rc == 0:
                raise Exception("AnsibleRunner failed")
            return r
        else:
            self.logger.warning(f"not running playbook {playbook}")
            return None

    def playbook_events(self, runner: ansible_runner.runner.Runner) -> dict:
        for e in runner.events:
            if 'event_data' in e:
                yield e['event_data']

    def playbook_task_res(self, runner: ansible_runner.runner.Runner, play: str, task: str) -> dict:
        for e in self.playbook_events(runner):
            if 'play' in e and play == e['play'] and 'task' in e and task == e['task'] and 'res' in e:
                return e['res']


class EmailRunner:
    """Mixin class for sending emails."""
    # Using loguru logger
    smtp_server = None
    subject_prefix = '[Coact] '
    j2 = jinja2.Environment()

    def send_email(self, receiver, body, sender='s3df-help@slac.stanford.edu', subject=None, smtp_server=None, vars={}):
        msg = EmailMessage()
        msg['Subject'] = self.subject_prefix + str(subject)
        msg['From'] = sender
        msg['To'] = receiver
        t = self.j2.from_string(body)
        msg.set_content(t.render(**vars))
        server = smtp_server if smtp_server else self.smtp_server
        if not server:
            raise Exception("No smtp server configured")
        s = smtplib.SMTP(server)
        self.logger.info(f"sending email {msg}")
        s.send_message(msg)
        return s.quit()


# ============================================================================
# Registration Base Class
# ============================================================================

class Registration(GraphQlSubscriber, AnsibleRunner):
    """Base class for servicing Coact Requests."""
    # Using loguru logger
    back_channel = None
    request_types: List[str] = []

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
                    percentOfFacility
                    allocated
                }
                operationType
            }
        }
    """

    def __init__(self, username: str, password_file: str, client_name: str, dry_run: bool = False):
        self.logger = logger
        self.username = username
        self.password_file = password_file
        self.client_name = client_name
        self.dry_run = dry_run

    def run(self):
        """Main entry point - connect and process subscription requests."""
        # Connect to GraphQL
        self.back_channel = self.connect_graph_ql(
            username=self.username,
            password_file=self.password_file,
            timeout=60
        )
        sub = self.connect_subscriber(
            username=self.username,
            password=self.get_password(self.password_file)
        )

        for req_id, op_type, req_type, approval, req in self.subscribe(
            self.SUBSCRIPTION_STR,
            var={"clientName": self.client_name}
        ):
            s = timer()
            self.logger.info(f"Processing {req_id}: {op_type} {req_type} - {approval}: {req}")
            self.ident = req_id  # set the request id for ansible runner

            try:
                if req_type in self.request_types:
                    result = self.do(req_id, op_type, req_type, approval, req, dry_run=self.dry_run)
                    if result:
                        self.logger.info(f"Marking request {req_id} complete")
                        self.markCompleteRequest(req, f'Request {self.ident} completed')
                        e = timer()
                        duration = e - s
                        self.logger.info(f"Done processing {req_id} in {duration:,.02f}s")
                    else:
                        self.logger.warning(f"Unknown return for {req_id}, type {op_type}")
                else:
                    self.logger.info(f"Ignoring {req_id}")

            except Exception as e:
                self.markIncompleteRequest(req, f'Request {self.ident} did not complete: {e}')
                end_time = timer()
                duration = end_time - s
                self.logger.exception(f"Error processing {req_id}: {e} in {duration:,.02f}s")

    def do(self, req_id: str, op_type: Any, req_type: Any, approval: str, req: dict, dry_run: bool) -> bool:
        """Process a request. Subclasses must override this method."""
        raise NotImplementedError('do() is abstract')


# ============================================================================
# Create the main coactd group
# ============================================================================

@click.group(name='coactd', help="Coact daemon/workflow processing tools", context_settings=CONTEXT_SETTINGS)
@click.pass_context
def coactd(ctx):
    """Coactd command group for processing Coact requests and workflows."""
    ctx.ensure_object(dict)


def registration_options(f):
    """Decorator for common registration command options."""
    f = click.option(
        '--username',
        default='sdf-bot',
        help='Basic auth username for graphql service'
    )(f)
    f = click.option(
        '--password-file',
        required=True,
        type=click.Path(exists=True),
        help='Basic auth password for graphql service'
    )(f)
    f = click.option(
        '--client-name',
        default=None,
        help='Subscriber queue name to connect to'
    )(f)
    f = click.option(
        '--dry-run',
        is_flag=True,
        default=False,
        help='Do not run any ansible playbooks'
    )(f)
    return f


# ============================================================================
# UserRegistration Command
# ============================================================================

class UserRegistration(Registration):
    """Workflow for user creation."""
    request_types = ['UserAccount', 'UserChangeShell']

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

    def do(self, req_id, op_type, req_type, approval, req, dry_run):
        user = req.get('preferredUserName', None)
        facility = req.get('facilityname', None)
        eppn = req.get('eppn', None)

        if approval in [RequestStatus.APPROVED]:
            if req_type == 'UserAccount':
                assert user and facility and eppn
                return self.do_new_user(user, eppn, facility)
            elif req_type == 'UserChangeShell':
                user = req.get('username', None)
                shell = req.get('shell', None)
                assert user and shell
                return self.do_change_shell(user, shell)
        else:
            self.logger.info(f"Ignoring {approval} state request")
            return None

    def do_change_shell(self, user: str, shell: str, playbook: str = "set_user_shell.yaml") -> bool:
        self.logger.info(f"Changing shell for user {user} using {playbook}")
        runner = self.run_playbook(playbook, user=user, user_login_shell=shell)
        user_id = self.back_channel.execute(
            self.USER_CHANGE_SHELL_GQL,
            {'user': {"username": user, "shell": shell}}
        )
        return True

    def do_new_user(self, user: str, eppn: str, facility: str, playbook: str = "coact/add_user.yaml") -> bool:
        self.logger.info(f"Creating user {user} at facility {facility} using {playbook}")

        # enable ldap
        runner = self.run_playbook(playbook, user=user, user_facility=facility, tags='ldap')
        self.logger.error(f"FACTS: {self.playbook_task_res(runner, 'Create user', 'gather user ldap facts')}")
        ldap_facts = self.playbook_task_res(runner, 'Create user', 'gather user ldap facts')['ansible_facts']
        shell = self.run_playbook(playbook, user=user, user_facility=facility, tags='shell')
        self.logger.debug(f"ldap facts: {ldap_facts}")

        user_create_req = {
            'user': {
                'username': user,
                'eppns': [eppn],
                'shell': ldap_facts['ldap_user_default_shell'],
                'preferredemail': eppn,
                'uidnumber': int(ldap_facts['ldap_user_uidNumber']),
                'fullname': ldap_facts['ldap_user_gecos'],
            }
        }
        self.logger.debug(f"upserting user record {user_create_req}")
        user_id = self.back_channel.execute(self.USER_UPSERT_GQL, user_create_req)
        self.logger.debug(f"upserted user {user_id}")

        # configure home directory
        runner = self.run_playbook(playbook, user=user, user_facility=facility, tags='home', force_copy_skel=False)

        # user storage
        user_storage_req = {
            'user': {
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
        self.logger.debug(f"upserting user storage record {user_storage_req}")
        self.back_channel.execute(self.USER_STORAGE_GQL, user_storage_req)

        # sshkeys
        runner = self.run_playbook(playbook, user=user, user_facility=facility, tags='sshkey')

        # do any facility specific tasks
        runner = self.run_playbook(playbook, user=user, user_facility=facility, tags='facility')

        # clear the sssd cache to allow users to log in immediately
        runner = self.run_playbook(playbook, user=user, user_facility=facility, tags='sssd')

        # always register user with the facility's default Repo
        add_user_req = {
            'repo': {'facility': facility, 'name': 'default'},
            'user': {'username': user},
        }
        self.logger.debug(f"add to default repo {add_user_req}")
        self.back_channel.execute(self.REPO_ADD_USER_GQL, add_user_req)

        return True


@coactd.command(name='userregistration')
@common_options
@registration_options
@click.pass_context
def user_registration(ctx, verbose, username, password_file, client_name, dry_run):
    """Workflow for user creation and shell changes.

    Handles UserAccount and UserChangeShell request types.
    """
    configure_logging_from_verbose(verbose)
    ctx.obj['verbose'] = verbose

    handler = UserRegistration(
        username=username,
        password_file=password_file,
        client_name=client_name or 'sdf-bot-UserRegistration',
        dry_run=dry_run
    )
    handler.run()


# ============================================================================
# RepoRegistration Command
# ============================================================================

class RepoRegistration(Registration):
    """Workflow for repo maintenance."""
    request_types = [
        'NewRepo',
        'RepoMembership',
        'RepoRemoveUser',
        'RepoChangeComputeRequirement',
        'RepoComputeAllocation',
        'RepoUpdateFeature'
    ]

    REPO_USERS_GQL = gql("""
      query getRepoUsers ( $repo: RepoInput! ) {
        repo( filter: $repo ) {
            users
        }
      }""")

    COMPUTE_ALLOCATION_UPSERT_GQL = gql("""
        mutation repoComputeAllocationUpsert($repo: RepoInput!, $repocompute: RepoComputeAllocationInput!, $qosinputs: [QosInput!]!) {
            repoComputeAllocationUpsert(repo: $repo, repocompute: $repocompute, qosinputs: $qosinputs) {
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
            features {
              name
              options
              state
            }
            computerequirement
            currentComputeAllocations {
              Id
              clustername
              start
              end
              percentOfFacility
              cpus: allocatedCpusCount
              memory: allocatedMemGb
              nodes: allocatedNodesCount
              gpus: allocatedGpusCount
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

    def do(self, req_id, op_type, req_type, approval, req, dry_run):
        user = req.get('username', None)
        repo = req.get('reponame', None)
        facility = req.get('facilityname', None)
        principal = req.get('principal', None)

        if approval in [RequestStatus.APPROVED]:
            if req_type == 'NewRepo':
                assert repo and facility and principal
                return self.do_new_repo(repo, facility, principal)

            elif req_type in ('RepoMembership', 'RepoRemoveUser'):
                action = 'present' if req_type == 'RepoMembership' else 'absent'
                assert user and repo and facility
                return self.do_repo_membership(user=user, repo=repo, facility=facility, action=action, dry_run=dry_run)

            elif req_type == 'RepoComputeAllocation':
                clustername = req.get('clustername', None)
                percent = req.get('percentOfFacility', 100.)
                allocated = req.get('allocated', None)
                start = pdl.parse(req.get('start', None), timezone='UTC')
                end = req.get('end', None)
                if end is not None:
                    end = pdl.parse(end, timezone='UTC')
                return self.do_repo_compute_allocation(
                    repo, facility, clustername, percent, allocated, start, end, dry_run=dry_run
                )

            elif req_type == 'RepoChangeComputeRequirement':
                requirement = req.get('computerequirement', None)
                assert repo and facility and requirement
                return self.do_compute_requirement(repo, facility, requirement)

            elif req_type == 'RepoUpdateFeature':
                return self.do_feature(repo, facility)

        return None

    def do_new_repo(
        self,
        repo: str,
        facility: str,
        principal: str,
        default_repo_allocation_percent=0,
        repo_allocation_start=None,
        repo_allocation_end_delta=None
    ) -> bool:
        if repo_allocation_start is None:
            repo_allocation_start = pdl.today().in_tz('UTC')
        if repo_allocation_end_delta is None:
            repo_allocation_end_delta = pdl.duration(years=5)

        # run the facility tasks for this repo
        runner = self.run_playbook("coact/add_repo.yaml", facility=facility, repo=repo)

        # Extract repo GID if it was created for facilities that use grouper
        repo_gid = None
        repo_group_name = ""
        if facility.lower() in ['lcls', 'cryoem']:
            try:
                # Get the repo_gid fact that was set in add_repo.yaml
                repo_facts = self.playbook_task_res(runner, 'Create a facility repo', 'Expose group values for runner')
                if repo_facts and 'ansible_facts' in repo_facts:
                    repo_gid = repo_facts['ansible_facts']['repo_gid']
                    repo_group_name = repo_facts['ansible_facts']['repo_group_name']
                    self.logger.info(f"Retrieved repo GID for {facility}:{repo}: {repo_gid}")
                else:
                    self.logger.warning(f"No repo GID found in playbook results for {facility}:{repo}")
            except Exception as e:
                self.logger.warning(f"Failed to extract repo GID for {facility}:{repo}: {e}")

        leaders = [principal]
        users = [principal]

        # write back to coact the repo information
        repo_create_req = {
            'repo': {
                'name': repo,
                'facility': facility,
                'principal': principal,
                'leaders': leaders,
                'users': users,
            }
        }
        self.logger.info(f"upserting repo record {repo_create_req}")
        REPO_UPSERT_GQL = gql("""
            mutation repoUpsert($repo: RepoInput! ) {
                repoUpsert(repo: $repo) {
                    Id
                }
            }
            """)
        repo_upserted = self.back_channel.execute(REPO_UPSERT_GQL, repo_create_req)
        repo_id = repo_upserted['repoUpsert']['Id']

        # Create a parameterized feature upsert mutation
        FEATURE_UPSERT_GQL = gql("""
            mutation repoUpsert($repo: RepoInput!, $feature: RepoFeatureInput!) {
                repoUpsertFeature(repo: $repo, feature: $feature) {
                    Id
                }
            }
        """)

        # Create slurm feature
        slurm_feature_req = {
            'repo': {'Id': repo_id},
            'feature': {'name': 'slurm', 'state': True, 'options': []}
        }
        self.back_channel.execute(FEATURE_UPSERT_GQL, slurm_feature_req)

        # Create posixgroup feature if GID was obtained from grouper
        if repo_gid is not None:
            posixgroup_options = [json.dumps({
                "name": repo_group_name,
                "gidNumber": int(repo_gid)
            })]

            posixgroup_feature_req = {
                'repo': {'Id': repo_id},
                'feature': {
                    'name': 'posixgroup',
                    'state': True,
                    'options': posixgroup_options
                }
            }

            try:
                self.back_channel.execute(FEATURE_UPSERT_GQL, posixgroup_feature_req)
                self.logger.info(f"Created posixgroup feature for {facility}:{repo} with GID {repo_gid}")
            except Exception as e:
                self.logger.warning(f"Failed to create posixgroup feature for {facility}:{repo}: {e}")

        return True

    def upsert_repo_compute_allocation(
        self,
        repo_id: str,
        cluster: str,
        percent: int,
        allocated_resource: float,
        start: pdl.DateTime,
        end: Optional[str],
        default_end_delta=None,
        dry_run: bool = False
    ):
        if default_end_delta is None:
            default_end_delta = pdl.duration(years=5)

        # must have an end
        if not end or end == '':
            end = start + default_end_delta
            end = end.isoformat()

        def format_datetime(iso, round_off=None):
            iso = str(iso).replace(' ', 'T').replace('+00:00', 'Z')
            if '.' not in iso:
                iso = iso.replace('Z', '.000000Z')
            return iso

        compute_allocation_req = {
            'repo': {'Id': repo_id},
            'repocompute': {
                'repoid': repo_id,
                'clustername': cluster,
                'percentOfFacility': percent,
                'allocated': allocated_resource,
                'start': format_datetime(start),
                'end': format_datetime(end)
            },
        }
        self.logger.info(f'upserting {compute_allocation_req}')
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
        resp = self.back_channel.execute(REPO_COMPUTE_ALLOCATION_UPSERT_GQL, compute_allocation_req)
        self.logger.info(f'modified {resp}')
        return resp

    def get_feature(self, repo_obj, name):
        state = None
        feature = None
        for i in repo_obj['features']:
            if 'name' in i and i['name'] == name:
                state = i['state'] if 'state' in i else None
                feature = i
                break
        return state, feature

    def do_repo_compute_allocation(
        self,
        repo: str,
        facility: str,
        cluster: str,
        percent: int,
        allocated_resource: float,
        start: pdl.DateTime,
        end: Optional[str],
        dry_run: bool = False
    ):
        """Does all the necessary tasks to setup a new or existing Repo."""
        self.logger.info(f"set repo compute allocation {facility}:{repo} at {cluster} to {percent}% ({allocated_resource} nodes) between {start} - {end}")

        def _get_allocation_info():
            repo_req = {'repo': {'facility': facility, 'name': repo}}
            resp = self.back_channel.execute(self.REPO_CURRENT_COMPUTE_REQUIREMENT_GQL, repo_req)
            repo_obj = resp['repo']
            assert facility == repo_obj['facility'] and repo == repo_obj['name']
            assert 'features' in repo_obj
            return repo_obj

        repo_obj = _get_allocation_info()

        # validate that the slurm feature is enabled
        enable_slurm, slurm_feature = self.get_feature(repo_obj, 'slurm')
        self.logger.info(f"slurm feature for {facility}:{repo} enabled? {enable_slurm}: {slurm_feature}")

        if not enable_slurm:
            # remove users
            ensure_users = self.run_playbook(
                'coact/slurm/ensure-users.yaml',
                users=','.join(repo_obj['users']),
                facility=facility,
                repo=repo,
                partitions=cluster,
                state='absent',
                dry_run=dry_run
            )
            # remove the account
            ensure_repos = self.run_playbook(
                'coact/slurm/ensure-repo.yaml',
                facility=facility,
                repo=repo,
                partition=cluster,
                state='absent',
                dry_run=dry_run
            )
            return True
        else:
            # upsert the record
            resp = self.upsert_repo_compute_allocation(
                repo_obj['Id'], cluster, percent, allocated_resource, start, end
            )

            # fetch it again to obtain the correct resources with the new percentage
            repo_obj = _get_allocation_info()

            # determine the alloc resources for this partition
            resources = [
                alloc for alloc in repo_obj['currentComputeAllocations']
                if 'clustername' in alloc and alloc['clustername'] == cluster
            ]

            if len(resources) != 1:
                raise Exception("Could not determine allocation resources")
            r = resources.pop(0)

            # enact it through slurm
            ensure_repos = self.run_playbook(
                'coact/slurm/ensure-repo.yaml',
                facility=facility,
                repo=repo,
                partition=cluster,
                cpus=int(r['cpus']),
                memory=int(r['memory']) * 1024,
                nodes=int(ceil(r['nodes'])),
                gpus=int(r['gpus']),
                state='present',
                dry_run=dry_run
            )

            # sync users
            ensure_users = self.run_playbook(
                'coact/slurm/ensure-users.yaml',
                users=','.join(repo_obj['users']),
                facility=facility,
                repo=repo,
                partitions=cluster,
                state='sync',
                dry_run=dry_run
            )

            return True

    def get_account_name(self, facility: str, repo: str) -> str:
        account_name = f'{facility}:{repo}'.lower()
        if account_name.endswith(':default'):
            account_name = f'{facility}'.lower()
        return account_name

    def do_repo_membership(self, user: str, repo: str, facility: str, action: str, dry_run: bool = False) -> bool:
        """Update the list of members for this Repo."""
        REPO_CURRENT_CLUSTERS_CGL = gql("""
            query repo( $facility: String!, $repo: String! ) {
              repo(filter: {facility: $facility, name: $repo}) {
                clusters: currentComputeAllocations {
                  name: clustername
                }
                features {
                  name
                  state
                  options
                }
                users
              }
            }
            """)
        runner = self.back_channel.execute(REPO_CURRENT_CLUSTERS_CGL, {'facility': facility, 'repo': repo})
        assert 'repo' in runner
        this = runner['repo']

        assert action in ['present', 'absent']

        # do membership of slurm
        enable_slurm, slurm_feature = self.get_feature(this, 'slurm')
        partitions = [cluster['name'] for cluster in this['clusters']]
        self.logger.info(f"slurm feature for {facility}:{repo} enabled? {enable_slurm}: {slurm_feature}")

        if enable_slurm and len(partitions):
            self.logger.info(f"{action} on user {user} account {facility}:{repo} on partitions {partitions}")
            runner = self.run_playbook(
                'coact/slurm/ensure-users.yaml',
                users=user,
                facility=facility,
                repo=repo,
                partitions=','.join(partitions),
                state=action,
                dry_run=dry_run
            )

        # do membership of netgroups
        enable_netgroup, netgroup_feature = self.get_feature(this, 'netgroup')
        self.logger.info(f"netgroup feature for {facility}:{repo} enabled? {enable_netgroup}: {netgroup_feature}")

        if enable_netgroup:
            netgroup_name = None
            for option in netgroup_feature['options']:
                j = json.loads(option)
                if 'name' in j:
                    netgroup_name = j['name']
            assert netgroup_name is not None
            runner = self.run_playbook(
                'coact/netgroup.yaml',
                user=user,
                users=this['users'],
                name=netgroup_name,
                state=action,
                create=True,
                dry_run=dry_run
            )

        # do membership of posixGroups
        enable_posixgroup, posixgroup_feature = self.get_feature(this, 'posixgroup')
        self.logger.info(f"posixgroup feature for {facility}:{repo} enabled? {enable_posixgroup}: {posixgroup_feature}")

        if enable_posixgroup:
            posixgroup_name = None
            gid_number = None
            for option in posixgroup_feature['options']:
                j = json.loads(option)
                if 'name' in j:
                    posixgroup_name = j['name']
                if 'gidNumber' in j:
                    gid_number = j['gidNumber']
            assert posixgroup_name is not None, "posixgroup not configured in feature options"
            assert gid_number is not None, "gidnumber not configured in feature options"
            runner = self.run_playbook(
                'coact/posixGroup.yaml',
                user=user,
                users=this['users'],
                groupName=posixgroup_name,
                gidNumber=gid_number,
                state=action,
                create=True,
                dry_run=dry_run
            )

        # finish up and mark record
        user_req = {
            "repo": {"name": repo, "facility": facility},
            "user": {"username": user}
        }

        if action == 'present':
            REPO_APPEND_USER_GQL = gql("""
                mutation repoAppendMember($repo: RepoInput!, $user: UserInput!) {
                  repoAppendMember(repo: $repo, user: $user) {
                      Id
                  }
                }""")
            self.back_channel.execute(REPO_APPEND_USER_GQL, user_req)
        elif action == 'absent':
            REPO_REMOVE_USER_GQL = gql("""
                mutation repoRemoveUser($repo: RepoInput!, $user: UserInput!) {
                  repoRemoveUser(repo: $repo, user: $user) {
                      Id
                  }
                }""")
            try:
                self.back_channel.execute(REPO_REMOVE_USER_GQL, user_req)
            except Exception as e:
                if 'is not a user in repo' not in str(e):
                    raise e

        return True

    def do_compute_requirement(self, repo: str, facility: str, requirement: str, playbook: str = "coact/repo_change_compute_requirement.yaml") -> bool:
        """Change compute requirement for a repo."""
        # get current compute requirement
        repo_data = self.back_channel.execute(
            self.REPO_CURRENT_COMPUTE_REQUIREMENT_GQL,
            {'repo': {'facility': facility, 'name': repo}}
        )
        self.logger.info(f"repo: {repo_data}")
        current = repo_data['repo']['computerequirement']
        users = repo_data['repo']['users']
        users_str = ','.join(users)

        account_name = self.get_account_name(facility, repo)
        self.logger.info(f"setting account {account_name} with users {users_str}")
        self.logger.info(f"change {facility} {repo}'s compute requirement from {current} to {requirement} for users {users_str}")

        QOS_ENUMS = {
            'offshift': 'high',
            'onshift': 'expedite',
            'normal': 'normal',
            'preemptable': 'preemptable',
        }
        allowed_qos = [QOS_ENUMS['normal'], QOS_ENUMS['preemptable']]
        default_qos = QOS_ENUMS[requirement]

        # 1) promote from normal to offshift
        if requirement == 'offshift':
            allowed_qos.append(QOS_ENUMS['offshift'])

        # 2) promote from offshift to onshift
        elif requirement == 'onshift':
            allowed_qos.append(QOS_ENUMS['offshift'])
            allowed_qos.append(QOS_ENUMS['onshift'])

        # 3) promote from normal to onshift
        # add both onshift and offshift qos, set default to onshift

        # 4) demote from onshift to offshift
        # remove onshift qos, set default to offshift

        # 5) demote from offshift to normal
        # remove both onshift and offshift qos, set default to normal

        # 6) demote from onshift to normal
        # remove both onshift and offshift qos, set default to normal

        raise NotImplementedError("do_compute_requirement is not fully implemented")

    def do_feature(self, repo, facility, dry_run: bool = False) -> bool:
        raise NotImplementedError("do_feature not yet implemented")


@coactd.command(name='reporegistration')
@common_options
@registration_options
@click.pass_context
def repo_registration(ctx, verbose, username, password_file, client_name, dry_run):
    """Workflow for repository maintenance.

    Handles NewRepo, RepoMembership, RepoRemoveUser, RepoChangeComputeRequirement,
    RepoComputeAllocation, and RepoUpdateFeature request types.
    """
    configure_logging_from_verbose(verbose)
    ctx.obj['verbose'] = verbose

    handler = RepoRegistration(
        username=username,
        password_file=password_file,
        client_name=client_name or 'sdf-bot-RepoRegistration',
        dry_run=dry_run
    )
    handler.run()


# ============================================================================
# Get Command
# ============================================================================

class Get(GraphQlSubscriber):
    """Just streams output from requests subscription."""
    # Using loguru logger

    def __init__(self, username: str, password_file: str):
        self.username = username
        self.password_file = password_file

    def run(self):
        """Stream subscription results."""
        self.connect_subscriber(
            username=self.username,
            password=self.get_password(self.password_file)
        )

        subscription_query = """
            subscription {
                requests {
                    theRequest {
                        reqtype
                        eppn
                        preferredUserName
                    }
                }
            }
        """

        self.logger.info("Starting request subscription stream...")
        for result in self.subscription_client.subscribe(gql(subscription_query)):
            print(result)


@coactd.command(name='get')
@common_options
@click.option(
    '--username',
    default='sdf-bot',
    help='Basic auth username for graphql service'
)
@click.option(
    '--password-file',
    required=True,
    type=click.Path(exists=True),
    help='Basic auth password for graphql service'
)
@click.pass_context
def get_requests(ctx, verbose, username, password_file):
    """Stream output from requests subscription.

    Connects to the GraphQL subscription and prints incoming requests.
    """
    configure_logging_from_verbose(verbose)
    ctx.obj['verbose'] = verbose

    handler = Get(
        username=username,
        password_file=password_file
    )
    handler.run()
