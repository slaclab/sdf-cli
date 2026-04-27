# sdf-cli and sdf-ansible Integration

**Date:** March 4, 2026  
**Status:** Draft

---

## Overview

`sdf-cli` is the automation driver that listens for request types from `coact-api` and executes system administration tasks. `sdf-ansible` is the execution engine that carries out those tasks against SLAC infrastructure (LDAP, home directories, Slurm, SSSD, etc.). The two are coupled via a daemons (e.g., `coact-userregistration-daemon.sh` and `coact-reporegistration-daemon.sh`) that listen for coact requests sent through mongo streams and call `sdf-cli` commands, which in turn invoke Ansible playbooks in `sdf-ansible`


---

## Structural Relationship

`sdf-ansible` is embedded inside `sdf-cli` as a **git submodule**, mounted at `sdf-cli/ansible-runner/project/`. This is the directory that the `ansible-runner` Python library treats as its project root.

- Declared in: `sdf-cli/.gitmodules`
- Installed via: `sdf-cli/Makefile` → `git submodule update --init --recursive`

```
sdf-cli/
└── ansible-runner/
    └── project/          ← sdf-ansible (git submodule)
        ├── coact/
        │   ├── add_user.yaml
        │   ├── add_repo.yaml
        │   ├── netgroup.yaml
        │   ├── posixGroup.yaml
        │   └── slurm/
        │       ├── ensure-repo.yaml
        │       └── ensure-users.yaml
        └── roles/, inventories/, group_vars/, ...
```

---

## How sdf-cli Invokes Ansible

sdf-cli uses the **`ansible-runner` Python library** rather than shelling out to `ansible-playbook` directly. The `AnsibleRunner` mixin class in `modules/coactd.py` is the core integration point:

```python
COACT_ANSIBLE_RUNNER_PATH = './ansible-runner/'

def run_playbook(self, playbook, tags='all', **kwargs):
    ansible_runner.run(
        private_data_dir='./ansible-runner/',   # root for ansible-runner
        playbook=playbook,                       # e.g. 'coact/add_user.yaml'
        tags=tags,
        extravars=kwargs,                        # variables passed into Ansible
        ident=f'{self.ident}_{playbook}:{tags}'  # namespaced artifact directory
    )
```

- `private_data_dir='./ansible-runner/'` + `project/` subdir = sdf-ansible repo
- A playbook path like `'coact/add_user.yaml'` resolves to `ansible-runner/project/coact/add_user.yaml`
- Run artifacts (stdout, events, return codes) land in `ansible-runner/artifacts/{ident}/`

---

## The Daemon Architecture

Two long-running daemons watch for approved requests from `coact-api` via **GraphQL WebSocket subscriptions** and fire Ansible playbooks in response:

| Daemon script | Click command | Handles |
|---|---|---|
| `coact-userregistration-daemon.sh` | `sdf_click.py coactd userregistration` | `UserAccount` requests |
| `coact-reporegistration-daemon.sh` | `sdf_click.py coactd reporegistration` | `RepoAccount`, `RepoMembership`, `RepoComputeAllocation` requests |

The daemons connect to `coact-api` using a service account password fetched from **Vault** (see the `vault kv get ...` target in the `Makefile`, which writes to `etc/.secrets/password`). Each daemon script runs its command in a restart loop so it recovers automatically from failures.

---

## Playbook Dispatch Map

The following table maps request types to the Ansible playbooks they trigger:

| Request Type | Playbook Invoked | Variables Passed |
|---|---|---|
| New user account | `coact/add_user.yaml` | `user`, `user_facility`, `tags=(ldap\|home\|sshkey\|facility\|sssd)` |
| Shell change | `set_user_shell.yaml` | `user`, `user_login_shell` |
| New repo | `coact/add_repo.yaml` | `facility`, `repo` |
| Slurm account allocation | `coact/slurm/ensure-repo.yaml` | `facility`, `repo`, `partition`, `cpus`, `memory`, `nodes`, `gpus`, `state` |
| Slurm user membership | `coact/slurm/ensure-users.yaml` | `users` (comma-sep), `facility`, `repo`, `partitions`, `state` |
| NIS/LDAP netgroup | `coact/netgroup.yaml` | `user`, `users`, `name`, `state` |
| POSIX group | `coact/posixGroup.yaml` | `user`, `users`, `groupName`, `gidNumber`, `state` |
| Grouper group | `coact/grouper.yaml` | `user`, `users`, `groupName`, `gidNumber`, `state` |

---

## Bidirectional Data Flow

Data flows **both directions** between sdf-cli and Ansible.

**Into Ansible** — via `extravars` (e.g. `user='jdoe'`, `facility='lcls'`, `cpus=128`)

**Out of Ansible** — sdf-cli reads structured facts back from Ansible's event JSON using the `AnsibleRunner.playbook_task_res()` method in `modules/coactd.py`, and uses that data to drive subsequent logic. For example, after running `add_user.yaml` with `tags=ldap`, sdf-cli reads back `ldap_user_uidNumber`, `ldap_user_homedir`, etc. set by the `gather user ldap facts` task in the `ansible-role-add-user` role's lookup tasks, then upserts them into `coact-api` via a GraphQL mutation.

```python
def playbook_task_res(self, runner, play, task) -> dict:
    for e in self.playbook_events(runner):
        if play == e['play'] and task == e['task']:
            return e['res']
```

---

## Shared Conventions

Both sides independently agree on a **`facility:repo` account naming scheme** for Slurm:

- sdf-cli (`RepoRegistration.get_account_name()` in `modules/coactd.py`): `f'{facility}:{repo}'.lower()`
- sdf-ansible (`coact/slurm/ensure-repo.yaml`): `'{{ facility | lower }}:{{ repo | lower }}'`

The `default` repo is treated specially in both — it maps to just the bare facility name for the Slurm account rather than `facility:default`.

---

## Typical Operation Flow: New User

The following illustrates the full path for a new user account request:

```
coactui (user submits request)
    → coact-api stores request, broadcasts via GraphQL subscription
        → sdf-cli daemon receives event (WebSocket)
            → run_playbook('coact/add_user.yaml', tags='ldap')
                → Ansible: kinit (Kerberos), ldapsearch, set_fact LDAP attributes
            → read back ldap facts from runner events
            → GraphQL mutation: upsert user in coact-api
            → run_playbook(..., tags='home')    # create /sdf/home/{l}/{user}
            → run_playbook(..., tags='sshkey')  # generate SSH keys
            → run_playbook(..., tags='sssd')    # sss_cache -E on all login nodes
            → GraphQL mutation: mark request complete
```

Note that `add_user.yaml` is invoked multiple times with different tags — each tag corresponds to an independent stage of provisioning (LDAP lookup, home directory creation, SSH key generation, facility-specific setup, SSSD cache flush). This allows stages to be retried independently.

---

## Key Files Reference

| File | Purpose |
|---|---|
| `sdf-cli/.gitmodules` | Declares sdf-ansible as a git submodule at `ansible-runner/project/` |
| `sdf-cli/Makefile` | `update-sdf-ansible` target installs the submodule; Vault secret retrieval |
| `sdf-cli/requirements.txt` | Declares `ansible-runner==2.3.1` as a Python dependency |
| `sdf-cli/modules/coactd.py` | click-based daemon commands; `AnsibleRunner`, `UserRegistration`, `RepoRegistration` classes |
| `sdf-cli/modules/utils/graphql.py` | GraphQL HTTP + WebSocket subscription client |
| `sdf-cli/coact-userregistration-daemon.sh` | Daemon restart loop for user registration |
| `sdf-cli/coact-reporegistration-daemon.sh` | Daemon restart loop for repo registration |
| `sdf-ansible/coact/` | Playbooks that form the integration surface with sdf-cli |
| `sdf-ansible/roles/ansible-role-add-user/` | Role handling full user provisioning |
| `sdf-ansible/roles/ansible-role-s3df-slurm-assoc/` | Role managing Slurm `sacctmgr` associations |
| `sdf-ansible/roles/ansible-role-ldap-auth/` | Role handling Kerberos keytab auth for LDAP operations |
| `sdf-ansible/library/` | Custom Ansible modules for LDAP, netgroup, and posixgroup management |
