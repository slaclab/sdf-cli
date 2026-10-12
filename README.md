# SDF-CLI

This repo contains command line tools for the SDF: the CoactD daemons that enact approved Coact requests, and the Slurm job accounting importer.

# Development

The CLI is built on [click](https://click.palletsprojects.com/). `sdf_click.py` is the root command group; the `coact` (Slurm job accounting) and `coactd` (request daemons) groups are defined in `modules/coact.py` and `modules/coactd.py` and registered onto it, giving a noun-verb syntax ala git.


# Installation

Dependencies are managed with [uv](https://docs.astral.sh/uv/):

```
uv sync --all-extras
```

The Ansible playbooks live in the `ansible-runner/project` git submodule, which must be checked out before any playbook-running command works:

```
make update-sdf-ansible
```

Secrets are pulled from Vault into `etc/.secrets/`:

```
make secrets
```

Tests:

```
uv run pytest tests/ -v
```


# Usage

```
uv run ./sdf_click.py --help
```



# CoactD

to provide the microservice abstration of user and disk requests from coact, we provide a daemon in sdf-cli to enact the required workflows for new user and repo registrations.

to run, do

    ❯ SDF_COACT_URI=wss://coact-dev.slac.stanford.edu/graphql-service  uv run ./sdf_click.py coactd get

note that the uri scheme is wss.
