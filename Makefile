PYTHON_BIN ?= ./bin/python3
PIP_BIN ?= ./bin/pip3
VENV_BIN ?= ./bin/activate
VAULT_SECRET_PATH ?= secret/tid/coact

get-secrets:
	mkdir etc/.secrets/ -p
	#set -e; for i in ldap_binddn ldap_bindpw; do vault kv get --field=$$i $(VAULT_SECRET_PATH) > etc/.secrets/$$i ; done
	set -e; for i in password; do vault kv get --field=$$i $(VAULT_SECRET_PATH)/service-account > etc/.secrets/$$i ; done
	chmod -R go-rwx etc/.secrets

clean-secrets:
	rm -rf etc/.secrets

virtualenv:
	python3.11 -m venv .

venv: virtualenv

pip:
	$(PYTHON_BIN) -m pip install --upgrade pip
	source $(VENV_BIN) && $(PIP_BIN) install -r requirements.txt

# OS level dependencies
deps:
#   note aiohttp won't build yet on python3.12 
	dnf install -y python3.11-devel openldap-devel
	dnf groupinstall -y "Development Tools"

# run this to configure the dev environment
environment: venv pip

update-sdf-ansible:
	git submodule update --init --recursive

apply: environment get-secrets update-sdf-ansible

# Docker
docker_build:
	docker build --platform=linux/amd64 --tag slaclab/sdf-cli .

docker_build_no_cache:
	docker build --no-cache --platform=linux/amd64 --tag slaclab/sdf-cli .

docker_run_it: docker_build
	docker run --platform=linux/amd64 -it slaclab/sdf-cli bash

docker_push: docker_build
	docker push slaclab/sdf-cli
