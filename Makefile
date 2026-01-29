VENV_DIR ?= venv
VENV_BIN ?= $(VENV_DIR)/bin/python3
PIP_BIN ?= $(VENV_DIR)/bin/pip
PYTHON_BIN ?= python3.9
VAULT_SECRET_PATH ?= secret/tid/coact

secrets:
	mkdir etc/.secrets/ -p
	#set -e; for i in ldap_binddn ldap_bindpw; do vault kv get --field=$$i $(VAULT_SECRET_PATH) > etc/.secrets/$$i ; done
	set -e; for i in password; do vault kv get --field=$$i $(VAULT_SECRET_PATH)/service-account > etc/.secrets/$$i ; done
	chmod -R go-rwx etc/.secrets

clean-secrets:
	rm -rf etc/.secrets

virtualenv:
	$(PYTHON_BIN) -m venv $(VENV_DIR)

venv: virtualenv

pip:
	$(VENV_BIN) -m pip install --upgrade pip
	$(PIP_BIN) install -r requirements.txt

# OS level dependencies
deps:
	dnf groupinstall -y "Development Tools"
	dnf install -y python36-devel openldap-devel

# run this to configure the dev environment
environment: venv pip

dev: environment

update-sdf-ansible:
	git submodule update --init --recursive

apply: environment get-secrets update-sdf-ansible

test:
	$(VENV_BIN) sdf_click.py 
