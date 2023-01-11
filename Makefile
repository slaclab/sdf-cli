PYTHON_BIN ?= ./bin/python3
PIP_BIN ?= ./bin/pip3
VENV_BIN ?= ./bin/activate
VAULT_SECRET_PATH ?= secret/tid/sdf/coact

get-secrets-from-vault:
	mkdir etc/.secrets/ -p
	set -e; for i in ldap_binddn ldap_bindpw; do vault kv get --field=$$i $(VAULT_SECRET_PATH) > etc/.secrets/$$i ; done

virtualenv:
	python3 -m venv .

venv: virtualenv

pip:
	$(PYTHON_BIN) -m pip install --upgrade pip
	source $(VENV_BIN) && $(PIP_BIN) install -r requirements.txt

deps:
	dnf groupinstall -y "Development Tools"
	dnf install -y python36-devel openldap-devel

apply: venv pip
