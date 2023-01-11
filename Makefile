PYTHON_BIN ?= ./bin/python
PIP_BIN ?= ./bin/pip
VAULT_SECRET_PATH ?= secret/tid/sdf/coact

get-secrets-from-vault:
	mkdir etc/.secrets/ -p
	set -e; for i in ldap_binddn ldap_bindpw; do vault kv get --field=$$i $(VAULT_SECRET_PATH) > etc/.secrets/$$i ; done

virtualenv:
	virtualenv .

deps:
	$(PIP_BIN) install -r requirements.txt


