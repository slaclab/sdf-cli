PYTHON_BIN ?= ./bin/python
PIP_BIN ?= ./bin/pip


virtualenv:
	virtualenv .

deps:
	$(PIP_BIN) install -r requirements.txt


