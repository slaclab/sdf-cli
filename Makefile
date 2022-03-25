PYTHON_BIN ?= ./bin/python
PIP_BIN ?= ./bin/pip


install:
	virtualenv .
	$(PIP_BIN) install -r requirements.txt


