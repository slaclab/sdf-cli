#!/bin/sh

while [ 1 ]; do SDF_COACT_URI=coact.slac.stanford.edu/graphql-service ./sdf.py coactd reporegistration --username=sdf-bot --password-file=etc/.secrets/password; sleep 1; done
