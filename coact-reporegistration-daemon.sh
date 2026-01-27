#!/bin/sh

while [ 1 ]; do
    SDF_COACT_URI=coact.slac.stanford.edu/graphql-service ./venv/bin/python3 ./sdf_click.py coactd reporegistration --username=sdf-bot --password-file=etc/.secrets/password -vv
    sleep 1
done
