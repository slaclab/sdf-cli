#!/bin/sh

while [ 1 ]; do
    SDF_COACT_URI=coact.slac.stanford.edu:443/graphql-service ./venv/bin/python3 ./sdf_click.py coactd userregistration --username sdf-bot --password-file ./etc/.secrets/password
    sleep 5
done
