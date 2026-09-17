#!/bin/bash

while [ 1 ]; do
    SDF_COACT_URI=coact.slac.stanford.edu/graphql-service old_venv/bin/python ./sdf_click.py coactd reporegistration --username=sdf-bot --password-file=etc/.secrets/password --grouper-password-file /sdf/home/r/ryanw/code/sdf-cli/etc/.secrets/grouper_password -vv
    sleep 1
done
