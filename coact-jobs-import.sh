#!/bin/sh

export SDF_COACT_URI=coact.slac.stanford.edu:443/graphql-service

while [ 1 ]; do ./import-jobs.sh; sleep 120; done
