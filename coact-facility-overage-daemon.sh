#!/bin/sh

export SDF_COACT_URI=coact.slac.stanford.edu:443/graphql-service
while [ 1 ]; do date;  ./sdf.py coact overage --password-file ../sdf-cli//etc/.secrets/password --windows 180 3600 10080 43800 --threshold=100 ; sleep 300; done
