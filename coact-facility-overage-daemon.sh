#!/bin/sh

export PATH=$PATH:/opt/slurm/slurm-curr/bin

export SDF_COACT_URI=coact.slac.stanford.edu:443/graphql-service
while [ 1 ]; do date;  ./sdf.py coact overage --password-file ../sdf-cli//etc/.secrets/password --windows 5 60 180 1440 --threshold=100 ; sleep 300; done
