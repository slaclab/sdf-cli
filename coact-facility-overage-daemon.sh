#!/bin/sh

export PATH=$PATH:/opt/slurm/slurm-curr/bin
export SDF_COACT_URI=coact.slac.stanford.edu:443/graphql-service

while [ 1 ]; do
    date
    ./venv/bin/python3  ./sdf_click.py coact overage --password-file ./etc/.secrets/password --windows 5 --windows 15 --windows 60 --windows 180 --windows 1440 --verbose |  sh
    sleep 300
done
