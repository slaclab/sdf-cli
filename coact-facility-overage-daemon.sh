#!/bin/sh

export PATH=$PATH:/opt/slurm/slurm-curr/bin
#export SDF_COACT_URI=coact-dev.slac.stanford.edu:443/graphql-service-dev
export SDF_COACT_URI=coact.slac.stanford.edu:443/graphql-service

while [ 1 ]; do
    date
    old_venv/bin/python  ./sdf_click.py coact overage --password-file ./etc/.secrets/password --verbose --influxdb-url=https://influxdb.slac.stanford.edu:443
    sleep 300
done
