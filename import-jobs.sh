#!/bin/bash

export SDF_COACT_URI=coact.slac.stanford.edu:443/graphql-service

DATE=$@
PASSWORD_FILE=./etc/.secrets/password

echo $DATE
./sdf.py coact slurmdump --date $DATE | tee ../slurm-job-history/$DATE | ./sdf.py coact slurmremap | tee ../slurm-job-remapped/$DATE | ./sdf.py coact slurmimport --password-file $PASSWORD_FILE --output=upload >/dev/null
./sdf.py coact slurmrecalculate --password-file=$PASSWORD_FILE --date=$DATE

