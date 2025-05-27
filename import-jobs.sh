#!/bin/bash

export PATH=$PATH:/opt/slurm/slurm-curr/bin
export SDF_COACT_URI=coact.slac.stanford.edu:443/graphql-service

PASSWORD_FILE=./etc/.secrets/password

if [ ! -z $1 ]; then
  DATE=$@
else
  DATE=$(date +"%Y-%m-%d")
fi

# deal with first few minutes of new day; need to do full import of previous day before running it
MIDNIGHT=$(date -d 'today 00:00:00' "+%s")
NOW=$(date "+%s")
DIFF=$(( ($NOW - $MIDNIGHT) ))
if [[ $DIFF -lt 300 ]]; then
  DATE=$(date -d 'yesterday' +"%Y-%m-%d")
fi

echo ">" $DATE" ("$(date)")"

# full
./sdf.py coact slurmdump --date $DATE | tee ../slurm-job-history/$DATE | ./sdf.py coact slurmremap | tee ../slurm-job-remapped/$DATE | ./sdf.py coact slurmimport --password-file $PASSWORD_FILE --output=upload >/dev/null

# just for 2023 imports
#cat ../slurm-job-remapped/$DATE | ./sdf.py coact slurmimport --password-file $PASSWORD_FILE --output=upload >/dev/null

# don't pull data from slurm
#cat ../slurm-job-history/$DATE | ./sdf.py coact slurmremap | tee ../slurm-job-remapped/$DATE | ./sdf.py coact slurmimport --password-file $PASSWORD_FILE --output=upload >/dev/null

###
# recalculate summaries
###
./sdf.py coact slurmrecalculate --password-file=$PASSWORD_FILE --date=$DATE

