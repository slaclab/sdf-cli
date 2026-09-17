#!/bin/bash
#
# Single-shot Slurm job accounting import.
#
#   sacct -> slurmdump | slurmremap | slurmimport   (then slurmrecalculate)
#
# Usage:
#   ./import-jobs.sh              # today (or yesterday just after midnight)
#   ./import-jobs.sh 2026-08-31   # explicit date, for replay/backfill
set -euo pipefail

: "${SDF_COACT_URI:?SDF_COACT_URI must be set, e.g. coact.slac.stanford.edu:443/graphql-service (no scheme)}"
export SDF_COACT_URI

SLURM_BIN_DIR="${SLURM_BIN_DIR:-/opt/slurm/slurm-curr/bin}"
case ":$PATH:" in
  *":$SLURM_BIN_DIR:"*) ;;
  *) PATH="$PATH:$SLURM_BIN_DIR" ;;
esac
export PATH

COACT_USERNAME="${COACT_USERNAME:-sdf-bot}"

# Sourced from Vault via the coact-daemon secret; see
# deploy/kubernetes/overlays/dev/daemon/externalsecret.yaml.
: "${COACT_PASSWORD:?COACT_PASSWORD must be set (Vault secret/scs/coact-dev/service-account field 'password')}"
export COACT_PASSWORD

JOB_HISTORY_DIR="${JOB_HISTORY_DIR:-/data/slurm-job-history}"
JOB_REMAPPED_DIR="${JOB_REMAPPED_DIR:-/data/slurm-job-remapped}"

PYTHON="${PYTHON:-python3}"
SDF_CLICK="${SDF_CLICK:-$(dirname "$0")/sdf_click.py}"

# --------------------------------------------------------------------------
# Which day to import
# --------------------------------------------------------------------------
if [ -n "${1:-}" ]; then
  DATE="$*"
else
  DATE=$(date +"%Y-%m-%d")

  # Deal with the first few minutes of a new day: the previous day needs a
  # full import before today's partial import is meaningful.  This is local
  # time, which is why the container pins TZ (America/Los_Angeles).
  MIDNIGHT=$(date -d 'today 00:00:00' "+%s")
  NOW=$(date "+%s")
  DIFF=$(( NOW - MIDNIGHT ))
  if [ "$DIFF" -lt 300 ]; then
    DATE=$(date -d 'yesterday' +"%Y-%m-%d")
  fi
fi

mkdir -p "$JOB_HISTORY_DIR" "$JOB_REMAPPED_DIR"

RAW_ARCHIVE="$JOB_HISTORY_DIR/$DATE"
REMAPPED_ARCHIVE="$JOB_REMAPPED_DIR/$DATE"
RAW_PARTIAL="$RAW_ARCHIVE.partial"
REMAPPED_PARTIAL="$REMAPPED_ARCHIVE.partial"

# Any exit before the promotion step below leaves the previous good archive
# for this date untouched.
trap 'rm -f "$RAW_PARTIAL" "$REMAPPED_PARTIAL"' EXIT

echo "> $DATE ($(date))"

# --------------------------------------------------------------------------
# Full pipeline.
#
# The dumps are teed to `.partial` files and only moved into place once the
# whole pipeline has succeeded.
#
# `--output-error=warn-nopipe` keeps tee writing after a
# downstream stage closes the pipe, instead of dying on SIGPIPE with its
# buffered writes unflushed.
# --------------------------------------------------------------------------
"$PYTHON" "$SDF_CLICK" coact slurmdump --date "$DATE" \
    | tee --output-error=warn-nopipe "$RAW_PARTIAL" \
    | "$PYTHON" "$SDF_CLICK" coact slurmremap \
    | tee --output-error=warn-nopipe "$REMAPPED_PARTIAL" \
    | "$PYTHON" "$SDF_CLICK" coact slurmimport \
        --username "$COACT_USERNAME" \
        --output=upload >/dev/null

# A successful sacct always emits at least the header row, so an empty dump
# here means something went wrong that the exit codes did not surface.
if [ ! -s "$RAW_PARTIAL" ]; then
  echo "error: raw sacct dump for $DATE is empty; keeping the existing archive" >&2
  exit 1
fi
if [ ! -s "$REMAPPED_PARTIAL" ]; then
  echo "error: remapped dump for $DATE is empty; keeping the existing archive" >&2
  exit 1
fi

mv -f "$RAW_PARTIAL" "$RAW_ARCHIVE"
mv -f "$REMAPPED_PARTIAL" "$REMAPPED_ARCHIVE"

# --------------------------------------------------------------------------
# Recalculate usage summaries
# --------------------------------------------------------------------------
"$PYTHON" "$SDF_CLICK" coact slurmrecalculate \
    --username "$COACT_USERNAME" \
    --date "$DATE"
