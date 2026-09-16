#!/bin/bash
#
# Single-shot facility overage enforcement.
#

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
COACT_PASSWORD_FILE="${COACT_PASSWORD_FILE:-/etc/coact/secrets/password}"

INFLUXDB_URL="${INFLUXDB_URL:-https://influxdb.slac.stanford.edu:443}"
INFLUXDB_DATABASE="${INFLUXDB_DATABASE:-coact}"

PYTHON="${PYTHON:-python3}"
SDF_CLICK="${SDF_CLICK:-$(dirname "$0")/sdf_click.py}"

if [ ! -r "$COACT_PASSWORD_FILE" ]; then
  echo "error: GraphQL password file not readable: $COACT_PASSWORD_FILE" >&2
  exit 1
fi

# OVERAGE_DRY_RUN=true collects usage and writes to InfluxDB but does not run
# `sacctmgr modify` to hold/release jobs.
DRY_RUN_ARGS=()
if [ "${OVERAGE_DRY_RUN:-false}" = "true" ]; then
  DRY_RUN_ARGS+=( --dry-run )
fi

date

exec "$PYTHON" "$SDF_CLICK" coact overage \
    --username "$COACT_USERNAME" \
    --password-file "$COACT_PASSWORD_FILE" \
    --windows 5 \
    --windows 15 \
    --windows 60 \
    --windows 180 \
    --windows 1440 \
    --verbose \
    --influxdb-url="$INFLUXDB_URL" \
    --influxdb-database="$INFLUXDB_DATABASE" \
    ${DRY_RUN_ARGS[@]+"${DRY_RUN_ARGS[@]}"}
