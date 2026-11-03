#!/bin/bash
#
# Runtime bootstrap for the coact-daemon image.
#
# Waits for the munged sidecar's socket, checks the Slurm client, then execs
# the command it was given.
#
# There is no sssd: user and group lookups are local files only.  sacct's
# User column comes from slurmdbd, and the users the image itself needs
# (slurm, munge, coact-bot) are created in the Dockerfile.

set -euo pipefail

MUNGE_SOCKET="${MUNGE_SOCKET:-/run/munge/munge.socket.2}"
MUNGE_WAIT_SECONDS="${MUNGE_WAIT_SECONDS:-30}"

log()  { printf '[entrypoint] %s\n' "$*" >&2; }
warn() { printf '[entrypoint] WARNING: %s\n' "$*" >&2; }

# --------------------------------------------------------------------------
# Slurm client
#
# munged runs in the pod's munge sidecar; wait for its socket rather than
# letting sacct fail to authenticate.
# --------------------------------------------------------------------------
waited=0
while ! [ -S "$MUNGE_SOCKET" ]; do
    if [ "$waited" -ge "$MUNGE_WAIT_SECONDS" ]; then
        warn "no munge socket at ${MUNGE_SOCKET} after ${MUNGE_WAIT_SECONDS}s; is the munged sidecar running?"
        exit 1
    fi
    sleep 1
    waited=$(( waited + 1 ))
done
log "munge socket present at ${MUNGE_SOCKET}"

if [ ! -r "${SLURM_CONF:-/run/slurm/conf/slurm.conf}" ]; then
    warn "no readable slurm.conf at ${SLURM_CONF:-/run/slurm/conf/slurm.conf}; is the slurm-conf ConfigMap mounted?"
    exit 1
fi
log "slurm.conf found at ${SLURM_CONF:-/run/slurm/conf/slurm.conf}"

if ! command -v sacct >/dev/null 2>&1; then
    warn "sacct not on PATH (${PATH})"
    exit 1
fi
# After the conf check: even `sacct -V` parses slurm.conf.  Checked
# explicitly: set -e does not catch a failure inside an argument.
if ! slurm_version=$(sacct -V); then
    warn "sacct cannot load ${SLURM_CONF:-/run/slurm/conf/slurm.conf}"
    exit 1
fi
log "slurm client: ${slurm_version}"

# `exec` with no arguments is a silent no-op that would exit 0 -- a green
# CronJob run that did nothing at all.
if [ "$#" -eq 0 ]; then
    warn "no command given; nothing to run"
    exit 1
fi

log "exec: $*"
exec "$@"
