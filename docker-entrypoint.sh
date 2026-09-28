#!/bin/bash
#
# Runtime bootstrap for the coact-daemon image.
#
# Starts sssd for NSS/LDAP resolution, waits for the munged sidecar's socket,
# then execs the command it was given.

set -euo pipefail

START_SSSD="${START_SSSD:-true}"
SSSD_WAIT_SECONDS="${SSSD_WAIT_SECONDS:-15}"
MUNGE_SOCKET="${MUNGE_SOCKET:-/run/munge/munge.socket.2}"
MUNGE_WAIT_SECONDS="${MUNGE_WAIT_SECONDS:-30}"

log()  { printf '[entrypoint] %s\n' "$*" >&2; }
warn() { printf '[entrypoint] WARNING: %s\n' "$*" >&2; }

# --------------------------------------------------------------------------
# sssd
# --------------------------------------------------------------------------
start_sssd() {
    if [ ! -f /etc/sssd/sssd.conf ]; then
        warn "/etc/sssd/sssd.conf missing; skipping sssd"
        return 0
    fi

    # Allow the LDAP endpoint to be retargeted without rebuilding the image.
    if [ -n "${SSSD_LDAP_URI:-}" ]; then
        log "overriding sssd ldap_uri with ${SSSD_LDAP_URI}"
        sed -i "s|^ldap_uri = .*|ldap_uri = ${SSSD_LDAP_URI}|" /etc/sssd/sssd.conf
    fi
    if [ -n "${SSSD_LDAP_BASE_DN:-}" ]; then
        log "overriding sssd ldap_search_base with ${SSSD_LDAP_BASE_DN}"
        sed -i "s|^ldap_search_base = .*|ldap_search_base = ${SSSD_LDAP_BASE_DN}|" /etc/sssd/sssd.conf
    fi
    chmod 0600 /etc/sssd/sssd.conf

    # mkdir -p, not `install -d -m`: the sssd RPM already creates these
    # owned by the sssd user, and chmod-ing a directory we do not own
    # would need CAP_FOWNER purely to set the mode it already has.
    mkdir -p /var/lib/sss/db /var/lib/sss/mc /var/lib/sss/pipes/private /var/log/sssd

    log "starting sssd"
    if ! /usr/sbin/sssd -D; then
        warn "sssd failed to start; NSS/LDAP name resolution unavailable (continuing)"
        return 0
    fi

    local waited=0
    while ! pgrep -x sssd >/dev/null 2>&1; do
        if [ "$waited" -ge "$SSSD_WAIT_SECONDS" ]; then
            warn "sssd did not come up within ${SSSD_WAIT_SECONDS}s (continuing)"
            return 0
        fi
        sleep 1
        waited=$(( waited + 1 ))
    done
    log "sssd running"
}

# --------------------------------------------------------------------------
if [ "$START_SSSD" = "true" ]; then
    start_sssd
else
    log "START_SSSD=${START_SSSD}; not starting sssd"
fi

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
# After the conf check: even `sacct -V` parses slurm.conf.
log "slurm client: $(sacct -V)"

# `exec` with no arguments is a silent no-op that would exit 0 -- a green
# CronJob run that did nothing at all.
if [ "$#" -eq 0 ]; then
    warn "no command given; nothing to run"
    exit 1
fi

log "exec: $*"
exec "$@"
