#!/bin/bash
#
# Pod-local munged for the coact-daemon image.
#
# The pod authenticates to slurmdbd with its own munged instead of the node's
# socket, so it needs no hostPath.  Two modes, run as two containers:
#
#   munge-sidecar.sh init   root init container.  Kubernetes volumes do not
#                           satisfy munged's ownership/permission checks (the
#                           Secret is a root-owned symlink, emptyDirs are 0777
#                           without the sticky bit), so this copies the key
#                           into a munge-owned 0700 directory and tightens the
#                           shared socket directory.  Needs only CAP_CHOWN.
#   munge-sidecar.sh run    munge user.  Runs munged in the foreground as a
#                           native sidecar; the job container talks to it
#                           through the shared socket directory.

set -euo pipefail

MUNGE_KEY_SOURCE="${MUNGE_KEY_SOURCE:-/run/munge-key/munge.key}"
MUNGE_KEY_DIR="${MUNGE_KEY_DIR:-/etc/munge}"
MUNGE_SOCKET_DIR="${MUNGE_SOCKET_DIR:-/run/munge}"

log() { printf '[munge-sidecar] %s\n' "$*" >&2; }

init() {
    if [ ! -s "$MUNGE_KEY_SOURCE" ]; then
        log "no munge key at ${MUNGE_KEY_SOURCE}; is the coact-daemon-munge secret synced?"
        exit 1
    fi

    # Take ownership, chmod, then hand to munge: changing the mode of a file
    # root does not own would additionally need CAP_FOWNER.  (A fresh emptyDir
    # is already root's; a pre-populated volume may not be.)
    chown root:root "$MUNGE_KEY_DIR" "$MUNGE_SOCKET_DIR"
    chmod 0700 "$MUNGE_KEY_DIR"
    chmod 0755 "$MUNGE_SOCKET_DIR"
    install -m 0400 "$MUNGE_KEY_SOURCE" "$MUNGE_KEY_DIR/munge.key"
    chown munge:munge "$MUNGE_KEY_DIR/munge.key" "$MUNGE_KEY_DIR" "$MUNGE_SOCKET_DIR"

    log "munge key installed in ${MUNGE_KEY_DIR}; socket directory ${MUNGE_SOCKET_DIR} prepared"
}

run() {
    log "starting munged"
    exec /usr/sbin/munged --foreground \
        --key-file="$MUNGE_KEY_DIR/munge.key" \
        --socket="$MUNGE_SOCKET_DIR/munge.socket.2"
}

case "${1:-}" in
    init) init ;;
    run)  run ;;
    *)    log "usage: $0 init|run"; exit 2 ;;
esac
