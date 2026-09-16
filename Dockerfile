# Container image for the Coact batch daemons
FROM rockylinux:9

LABEL org.opencontainers.image.source=https://github.com/slaclab/sdf-cli
LABEL org.opencontainers.image.description="Coact batch daemons (slurm job import, facility overage)"

COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /bin/

# Build behind the SDF proxy with:
#   podman build --build-arg https_proxy=http://sdfproxy.sdf.slac.stanford.edu:3128 .
ARG http_proxy
ARG https_proxy
ARG no_proxy

# ---------------------------------------------------------------------------
# OS packages
# ---------------------------------------------------------------------------
RUN dnf -y install epel-release \
 && dnf -y --setopt=install_weak_deps=False --setopt=tsflags=nodocs install \
      munge \
      sssd \
      sssd-client \
      nss-pam-ldapd \
      openldap-clients \
      krb5-workstation \
      python3.12 \
      python3.12-pip \
      tini \
      tzdata \
      procps-ng \
      which \
      glibc-langpack-en \
      shadow-utils \
      ca-certificates \
 && dnf clean all \
 && rm -rf /var/cache/dnf /var/cache/yum

# Align the munge uid/gid with the SDF hosts
ARG MUNGE_UID=16952
ARG MUNGE_GID=3761
RUN groupmod -g $MUNGE_GID munge \
 && usermod  -u $MUNGE_UID -g $MUNGE_GID munge

# ---------------------------------------------------------------------------
# Runtime environment
# ---------------------------------------------------------------------------
ENV UV_PROJECT_ENVIRONMENT=/opt/venv \
    UV_PYTHON=/usr/bin/python3.12 \
    UV_LINK_MODE=copy \
    SLURM_BIN_DIR=/opt/slurm/slurm-curr/bin \
    SLURM_CONF=/run/slurm/conf/slurm.conf \
    PATH=/opt/venv/bin:/opt/slurm/slurm-curr/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin \
    TZ=America/Los_Angeles \
    LANG=en_US.UTF-8 \
    PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1

WORKDIR /app

# Dependency layer first so code changes do not invalidate the resolve.
COPY pyproject.toml uv.lock /app/
RUN uv sync --frozen --no-install-project --no-dev

COPY . /app
RUN uv sync --frozen --no-dev

# ansible-runner 2.3.1 imports `pkg_resources` at module scope, and
# sdf_click.py imports modules/coactd.py (hence ansible_runner) unconditionally
# -- even for the `coact` batch subcommands.  setuptools >= 81 dropped
# pkg_resources, so the CLI will not start without an older setuptools.
RUN uv pip install --python /opt/venv/bin/python "setuptools<81"

# ---------------------------------------------------------------------------
# Identity / auth configuration
# ---------------------------------------------------------------------------
COPY etc/nsswitch.conf /etc/nsswitch.conf
COPY etc/krb5.conf     /etc/krb5.conf
COPY etc/ldap.conf     /etc/openldap/ldap.conf
COPY etc/sssd.conf     /etc/sssd/sssd.conf
RUN chmod 0600 /etc/sssd/sssd.conf \
 && install -d -m 0755 /data

COPY docker-entrypoint.sh /usr/local/bin/docker-entrypoint.sh
RUN chmod 0755 /usr/local/bin/docker-entrypoint.sh /app/import-jobs.sh /app/run-overage.sh

# tini reaps sssd and forwards signals; the entrypoint execs the CronJob
# command so the container exits with the job's own exit code.
ENTRYPOINT ["/usr/bin/tini", "--", "/usr/local/bin/docker-entrypoint.sh"]
CMD ["python3", "/app/sdf_click.py", "--help"]
