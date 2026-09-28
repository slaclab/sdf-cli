# Container image for the Coact batch daemons

# ---------------------------------------------------------------------------
# Slurm client build
#
# The Slurm client ships in the image rather than being mounted from the
# node's /opt/slurm, so the pod needs no hostPath.  SLURM_VERSION must track
# the S3DF slurmctld/slurmdbd: a client newer than the servers is unsupported.
# ---------------------------------------------------------------------------
FROM rockylinux:9 AS slurm-build

ARG http_proxy
ARG https_proxy
ARG no_proxy

ARG SLURM_VERSION=25.11.8
ARG SLURM_SHA256=34ace13f81011add6094569d13bfc4006ad8868201c2236e2905443c7e526393

# munge-devel and readline-devel live in CRB.
RUN dnf -y install epel-release dnf-plugins-core \
 && dnf config-manager --set-enabled crb \
 && dnf -y --setopt=install_weak_deps=False install \
      gcc make bzip2 perl python3 \
      munge-devel readline-devel \
 && dnf clean all

WORKDIR /build
RUN curl -fsSLo slurm.tar.bz2 "https://download.schedmd.com/slurm/slurm-${SLURM_VERSION}.tar.bz2" \
 && echo "${SLURM_SHA256}  slurm.tar.bz2" | sha256sum -c - \
 && tar xjf slurm.tar.bz2 --strip-components=1 \
 && rm slurm.tar.bz2

# Same prefix as the S3DF RPMs (/opt/slurm/slurm-<ver>, with slurm-curr
# symlinked to it), so PATH and SLURM_BIN_DIR are unchanged from bare metal.
RUN ./configure \
      --prefix=/opt/slurm/slurm-${SLURM_VERSION} \
      --libdir=/opt/slurm/slurm-${SLURM_VERSION}/lib64 \
      --sysconfdir=/etc/slurm \
      --disable-slurmrestd \
 && make -j"$(nproc)" \
 && make install \
 && rm -rf /opt/slurm/slurm-${SLURM_VERSION}/share /opt/slurm/slurm-${SLURM_VERSION}/include \
 && test -e /opt/slurm/slurm-${SLURM_VERSION}/lib64/slurm/auth_munge.so \
 && test -e /opt/slurm/slurm-${SLURM_VERSION}/lib64/slurm/accounting_storage_slurmdbd.so

# ---------------------------------------------------------------------------
# Runtime image
# ---------------------------------------------------------------------------
FROM rockylinux:9

ARG SLURM_VERSION=25.11.8

LABEL org.opencontainers.image.source=https://github.com/slaclab/sdf-cli
LABEL org.opencontainers.image.description="Coact batch daemons (slurm job import, facility overage)"
LABEL edu.stanford.slac.slurm.version="${SLURM_VERSION}"

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
      readline \
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
# usermod only re-owns the home directory, so the rest of munge's tree is
# re-owned explicitly -- munged refuses directories it does not own.
RUN groupmod -g $MUNGE_GID munge \
 && usermod  -u $MUNGE_UID -g $MUNGE_GID munge \
 && chown -R munge:munge /etc/munge /var/lib/munge /var/log/munge /run/munge

COPY --from=slurm-build /opt/slurm /opt/slurm
RUN ln -s slurm-${SLURM_VERSION} /opt/slurm/slurm-curr \
 && echo /opt/slurm/slurm-curr/lib64 > /etc/ld.so.conf.d/slurm.conf \
 && ldconfig

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
COPY munge-sidecar.sh     /usr/local/bin/munge-sidecar.sh
RUN chmod 0755 /usr/local/bin/docker-entrypoint.sh /usr/local/bin/munge-sidecar.sh /app/import-jobs.sh

# tini reaps sssd and forwards signals; the entrypoint execs the CronJob
# command so the container exits with the job's own exit code.
ENTRYPOINT ["/usr/bin/tini", "--", "/usr/local/bin/docker-entrypoint.sh"]
CMD ["python3", "/app/sdf_click.py", "--help"]
