FROM python:3.9-slim

# Install system-level dependencies required by Ansible modules and LDAP client
RUN apt-get update && apt-get install -y --no-install-recommends \
        build-essential \
        libldap2-dev \
        libsasl2-dev \
        libssl-dev \
        openssh-client \
        git \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Install Python dependencies before copying source for better layer caching
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy sdf-cli source and ansible-runner content.
# IMPORTANT: run 'git submodule update --init --recursive' before building
# so that ansible-runner/project (sdf-ansible) is fully populated.
COPY . .

# Prepare the secrets mount point; contents are injected at runtime via
# Kubernetes Secrets — never baked into the image.
RUN mkdir -p /app/etc/.secrets && chmod 700 /app/etc/.secrets

# SSH directory for Ansible connections to managed infrastructure hosts.
# The private key is mounted at runtime from a Kubernetes Secret.
RUN mkdir -p /root/.ssh && chmod 700 /root/.ssh

ENTRYPOINT ["python", "/app/sdf_click.py"]
