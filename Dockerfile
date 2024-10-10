# deps
FROM fedora:latest AS deps

WORKDIR /app
COPY Makefile .

RUN dnf install -y make
RUN make deps

# install vault
# consider  adding these to Makefile deps recipe unless we need to omit in certain envs
RUN dnf install -y dnf-plugins-core
RUN dnf config-manager --add-repo https://rpm.releases.hashicorp.com/fedora/hashicorp.repo
RUN dnf -y install vault

# resolve "operation not permitted" vault error https://github.com/hashicorp/vault/issues/10924
RUN setcap -r /usr/bin/vault

# build env
FROM deps AS build_1
COPY . ./
RUN make environment

# build sdf protected assets
FROM build_1 AS build_2
RUN echo "building protected assets"
#RUN make update-sdf-ansible
#RUN make get-secrets
