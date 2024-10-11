# deps
FROM fedora:latest AS deps

WORKDIR /app
COPY Makefile .

RUN dnf install -y make
RUN make deps


# build env
FROM deps AS build_1
COPY . ./
RUN make environment


# DO NOT BAKE SECRETS AND PROTECTED ASSETS INTO PROD IMAGE
# THIS IS ONLY FOR REFERENCE

## build sdf protected assets
#FROM build_1 AS build_2
#RUN echo "building protected assets"
#
## install vault
#RUN dnf install -y dnf-plugins-core
#RUN dnf config-manager --add-repo https://rpm.releases.hashicorp.com/fedora/hashicorp.repo
#RUN dnf -y install vault
# resolve "operation not permitted" vault error https://github.com/hashicorp/vault/issues/10924
#RUN setcap -r /usr/bin/vault
#
#RUN make update-sdf-ansible
#RUN make get-secrets
