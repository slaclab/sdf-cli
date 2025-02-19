# deps
FROM rockylinux:9.2 AS deps

WORKDIR /app
COPY Makefile .

RUN dnf install -y epel-release 
RUN dnf install -y make ansible
RUN make deps


# build env
FROM deps AS build_1
COPY . ./
RUN make environment
