# deps
FROM fedora:latest AS deps

WORKDIR /app
COPY Makefile .

RUN dnf install -y make
RUN make deps

# build
FROM deps AS build
COPY . ./

RUN make apply
