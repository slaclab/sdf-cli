FROM fedora:latest

WORKDIR /app

COPY . ./

RUN dnf install -y make

RUN make deps
