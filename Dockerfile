FROM golang:1.20-bullseye

ARG commit=master

WORKDIR /workspace

COPY go.mod go.sum ./
COPY . ./

RUN go mod download
RUN make side-voter
