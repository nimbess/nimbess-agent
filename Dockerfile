FROM golang:1.11
WORKDIR $GOPATH/src/github.com/nimbess/nimbess-agent
ENV GO111MODULE=on
COPY . .
RUN ./build/build-go.sh

FROM centos:latest
COPY --from=0 /go/src/github.com/nimbess/nimbess-agent/bin/nimbess-agent .
