FROM golang:alpine as builder

WORKDIR $GOPATH/kafka_exporter
COPY *.go go.mod go.sum ./

ENV GOBIN /usr/local/go/bin

ENTRYPOINT ash
RUN go get ./...
RUN go build -ldflags="-s -w" -o kafka_exporter -i *.go

FROM alpine:latest

WORKDIR /app
COPY --from=builder /go/kafka_exporter/kafka_exporter /bin/kafka_exporter

EXPOSE     9308
ENTRYPOINT [ "/bin/kafka_exporter" ]
