FROM alpine:latest
RUN apk add --no-cache ca-certificates
COPY manager /manager
ENTRYPOINT ["/manager"]
