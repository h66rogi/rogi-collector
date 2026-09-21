# Deployment candidate built from locally verified static Go binaries. No host build.
FROM alpine:3.23
RUN apk add --no-cache ca-certificates
COPY bin/ /app/
COPY LICENSE NOTICE /licenses/
USER 65532:65532
WORKDIR /app
ENTRYPOINT ["/app/query"]
