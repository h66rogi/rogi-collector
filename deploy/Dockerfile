# syntax=docker/dockerfile:1.7
FROM golang:1.27.1-alpine AS build
ARG ROLE
WORKDIR /src
COPY go.work ./
COPY discover discover
COPY coordinator coordinator
COPY worker worker
COPY query query
COPY shared shared
COPY proto proto
COPY cleanup cleanup
COPY chat-exporter chat-exporter
RUN test "$ROLE" = discover -o "$ROLE" = coordinator -o "$ROLE" = worker -o "$ROLE" = query -o "$ROLE" = data-api
RUN --mount=type=cache,target=/go/pkg/mod --mount=type=cache,target=/root/.cache/go-build target="./${ROLE}/cmd"; \
    if [ "$ROLE" = data-api ]; then target=./query/cmd/data-api; fi; \
    CGO_ENABLED=0 go build -trimpath -ldflags='-s -w' -o /out/service "$target" && \
    CGO_ENABLED=0 go build -trimpath -ldflags='-s -w' -o /out/healthcheck ./shared/cmd/healthcheck && \
    CGO_ENABLED=0 go build -trimpath -ldflags='-s -w' -o /out/migrate ./shared/cmd/migrate && \
    CGO_ENABLED=0 go build -trimpath -ldflags='-s -w' -o /out/collector-check ./shared/cmd/collector-check
FROM alpine:3.23
RUN apk add --no-cache ca-certificates
COPY --from=build /out/ /
COPY LICENSE NOTICE /licenses/
USER 65532:65532
ENV ROLE_ENV_FILE=/run/secrets/role.env
EXPOSE 8080 7443
ENTRYPOINT ["/service"]
