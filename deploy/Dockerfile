# syntax=docker/dockerfile:1.7
FROM golang:1.27.1-alpine AS build
ARG ROLE
WORKDIR /src
COPY go.work ./
COPY cmd cmd
COPY pkg pkg
COPY proto proto
RUN test "$ROLE" = discover -o "$ROLE" = coordinator -o "$ROLE" = worker -o "$ROLE" = query
RUN CGO_ENABLED=0 go build -trimpath -ldflags='-s -w' -o /out/service ./cmd/${ROLE}

FROM scratch
ARG ROLE
COPY --from=build /out/service /service
USER 65532:65532
EXPOSE 8080
ENTRYPOINT ["/service"]
