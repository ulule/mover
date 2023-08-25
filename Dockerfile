# syntax=docker/dockerfile:1
FROM golang:1.15-alpine AS mover-builder

ARG GO111MODULE=on
ARG CGO_ENABLED=0

WORKDIR /go/src/github.com/ulule/mover

COPY --link . .

RUN --mount=type=cache,target=/root/.cache/go-build \
  go build -o mover ./cmd/mover/

FROM scratch

COPY --from=mover-builder /go/src/github.com/ulule/mover/mover /mover

ENTRYPOINT ["/mover"]