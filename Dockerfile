FROM golang:1.26.7-bookworm@sha256:e8c859f5632dcfde7b32d2012b4351728f6437930887c2f6a91ea242459e5514 AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN --mount=type=cache,target=/go/pkg/mod go mod download
COPY . .
RUN --mount=type=cache,target=/root/.cache/go-build --mount=type=cache,target=/go/pkg/mod \
    for app in coordinator agent begomctl mockdonor node-helper; do \
      CGO_ENABLED=0 go build -trimpath -ldflags='-s -w' -o /out/$app ./cmd/$app || exit; \
    done

FROM debian:bookworm-slim@sha256:7ff6ba31527b9e336162fa061746c366c4b2032f4d2cff2078bea43f78efe3f8 AS app
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/* \
    && mkdir -p /config /data/staging/worker-1 /data/staging/worker-2 /data/staging/worker-3 /run/secrets /updates
COPY --from=build /out/ /usr/local/bin/
USER 10000:10000
ENTRYPOINT ["/usr/local/bin/coordinator"]

FROM postgres:18.6-bookworm@sha256:bbf83594798e144bff365a9c191c5d779f181fb3135902064ab8ff11918020fc AS database
COPY --from=build /out/begomctl /usr/local/bin/begomctl
COPY deploy/postgres/init.sh /docker-entrypoint-initdb.d/10-begom.sh

FROM restic/restic:0.19.1@sha256:136600b6ff6843d61d355f7f71f460a166429f35de6fd11b568fece3c9a4d510 AS restic
FROM postgres:18.6-bookworm@sha256:bbf83594798e144bff365a9c191c5d779f181fb3135902064ab8ff11918020fc AS backup
COPY --from=restic /usr/bin/restic /usr/local/bin/restic
COPY --from=build /out/begomctl /usr/local/bin/begomctl
USER 10000:10000
ENTRYPOINT ["/usr/local/bin/begomctl"]

FROM build AS fault-test-build
RUN --mount=type=cache,target=/root/.cache/go-build --mount=type=cache,target=/go/pkg/mod \
    CGO_ENABLED=0 go test -c -o /out/agent.test ./internal/agent
FROM app AS storage-faults
COPY --from=fault-test-build /out/agent.test /usr/local/bin/agent.test
ENTRYPOINT ["/usr/local/bin/agent.test"]
CMD ["-test.run=^TestDownloadENOSPC$", "-test.v"]
