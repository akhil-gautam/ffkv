# syntax=docker/dockerfile:1.7
# Published as ghcr.io/akhil-gautam/ffkv. Build stages run on the build host's
# architecture and cross-compile for TARGETARCH, so multi-arch builds avoid
# emulating the Rust compiler.

FROM --platform=$BUILDPLATFORM node:22-bookworm-slim AS console
WORKDIR /build/console
COPY apps/console/package.json apps/console/package-lock.json ./
RUN npm ci --no-audit --no-fund
COPY apps/console/ ./
RUN npm run build

FROM --platform=$BUILDPLATFORM rust:1.99-bookworm AS rust
ARG BUILDARCH
ARG TARGETARCH
RUN set -eux; \
    case "$TARGETARCH" in \
      amd64) triple=x86_64-unknown-linux-gnu; gnu=x86_64-linux-gnu; pkg=gcc-x86-64-linux-gnu ;; \
      arm64) triple=aarch64-unknown-linux-gnu; gnu=aarch64-linux-gnu; pkg=gcc-aarch64-linux-gnu ;; \
      *) echo "unsupported TARGETARCH $TARGETARCH" >&2; exit 1 ;; \
    esac; \
    if [ "$TARGETARCH" != "$BUILDARCH" ]; then \
      apt-get update && apt-get install -y --no-install-recommends "$pkg" libc6-dev-"$TARGETARCH"-cross \
      && rm -rf /var/lib/apt/lists/*; \
    fi; \
    rustup target add "$triple"; \
    echo "$triple" > /target-triple; \
    upper=$(echo "$triple" | tr 'a-z-' 'A-Z_'); lower=$(echo "$triple" | tr '-' '_'); \
    { echo "export CARGO_TARGET_${upper}_LINKER=${gnu}-gcc"; \
      echo "export CC_${lower}=${gnu}-gcc"; \
      echo "export AR_${lower}=${gnu}-ar"; } > /cross-env; \
    if [ "$TARGETARCH" = "$BUILDARCH" ]; then : > /cross-env; fi
WORKDIR /build
COPY Cargo.toml Cargo.lock build.rs ./
COPY src/ ./src/
RUN --mount=type=cache,target=/usr/local/cargo/registry,sharing=locked \
    --mount=type=cache,target=/build/target,id=ffkv-target-$TARGETARCH,sharing=locked \
    set -eux; . /cross-env; triple=$(cat /target-triple); \
    cargo build --release --locked --bins --target "$triple"; \
    mkdir -p /out; \
    cp target/"$triple"/release/ffkv target/"$triple"/release/ffkv-cli target/"$triple"/release/ffkv-mcp /out/

FROM debian:bookworm-slim AS runtime
ARG VERSION=dev
ARG REVISION=unknown
LABEL org.opencontainers.image.title="ffkv" \
      org.opencontainers.image.description="Redis-compatible datastore in Rust, built for queues and background jobs" \
      org.opencontainers.image.source="https://github.com/akhil-gautam/ffkv" \
      org.opencontainers.image.url="https://ffkv.pages.dev/deployment/docker/" \
      org.opencontainers.image.documentation="https://ffkv.pages.dev/" \
      org.opencontainers.image.version="$VERSION" \
      org.opencontainers.image.revision="$REVISION"
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates python3 libgcc-s1 openssl \
    && rm -rf /var/lib/apt/lists/* \
    && groupadd --gid 10001 ffkv && useradd --uid 10001 --gid 10001 --no-create-home --home-dir /nonexistent ffkv \
    && mkdir -p /var/lib/ffkv /run/ffkv /opt/ffkv/console /usr/local/libexec/ffkv \
    && chown 10001:10001 /var/lib/ffkv /run/ffkv
COPY --from=rust /out/ffkv /out/ffkv-mcp /usr/local/bin/
COPY --from=rust /out/ffkv-cli /usr/local/libexec/ffkv/ffkv-cli
COPY --from=console /build/console/dist/ /opt/ffkv/console/
COPY --chmod=0555 deploy/probe.py /usr/local/bin/ffkv-probe
COPY --chmod=0555 deploy/install-secrets.py /usr/local/bin/ffkv-install-secrets
COPY --chmod=0555 deploy/docker-entrypoint.py /usr/local/bin/ffkv-docker-entrypoint
COPY --chmod=0555 deploy/docker-cli.py /usr/local/bin/ffkv-cli
COPY --chmod=0555 deploy/docker-healthcheck.py /usr/local/bin/ffkv-healthcheck
USER 10001:10001
VOLUME ["/var/lib/ffkv"]
EXPOSE 6379 8080
STOPSIGNAL SIGTERM
HEALTHCHECK --interval=15s --timeout=5s --start-period=30s --retries=3 CMD ["python3", "/usr/local/bin/ffkv-healthcheck"]
ENTRYPOINT ["ffkv-docker-entrypoint"]
