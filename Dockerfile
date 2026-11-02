FROM node:22-bookworm-slim AS console
WORKDIR /build/console
COPY apps/console/package.json apps/console/package-lock.json ./
RUN npm ci --no-audit --no-fund
COPY apps/console/ ./
RUN npm run build

FROM rust:1.99-bookworm AS rust
WORKDIR /build
COPY Cargo.toml Cargo.lock ./
COPY src/ ./src/
RUN cargo build --release --locked

FROM debian:bookworm-slim AS runtime
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates python3 libgcc-s1 \
    && rm -rf /var/lib/apt/lists/* \
    && groupadd --gid 10001 ffkv && useradd --uid 10001 --gid 10001 --no-create-home --home-dir /nonexistent ffkv \
    && mkdir -p /var/lib/ffkv /run/ffkv /opt/ffkv/console \
    && chown 10001:10001 /var/lib/ffkv /run/ffkv
COPY --from=rust /build/target/release/ffkv /usr/local/bin/ffkv
COPY --from=rust /build/target/release/ffkv-cli /usr/local/bin/ffkv-cli
COPY --from=console /build/console/dist/ /opt/ffkv/console/
COPY --chmod=0555 deploy/probe.py /usr/local/bin/ffkv-probe
COPY --chmod=0555 deploy/install-secrets.py /usr/local/bin/ffkv-install-secrets
USER 10001:10001
EXPOSE 6379 8080
STOPSIGNAL SIGTERM
HEALTHCHECK --interval=15s --timeout=5s --start-period=15s --retries=3 CMD ["python3", "/usr/local/bin/ffkv-probe", "--host", "127.0.0.1", "--port", "6379", "--username", "health", "--password-file", "/run/ffkv/health-password", "--tls-ca", "/run/ffkv/ca.crt", "--tls-server-name", "localhost"]
ENTRYPOINT ["ffkv"]
CMD ["--bind", "0.0.0.0:6379", "--data-dir", "/var/lib/ffkv", "--durability", "strict", "--acl-file", "/run/ffkv/users.acl", "--tls-cert", "/run/ffkv/tls.crt", "--tls-key", "/run/ffkv/tls.key"]
