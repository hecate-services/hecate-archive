# hecate-archive — L2 hecate-om service: the collector.
#
# Subscribes to verbatim sensor observations on the mesh and writes them to an
# append-only tape of sealed segment files. Storeless in the reckon sense (no
# reckon-db), but it is the one service in the sensor family that owns a disk,
# and that disk is the record.
#
# Depends on hecate_om, which brings the macula mesh SDK and its Rust QUIC NIF,
# so the builder carries the Rust toolchain and builds that NIF from source
# (never a fetched artifact), the same as every macula-linked image.
#
# Pushed to ghcr.io/hecate-services/hecate-archive:latest + :semver.

#----------------------------------------------------------------------
# Stage 1 — builder: Erlang + Rust + rebar3 + deps + release
#----------------------------------------------------------------------
FROM docker.io/erlang:27-alpine AS builder
WORKDIR /build

RUN apk add --no-cache git curl bash build-base cmake perl linux-headers
RUN curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs \
        | sh -s -- -y --default-toolchain stable --profile minimal
ENV PATH="/root/.cargo/bin:${PATH}"
ENV RUSTFLAGS="-C target-feature=-crt-static"
ENV MACULA_FORCE_SOURCE_BUILD=1
# rebar3 runs the macula NIF build hook in a subprocess whose PATH lacks
# /root/.cargo/bin, so symlink the rustup proxies onto the default PATH.
RUN ln -sf /root/.cargo/bin/rustup /usr/local/bin/cargo \
    && ln -sf /root/.cargo/bin/rustup /usr/local/bin/rustc \
    && ln -sf /root/.cargo/bin/rustup /usr/local/bin/rustup \
    && cargo --version && rustc --version

RUN curl -fsSL https://s3.amazonaws.com/rebar3/rebar3 -o /usr/local/bin/rebar3 \
    && chmod +x /usr/local/bin/rebar3

# Deps first (cacheable until rebar.config changes). No rebar.lock in git.
COPY rebar.config ./
RUN rebar3 get-deps

COPY config ./config
COPY apps ./apps
RUN rebar3 as prod release

#----------------------------------------------------------------------
# Stage 2 — runtime: bare Alpine + the assembled release
#----------------------------------------------------------------------
FROM docker.io/alpine:3.22
RUN apk add --no-cache ncurses-libs libstdc++ libgcc openssl ca-certificates curl
WORKDIR /app
COPY --from=builder /build/_build/prod/rel/hecate_archive ./
RUN mkdir -p /var/lib/hecate-archive /bulk0/hecate-archive

ENV HOME=/app
# Substitute ${VAR} in vm.args/sys.config from the container env at boot.
ENV RELX_REPLACE_OS_VARS=true

# Per-node defaults; every ${VAR} in sys.config/vm.args must resolve or the term
# is malformed. The deploy overrides realm, node name/host/cookie, and the tape
# root.
ENV HECATE_NODE_NAME=hecate_archive
ENV HECATE_NODE_HOST=127.0.0.1
ENV HECATE_COOKIE=hecate_archive
ENV HECATE_HEALTH_PORT=8471

# The tape. On a beam node this MUST be bound to a /bulk mount: the eMMC root is
# for the OS, and an archive that fills the root partition takes the node with
# it.
ENV HECATE_ARCHIVE_ROOT=/bulk0/hecate-archive

# Realm service-principal cert mounts here; station socket under /run/macula.
VOLUME ["/etc/hecate/secrets", "/var/lib/hecate-archive", "/bulk0/hecate-archive"]

EXPOSE 8471
HEALTHCHECK --interval=30s --timeout=5s --start-period=20s --retries=3 \
    CMD curl -fsS "http://127.0.0.1:${HECATE_HEALTH_PORT}/health" || exit 1

ENTRYPOINT ["/app/bin/hecate_archive"]
CMD ["foreground"]
