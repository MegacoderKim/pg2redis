# syntax=docker/dockerfile:1.7

ARG PG_MAJOR=15

FROM postgres:${PG_MAJOR}-bookworm AS builder-base

ARG PG_MAJOR=15
ENV PG_MAJOR=${PG_MAJOR}
ENV DEBIAN_FRONTEND=noninteractive
ENV RUSTUP_HOME=/usr/local/rustup
ENV CARGO_HOME=/usr/local/cargo
ENV PATH=/usr/local/cargo/bin:/usr/lib/postgresql/${PG_MAJOR}/bin:$PATH
ENV PGRX_VERSION=0.11.4

RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    ca-certificates \
    curl \
    git \
    pkg-config \
    libssl-dev \
    libreadline-dev \
    zlib1g-dev \
    flex \
    bison \
    libxml2-dev \
    libxslt-dev \
    clang \
    libclang-dev \
    llvm-dev \
    postgresql-server-dev-${PG_MAJOR} \
    && rm -rf /var/lib/apt/lists/*

RUN curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | \
    sh -s -- -y --default-toolchain stable --profile minimal --component rustfmt,clippy \
    && chmod -R a+rX /usr/local/rustup /usr/local/cargo

RUN cargo install cargo-pgrx --version ${PGRX_VERSION} --locked \
    && chmod -R a+rX /usr/local/cargo

RUN cargo pgrx init --pg${PG_MAJOR} /usr/lib/postgresql/${PG_MAJOR}/bin/pg_config

FROM builder-base AS builder

ARG PG_MAJOR=15
WORKDIR /build
COPY Cargo.toml pg_redis_core.control ./
COPY src ./src
COPY sql ./sql

RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/usr/local/cargo/git \
    --mount=type=cache,id=target-pg${PG_MAJOR},target=/build/target \
    cargo pgrx package --pg-config /usr/lib/postgresql/${PG_MAJOR}/bin/pg_config --no-default-features --features pg${PG_MAJOR} \
    && mkdir -p /out \
    && cp -a /build/target/release/pg_redis_core-pg${PG_MAJOR}/. /out/ \
    && cp -a /build/sql/*.sql /out/usr/share/postgresql/${PG_MAJOR}/extension/

FROM builder-base AS dev

ARG PG_MAJOR=15
ENV PG_MAJOR=${PG_MAJOR}
WORKDIR /build
RUN mkdir -p /var/lib/postgresql/.pgrx \
    && chown -R postgres:postgres \
        /build \
        /usr/local/cargo \
        /var/lib/postgresql \
        /usr/lib/postgresql/${PG_MAJOR}/lib \
        /usr/share/postgresql/${PG_MAJOR}/extension
USER postgres
ENV USER=postgres
RUN cargo pgrx init --pg${PG_MAJOR} /usr/lib/postgresql/${PG_MAJOR}/bin/pg_config
COPY --chown=postgres:postgres Cargo.toml pg_redis_core.control ./
COPY --chown=postgres:postgres src ./src
RUN cargo fetch
CMD ["sh", "-c", "cargo pgrx test pg${PG_MAJOR} --no-default-features --features pg${PG_MAJOR}"]

FROM postgres:${PG_MAJOR}-bookworm AS runtime

# Install redis-tools (redis-cli) and netcat for healthchecks & local diagnostics
RUN apt-get update && apt-get install -y --no-install-recommends \
    redis-tools \
    netcat-openbsd \
    && rm -rf /var/lib/apt/lists/*

COPY --from=builder /out/ /
COPY docker/init.sql /docker-entrypoint-initdb.d/01-init-pg-redis.sql

EXPOSE 5432 6379

CMD [ \
    "postgres", \
    "-c", "shared_preload_libraries=pg_redis_core", \
    "-c", "max_worker_processes=16", \
    "-c", "pg_redis.bind_address=0.0.0.0", \
    "-c", "pg_redis.port=6379", \
    "-c", "pg_redis.database=postgres" \
]
