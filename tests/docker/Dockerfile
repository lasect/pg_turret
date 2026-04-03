## Multi-stage image that builds pg_turret inside a Postgres 18 container

FROM postgres:18-bookworm AS builder

RUN apt-get update && apt-get install -y \
    curl \
    ca-certificates \
    build-essential \
    pkg-config \
    clang \
    libssl-dev \
    postgresql-server-dev-18 \
    && rm -rf /var/lib/apt/lists/*

ENV PATH="/root/.cargo/bin:${PATH}"

WORKDIR /build

# Install Rust toolchain and cargo-pgrx
RUN curl https://sh.rustup.rs -sSf | sh -s -- -y --default-toolchain stable
RUN cargo install --locked cargo-pgrx
RUN cargo pgrx init --pg18 /usr/lib/postgresql/18/bin/pg_config

COPY Cargo.toml Cargo.lock ./
COPY src/ ./src/
COPY pg_turret.control ./

RUN cargo build --release --no-default-features --features "pg18 std"
RUN cargo pgrx schema --no-default-features --features "pg18 std" --pg-config /usr/lib/postgresql/18/bin/pg_config > /build/pg_turret--0.0.0.sql
RUN sed -i 's/@CARGO_VERSION@/0.0.0/g' /build/pg_turret.control

FROM postgres:18-bookworm

COPY --from=builder /build/target/release/libpg_turret.so /usr/lib/postgresql/18/lib/pg_turret.so
COPY --from=builder /build/pg_turret.control /usr/share/postgresql/18/extension/pg_turret.control
COPY --from=builder /build/pg_turret--0.0.0.sql /usr/share/postgresql/18/extension/pg_turret--0.0.0.sql

RUN echo "shared_preload_libraries = 'pg_turret'" >> /usr/share/postgresql/postgresql.conf.sample
