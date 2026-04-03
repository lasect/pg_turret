#!/bin/bash
set -e

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$SCRIPT_DIR"

if [ -z "$1" ]; then
    echo "Usage: $0 <SENTRY_DSN>"
    exit 1
fi

SENTRY_DSN="$1"

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

log_info() { echo -e "${GREEN}[INFO]${NC} $1"; }
log_warn() { echo -e "${YELLOW}[WARN]${NC} $1"; }
log_error() { echo -e "${RED}[ERROR]${NC} $1"; }
log_test() { echo -e "${GREEN}[TEST]${NC} $1"; }

cleanup() {
    log_info "Cleaning up..."
    docker compose -f docker-compose.kafka.yml down --volumes --remove-orphans 2>/dev/null || true
    rm -rf dist/
}

trap cleanup EXIT

# 0. Kill any existing containers
docker compose -f docker-compose.kafka.yml down -v --remove-orphans 2>/dev/null || true

# 1. Build extension
log_info "Building extension for PostgreSQL 18..."
rm -rf dist/
mkdir -p dist/usr/lib/postgresql/18/lib/
mkdir -p dist/usr/share/postgresql/18/extension/

cargo build --manifest-path ../../Cargo.toml --release --no-default-features --features "pg18 std"
cargo pgrx schema --manifest-path ../../Cargo.toml --no-default-features --features "pg18 std" --pg-config pg18 > dist/usr/share/postgresql/18/extension/pg_turret--0.0.0.sql

FIND_ROOT="../../target/release"
cp "$FIND_ROOT/libpg_turret.so" dist/usr/lib/postgresql/18/lib/pg_turret.so 2>/dev/null || \
cp "$FIND_ROOT/pg_turret.so" dist/usr/lib/postgresql/18/lib/pg_turret.so

cp "../../pg_turret.control" dist/usr/share/postgresql/18/extension/
sed -i 's/@CARGO_VERSION@/0.0.0/g' dist/usr/share/postgresql/18/extension/pg_turret.control

# 3. Start containers (reuse existing docker-compose for Postgres)
log_info "Building and starting containers..."
docker compose -f docker-compose.kafka.yml build --no-cache
docker compose -f docker-compose.kafka.yml up -d

wait_for_postgres() {
    log_info "Waiting for Postgres..."
    local attempts=0
    until pg_isready -h localhost -p 5434 -U postgres 2>/dev/null; do
        sleep 2
        attempts=$((attempts + 1))
        if [ $attempts -gt 30 ]; then
            log_error "Postgres failed to start"
            docker compose -f docker-compose.kafka.yml logs db
            exit 1
        fi
    done
    log_info "Postgres is ready"
}

wait_for_postgres

log_info "Creating pg_turret extension..."
docker exec -e PGPASSWORD=password pg_turret_db_test psql -U postgres -h localhost -p 5432 \
    -c "DROP EXTENSION IF EXISTS pg_turret CASCADE; CREATE EXTENSION pg_turret;"

log_info "Configuring Sentry DSN and enabling Sentry adapter..."
docker exec -e PGPASSWORD=password pg_turret_db_test psql -U postgres -h localhost -p 5432 <<EOF
ALTER SYSTEM SET pg_turret.sentry.enabled TO 'on';
ALTER SYSTEM SET pg_turret.sentry.dsn TO '${SENTRY_DSN}';
ALTER SYSTEM SET pg_turret.sentry.environment TO 'test';
ALTER SYSTEM SET pg_turret.sentry.min_level TO '20'; -- ERROR and above
ALTER SYSTEM SET pg_turret.sentry.sample_rate TO '100';
SELECT pg_reload_conf();
EOF

TESTS_RUN=0

run_test() {
    local name="$1"
    local sql="$2"

    log_test "Running: $name"
    docker exec -e PGPASSWORD=password pg_turret_db_test psql -U postgres -h localhost -p 5432 \
        -c "$sql" >/dev/null 2>&1 || true
    TESTS_RUN=$((TESTS_RUN + 1))
}

# Wait a moment for the background worker to settle after config reload
sleep 2

log_info "Running SQL tests to generate Sentry events..."

run_test "Division by zero (ERROR)" \
    "SELECT 1/0;"

run_test "Invalid SQL error" \
    "SELECT * FROM nonexistent_table_for_sentry_tests;"

run_test "Exception with detail" \
    "DO \$\$ BEGIN INSERT INTO nonexistent_table VALUES(1); EXCEPTION WHEN undefined_table THEN RAISE EXCEPTION 'sentry-test-exception-001'; END \$\$; "

# Give the background worker time to flush events to Sentry
log_info "Waiting for background worker to flush logs to Sentry..."
sleep 6

log_info "========================================"
log_info "Done. $TESTS_RUN SQL statements executed"
log_info "Sentry DSN used: $SENTRY_DSN"
log_info "Check your Sentry project for new events."
log_info "========================================"

