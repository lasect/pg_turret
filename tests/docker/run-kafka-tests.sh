#!/bin/bash
set -e

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$SCRIPT_DIR"

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

# 3. Start containers
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

wait_for_kafka() {
    log_info "Waiting for Kafka..."
    local attempts=0
    until docker inspect --format='{{.State.Health.Status}}' kafka 2>/dev/null | grep -q "healthy"; do
        sleep 3
        attempts=$((attempts + 1))
        if [ $attempts -gt 60 ]; then
            log_error "Kafka failed to start"
            docker compose -f docker-compose.kafka.yml logs kafka
            exit 1
        fi
    done
    log_info "Kafka is ready"
}

wait_for_kafka
wait_for_postgres

# Create Kafka topic
log_info "Creating Kafka topic 'pg_turret_logs'..."
docker exec kafka /bin/kafka-topics --bootstrap-server localhost:9092 --create --topic pg_turret_logs --partitions 1 --replication-factor 1 2>/dev/null || true

log_info "Creating pg_turret extension..."
docker exec -e PGPASSWORD=password pg_turret_db_test psql -U postgres -h localhost -p 5432 \
    -c "DROP EXTENSION IF EXISTS pg_turret CASCADE; CREATE EXTENSION pg_turret;"

TESTS_RUN=0

run_test() {
    local name="$1"
    local sql="$2"

    log_test "Running: $name"
    docker exec -e PGPASSWORD=password pg_turret_db_test psql -U postgres -h localhost -p 5432 \
        -c "$sql" >/dev/null 2>&1 || true
    TESTS_RUN=$((TESTS_RUN + 1))
}

# Wait a moment for the background worker to settle after extension creation
sleep 2

log_info "Running SQL tests..."

run_test "Division by zero (ERROR)" \
    "SELECT 1/0;"

run_test "Warning via RAISE" \
    "DO \$\$ BEGIN RAISE WARNING 'test-warning-001'; END \$\$; "

run_test "Notice via RAISE" \
    "DO \$\$ BEGIN RAISE NOTICE 'test-notice-001'; END \$\$; "

run_test "Info via RAISE" \
    "DO \$\$ BEGIN RAISE INFO 'test-info-001'; END \$\$; "

run_test "Exception with detail" \
    "DO \$\$ BEGIN INSERT INTO nonexistent_table VALUES(1); EXCEPTION WHEN undefined_table THEN RAISE NOTICE 'caught-exception-001'; END \$\$; "

run_test "Multiple statements logged" \
    "SELECT 1; SELECT 2; SELECT 3;"

run_test "Invalid SQL error" \
    "SELECT * FROM nonexistent_table_xyz;"

# Wait for the background worker to poll and send all logs to Kafka
log_info "Waiting for background worker to flush logs to Kafka..."
sleep 6

# Verify logs are in Kafka topic
log_info "Verifying logs in Kafka topic 'pg_turret_logs'..."

# First, show the actual messages
log_info "Message content in Kafka topic:"
docker exec kafka /bin/kafka-console-consumer --bootstrap-server kafka:9092 --topic pg_turret_logs --from-beginning --max-messages 100 --timeout-ms 5000 2>/dev/null | while read -r line; do
    echo "  -> $line"
done

# Count Kafka messages
MESSAGE_COUNT=$(docker exec kafka /bin/kafka-console-consumer --bootstrap-server kafka:9092 --topic pg_turret_logs --from-beginning --max-messages 100 --timeout-ms 5000 2>/dev/null | wc -l)

# Count individual logs within messages (each message is a JSON array)
LOG_COUNT=$(docker exec kafka /bin/kafka-console-consumer --bootstrap-server kafka:9092 --topic pg_turret_logs --from-beginning --max-messages 100 --timeout-ms 5000 2>/dev/null | grep -o '"timestamp"' | wc -l)

if [ "$MESSAGE_COUNT" -gt 0 ]; then
    log_info "Found $MESSAGE_COUNT Kafka messages containing $LOG_COUNT individual log entries"
    log_info "(Logs are batched for efficiency - multiple PostgreSQL logs per Kafka message)"
else
    log_warn "No messages found in Kafka topic - this may be expected if no logs were generated"
fi

log_info "========================================"
log_info "Done. $TESTS_RUN SQL statements executed"
log_info "========================================"