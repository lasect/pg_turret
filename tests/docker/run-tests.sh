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
    docker compose down --volumes --remove-orphans 2>/dev/null || true
    rm -rf dist/
}

trap cleanup EXIT

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

log_info "Building Docker image..."
docker build -t pg_turret_db -f Dockerfile . --no-cache

log_info "Starting containers..."
docker compose up -d

wait_for_postgres() {
    log_info "Waiting for Postgres..."
    local attempts=0
    until docker compose exec -T db pg_isready -U postgres 2>/dev/null; do
        sleep 2
        attempts=$((attempts + 1))
        if [ $attempts -gt 30 ]; then
            log_error "Postgres failed to start"
            docker compose logs db
            exit 1
        fi
    done
    log_info "Postgres is ready"
}

wait_for_postgres

log_info "Creating pg_turret extension..."
docker compose exec -T db psql -U postgres -c "DROP EXTENSION IF EXISTS pg_turret CASCADE; CREATE EXTENSION pg_turret;"

TESTS_PASSED=0
TESTS_FAILED=0

# Track the number of mock-server log lines seen so far.
# Each test only checks lines AFTER this offset.
MOCK_LOG_LINES_SEEN=0

# Snapshot the current mock-server log line count (call before each test).
snapshot_mock_logs() {
    MOCK_LOG_LINES_SEEN=$(docker compose logs mock-server 2>&1 | wc -l)
}

# Get only NEW mock-server log lines since the last snapshot.
get_new_mock_logs() {
    docker compose logs mock-server 2>&1 | tail -n +$((MOCK_LOG_LINES_SEEN + 1))
}

run_test() {
    local name="$1"
    local sql="$2"
    local expected_pattern="$3"

    log_test "Running: $name"

    # Snapshot log position BEFORE executing the SQL
    snapshot_mock_logs

    docker compose exec -T db psql -U postgres -c "$sql" 2>/dev/null || true
    
    # Wait for the background worker to poll and send logs to mock-server.
    # poll_interval_s=1, so 4s gives enough time for at least one cycle.
    sleep 4
    
    local new_logs=$(get_new_mock_logs)
    
    if echo "$new_logs" | grep -q "$expected_pattern"; then
        log_info "  PASSED: $name"
        TESTS_PASSED=$((TESTS_PASSED + 1))
    else
        log_error "  FAILED: $name (expected: $expected_pattern)"
        echo "  New logs received since test start:"
        echo "$new_logs" | head -20 | sed 's/^/    /'
        TESTS_FAILED=$((TESTS_FAILED + 1))
    fi
}

log_info "Running SQL tests..."

run_test "Division by zero (ERROR)" \
    "SELECT 1/0;" \
    "division by zero"

run_test "Warning via RAISE" \
    "DO \$\$ BEGIN RAISE WARNING 'test-warning-001'; END \$\$; " \
    "test-warning-001"

run_test "Notice via RAISE" \
    "DO \$\$ BEGIN RAISE NOTICE 'test-notice-001'; END \$\$; " \
    "test-notice-001"

run_test "Info via RAISE" \
    "DO \$\$ BEGIN RAISE INFO 'test-info-001'; END \$\$; " \
    "test-info-001"

run_test "Exception with detail" \
    "DO \$\$ BEGIN INSERT INTO nonexistent_table VALUES(1); EXCEPTION WHEN undefined_table THEN RAISE NOTICE 'caught-exception-001'; END \$\$; " \
    "caught-exception-001"

run_test "Multiple statements logged" \
    "SELECT 1; SELECT 2; SELECT 3;" \
    "LOG"

run_test "Invalid SQL error" \
    "SELECT * FROM nonexistent_table_xyz;" \
    "relation.*does not exist"

log_info "========================================"
log_info "Test Results: $TESTS_PASSED passed, $TESTS_FAILED failed"
log_info "========================================"

if [ $TESTS_FAILED -gt 0 ]; then
    log_error "Some tests failed!"
    exit 1
else
    log_info "All tests passed!"
    exit 0
fi
