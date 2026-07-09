#!/bin/bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
DT_TESTS_DIR="$(dirname "$SCRIPT_DIR")"
PROJECT_ROOT="$(dirname "$DT_TESTS_DIR")"

COMPOSE_FILE="$DT_TESTS_DIR/docker-compose.zk.yml"
EVIDENCE_DIR="$DT_TESTS_DIR/evidence/zk_to_zk"
TASK_DIR="$DT_TESTS_DIR/tests/zk_to_zk/cdc/basic_test"

ZK1_HOST="localhost:2181"
ZK2_HOST="localhost:2182"

mkdir -p "$EVIDENCE_DIR"

log() { echo "[$(date '+%H:%M:%S')] $*"; }

cleanup() {
    log "Stopping containers..."
    docker-compose -f "$COMPOSE_FILE" down -v 2>/dev/null || true
}

wait_for_zk() {
    local host=$1 name=$2
    log "Waiting for $name ($host)..."
    for i in $(seq 1 30); do
        if echo ruok | nc -w 2 "${host%%:*}" "${host##*:}" 2>/dev/null | grep -q imok; then
            log "$name ready"
            return 0
        fi
        sleep 1
    done
    log "ERROR: $name not ready after 30s"
    return 1
}

phase_1_start_zk() {
    log "=== Phase 1: Start dual ZK ensembles ==="
    docker-compose -f "$COMPOSE_FILE" up -d
    wait_for_zk "$ZK1_HOST" "zk-node1"
    wait_for_zk "$ZK2_HOST" "zk-node2"
    log "Phase 1 PASS"
}

phase_2_seed_data() {
    log "=== Phase 2: Seed test data on zk-node1 ==="
    docker exec zk-node1 /apache-zookeeper-*/bin/zkCli.sh -server localhost:2181 <<'ZK_CMDS'
create /app ""
create /app/service-a "host1:8080"
create /app/service-b "host2:9090"
create /app/service-a/config "timeout=30"
quit
ZK_CMDS
    log "Seeded: /app/service-a, /app/service-b, /app/service-a/config"
    log "Phase 2 PASS"
}

phase_3_run_sync() {
    log "=== Phase 3: Run ape-dts sync (node1 → node2) ==="
    log "NOTE: Build and run manually:"
    log "  cargo build --release -p dt-main"
    log "  ./target/release/dt-main --task_conf $TASK_DIR/topo1_node1_to_node2/task_config.ini"
    log ""
    log "After sync runs for ~10 seconds, Ctrl+C and proceed to phase 4."
}

phase_4_verify() {
    log "=== Phase 4: Verify sync results on zk-node2 ==="
    local result_file="$EVIDENCE_DIR/verify_$(date +%Y%m%d_%H%M%S).log"

    {
        echo "=== zk-node2 data after sync ==="
        docker exec zk-node2 /apache-zookeeper-*/bin/zkCli.sh -server localhost:2181 <<'ZK_CMDS'
ls /app
get /app/service-a
get /app/service-b
get /app/service-a/config
quit
ZK_CMDS
    } 2>&1 | tee "$result_file"

    log "Evidence saved to: $result_file"

    if grep -q "host1:8080" "$result_file" && grep -q "host2:9090" "$result_file"; then
        log "Phase 4 PASS — data synced correctly"
    else
        log "Phase 4 FAIL — data not found on zk-node2"
    fi
}

phase_5_verify_bidirectional() {
    log "=== Phase 5: Verify bidirectional sync ==="
    log "1. Write new data on zk-node2:"
    log "   docker exec zk-node2 zkCli.sh create /app/service-c 'host3:7070'"
    log "2. Run reverse sync task:"
    log "   ./target/release/dt-main --task_conf $TASK_DIR/topo1_node2_to_node1/task_config.ini"
    log "3. Verify on zk-node1:"
    log "   docker exec zk-node1 zkCli.sh get /app/service-c"
}

case "${1:-all}" in
    start)    phase_1_start_zk ;;
    seed)     phase_2_seed_data ;;
    sync)     phase_3_run_sync ;;
    verify)   phase_4_verify ;;
    bidir)    phase_5_verify_bidirectional ;;
    cleanup)  cleanup ;;
    all)
        trap cleanup EXIT
        phase_1_start_zk
        phase_2_seed_data
        phase_3_run_sync
        phase_4_verify
        phase_5_verify_bidirectional
        ;;
    *)
        echo "Usage: $0 {start|seed|sync|verify|bidir|cleanup|all}"
        exit 1
        ;;
esac
