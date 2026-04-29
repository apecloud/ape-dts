#!/usr/bin/env bash

set -o pipefail

SCRIPT_DIR=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
DT_TESTS_DIR=$(cd -- "${SCRIPT_DIR}/.." && pwd)
PROJECT_ROOT=$(cd -- "${DT_TESTS_DIR}/.." && pwd)

detect_it_os() {
  case "$(uname -s)" in
    Darwin) echo "mac" ;;
    Linux) echo "linux" ;;
    *) echo "unsupported OS: $(uname -s)" >&2; exit 1 ;;
  esac
}

detect_it_arch() {
  case "$(uname -m)" in
    x86_64|amd64) echo "x86" ;;
    arm64|aarch64) echo "arm" ;;
    *) echo "unsupported architecture: $(uname -m)" >&2; exit 1 ;;
  esac
}

detect_it_host_ip() {
  local ip=""
  local default_iface=""

  case "${DT_IT_OS}" in
    mac)
      default_iface="$(route -n get default 2>/dev/null | awk '/interface: / { print $2; exit }')"
      [[ -n "${default_iface}" ]] || {
        echo "failed to detect default macOS network interface" >&2
        exit 1
      }
      ip="$(ipconfig getifaddr "${default_iface}" 2>/dev/null || true)"
      ;;
    linux)
      if command -v ip >/dev/null 2>&1; then
        ip="$(ip -4 route get 1.1.1.1 2>/dev/null | awk '{for (i = 1; i <= NF; i++) if ($i == "src") { print $(i + 1); exit }}')"
      fi
      if [[ -z "${ip}" ]]; then
        ip="$(hostname -I 2>/dev/null | awk '{ print $1 }')"
      fi
      ;;
    *)
      echo "unsupported OS: ${DT_IT_OS}" >&2
      exit 1
      ;;
  esac

  [[ -n "${ip}" ]] || {
    echo "failed to detect host IP for ${DT_IT_OS}/${DT_IT_ARCH}" >&2
    exit 1
  }

  echo "${ip}"
}

export DT_IT_OS="$(detect_it_os)"
export DT_IT_ARCH="$(detect_it_arch)"
export DT_IT_HOST_IP="$(detect_it_host_ip)"

export RUST_BACKTRACE="${RUST_BACKTRACE:-1}"
export RUST_LIB_BACKTRACE="${RUST_LIB_BACKTRACE:-1}"

DEFAULT_COMPOSE_FILE="${DT_TESTS_DIR}/docker-compose.integration.yml"
DEFAULT_ENV_FILE="${DT_TESTS_DIR}/tests/.env"
DEFAULT_WAIT_TIMEOUT_SECS=30
DEFAULT_LOG_TAIL=200
DEFAULT_LOG_BASE_DIR="${PROJECT_ROOT}/tmp/integration-logs"
DEFAULT_RUN_ID="$(date '+%Y%m%d-%H%M%S')-$$"

declare -a ALL_SUITES=(
  "mysql_to_clickhouse"
  # "mysql_to_doris"       # disabled: local/CI Doris suite is temporarily excluded
  # "mysql_to_foxlake"      # disabled: local runnable Foxlake service is not provisioned
  "mysql_to_kafka_to_mysql"
  "mysql_to_mysql"
  "mysql_to_mysql_5_7"
  "mysql_to_mysql_case_sensitive"
  "mysql_to_mysql_lua"
  "mysql_to_redis"
  # "mysql_to_starrocks"    # disabled: temporarily excluded from default local matrix
  "mysql_to_tidb"
  "pg_to_clickhouse"
  # "pg_to_doris"          # disabled: local/CI Doris suite is temporarily excluded
  "pg_to_kafka_to_pg"
  "pg_to_pg"
  "pg_to_pg_lua"
  # "pg_to_starrocks"       # disabled: temporarily excluded from default local matrix
  "mongo_to_mongo"
  "redis_to_redis_2_8"    # disabled: Redis 2.8 image replacement not finished
  "redis_to_redis_4_0"
  "redis_to_redis_5_0"
  "redis_to_redis_6_0"
  "redis_to_redis_6_2"
  "redis_to_redis_7_0"
  "redis_to_redis_8_0"
  "redis_to_redis_cross_version"
  "redis_to_redis_graph"
  "redis_to_redis_rebloom"
  # "redis_to_redis_redisearch" # disabled: local/CI Redisearch suite is temporarily excluded
  "redis_to_redis_rejson"
  "redis_to_redis_precheck"
  "no_services"
)

COMPOSE_FILE="${DEFAULT_COMPOSE_FILE}"
ENV_FILE="${DEFAULT_ENV_FILE}"
WAIT_TIMEOUT_SECS="${DEFAULT_WAIT_TIMEOUT_SECS}"
LOG_TAIL="${DEFAULT_LOG_TAIL}"
RUNNER="nextest"
RUN_LOG_DIR="${DEFAULT_LOG_BASE_DIR}/${DEFAULT_RUN_ID}"
CLEANUP_DOCKER=1
CLEANUP_DONE=0
LOGGING_READY=0
GLOBAL_LOG_FILE=""
RUNNER_LOG_FILE=""
TEST_LOG_FILE=""
CURRENT_SUITE=""

ACTION_UP=0
ACTION_WAIT=0
ACTION_TEST=0
ACTION_LOGS=0
ACTION_DOWN=0
ACTION_LIST=0
ACTION_LIST_JSON=0
USE_ALL_ACTIONS=0
KEEP_GOING=0
AUTO_LOGS_ON_FAILURE=0
SHOW_TEST_OUTPUT=0
DOWN_EACH_SUITE=0

declare -a REQUESTED_SUITES=()
declare -a EXTRA_TEST_ARGS=()
declare -a ARM_UNSUPPORTED_SUITES=(
  "mysql_to_mysql_5_7"
  "redis_to_redis_2_8"
)

print_usage() {
  cat <<'EOF'
Usage:
  ./scripts/run-integration-tests.sh [options] [-- extra test args]

Options:
  --suite <name>         Run a suite from the built-in matrix. Repeatable.
  --suite all            Run every enabled suite in the matrix (excluding commented-out suites).
  --list-suites          Print suite matrix metadata and exit.
  --list-suites-json     Print enabled suite names as a JSON array and exit.
  --up                   Start Docker services for each selected suite.
  --wait                 Wait for selected services via compose healthcheck, or running state if no healthcheck is defined.
  --test                 Run Rust integration tests for each selected suite.
  --logs                 Dump Docker logs for each selected suite.
  --down                 Stop all integration Docker services and exit.
  --all                  Equivalent to --up --wait --test.
  --runner <mode>        nextest only. Default: nextest.
  --env-file <path>      Env file passed to docker compose. Default: dt-tests/tests/.env.
  --compose-file <path>  Compose file to use. Default: dt-tests/docker-compose.integration.yml.
  --log-dir <path>       Directory for script output logs.
  --wait-timeout <secs>  Service wait timeout. Default: 30.
  --log-tail <lines>     Number of log lines for Docker logs. Default: 200.
  --keep-docker          Skip the final docker compose down cleanup.
  --down-each-suite     Stop all integration Docker services after each suite.
  --keep-going           Continue with later suites after a suite fails.
  --logs-on-failure      Dump Docker logs automatically when a test step fails.
  --show-test-output     Print stdout/stderr for successful tests too.
  --help                 Show this help.

Examples:
  # List available suites
  ./scripts/run-integration-tests.sh --list-suites

  # Run one suite end-to-end
  ./scripts/run-integration-tests.sh --suite mysql_to_mysql --all

  # Start containers and keep them running for later steps
  ./scripts/run-integration-tests.sh --suite mysql_to_mysql --up --wait --keep-docker

  # Check whether each suite's containers can start successfully
  ./scripts/run-integration-tests.sh --suite all --up --wait --down-each-suite --keep-going

  # Run all suites serially and stop each suite's containers before the next suite starts
  ./scripts/run-integration-tests.sh --suite all --all --down-each-suite --keep-going

  # Run tests against already-started containers
  ./scripts/run-integration-tests.sh --suite mysql_to_mysql --test --runner nextest --keep-docker

  # Show stdout/stderr for successful tests too
  ./scripts/run-integration-tests.sh --suite mysql_to_mysql --test --show-test-output

  # Run one exact test case
  ./scripts/run-integration-tests.sh --suite mysql_to_mysql --test -- --exact snapshot_tests::test::snapshot_basic_test

  # Dump docker logs for the current suite
  ./scripts/run-integration-tests.sh --suite mysql_to_mysql --logs --keep-docker

  # Stop all integration containers
  ./scripts/run-integration-tests.sh --suite mysql_to_mysql --down

  # Dump docker logs automatically when a test step fails
  ./scripts/run-integration-tests.sh --suite mysql_to_mysql --all --logs-on-failure

  # Write logs to a custom directory
  ./scripts/run-integration-tests.sh --suite mysql_to_mysql --all --log-dir /tmp/dt-it-logs

  # Run multiple suites and continue after failures
  ./scripts/run-integration-tests.sh --suite mysql_to_mysql --suite pg_to_pg --keep-going --all
EOF
}

die() {
  echo "Error: $*" >&2
  exit 1
}

log() {
  local message
  local log_file
  message="[integration-tests] $*"
  printf '%s\n' "${message}"
  log_file="$(active_log_file)"
  if [[ -n "${log_file}" ]]; then
    printf '%s\n' "${message}" >> "${log_file}"
  fi
}

suite_log_dir() {
  local suite="$1"
  echo "${RUN_LOG_DIR}/${suite}"
}

suite_runner_log_file() {
  local suite="$1"
  echo "${RUN_LOG_DIR}/${suite}.log"
}

suite_log_file() {
  local suite="$1"
  local file_name="$2"
  local dir
  dir="$(suite_log_dir "${suite}")"
  mkdir -p "${dir}"
  echo "${dir}/${file_name}"
}

active_log_file() {
  if [[ -n "${RUNNER_LOG_FILE}" ]]; then
    echo "${RUNNER_LOG_FILE}"
    return
  fi

  echo "${GLOBAL_LOG_FILE}"
}

active_test_log_file() {
  if [[ -n "${TEST_LOG_FILE}" ]]; then
    echo "${TEST_LOG_FILE}"
    return
  fi

  echo "$(active_log_file)"
}

resolve_runner() {
  if [[ "${RUNNER}" != "nextest" ]]; then
    die "unsupported runner '${RUNNER}', only 'nextest' is supported"
  fi

  cargo nextest --version >/dev/null 2>&1 || die "runner 'nextest' requested but cargo-nextest is not installed"
  echo "nextest"
}

add_suite() {
  local suite="$1"
  if [[ "${suite}" == "all" ]]; then
    REQUESTED_SUITES=("${ALL_SUITES[@]}")
    return
  fi

  if ! is_known_suite "${suite}"; then
    die "unknown suite '${suite}'"
  fi

  local existing
  for existing in "${REQUESTED_SUITES[@]}"; do
    if [[ "${existing}" == "${suite}" ]]; then
      return
    fi
  done
  REQUESTED_SUITES+=("${suite}")
}

is_known_suite() {
  local suite="$1"
  local existing
  for existing in "${ALL_SUITES[@]}"; do
    if [[ "${existing}" == "${suite}" ]]; then
      return 0
    fi
  done
  return 1
}

is_suite_supported_on_current_arch() {
  local suite="$1"
  local unsupported_suite

  if [[ "${DT_IT_ARCH}" != "arm" ]]; then
    return 0
  fi

  for unsupported_suite in "${ARM_UNSUPPORTED_SUITES[@]}"; do
    if [[ "${unsupported_suite}" == "${suite}" ]]; then
      return 1
    fi
  done

  return 0
}

suite_services() {
  local suite="$1"
  case "${suite}" in
    mysql_to_clickhouse) echo "mysql-src clickhouse" ;;
    mysql_to_doris) echo "mysql-src doris-2-1-0" ;;
    mysql_to_foxlake) echo "mysql-src minio minio-init" ;;
    mysql_to_kafka_to_mysql) echo "mysql-src mysql-dst kafka" ;;
    mysql_to_mysql) echo "mysql-src mysql-dst mysql-meta mysql-src-8-0 mysql-dst-8-0" ;;
    mysql_to_mysql_5_7) echo "mysql-src-5-7 mysql-dst-5-7 mysql-meta-5-7 mysql-src-8-0 mysql-dst-8-0" ;;
    mysql_to_mysql_case_sensitive) echo "mysql-src-8-0 mysql-dst-8-0" ;;
    mysql_to_mysql_lua) echo "mysql-src mysql-dst mysql-meta" ;;
    mysql_to_redis) echo "mysql-src redis-dst" ;;
    mysql_to_starrocks) echo "mysql-src starrocks-3-2-11 starrocks-2-5-4" ;;
    mysql_to_tidb) echo "mysql-src tidb" ;;
    pg_to_clickhouse) echo "postgres-src clickhouse" ;;
    pg_to_doris) echo "postgres-src doris-2-1-0" ;;
    pg_to_kafka_to_pg) echo "postgres-src postgres-dst kafka" ;;
    pg_to_pg) echo "postgres-src postgres-dst postgres-node3" ;;
    pg_to_pg_lua) echo "postgres-src postgres-dst" ;;
    pg_to_starrocks) echo "postgres-src starrocks-3-2-11" ;;
    mongo_to_mongo) echo "mongo-src mongo-dst" ;;
    redis_to_redis_2_8) echo "redis-2-8" ;;
    redis_to_redis_4_0) echo "redis-src-4-0 redis-dst-4-0" ;;
    redis_to_redis_5_0) echo "redis-src-5-0 redis-dst-5-0" ;;
    redis_to_redis_6_0) echo "redis-src-6-0 redis-dst-6-0" ;;
    redis_to_redis_6_2) echo "redis-src-6-2 redis-dst-6-2" ;;
    redis_to_redis_7_0) echo "redis-src redis-dst redis-cycle-node3 redis-cluster-node1 redis-cluster-node2 redis-cluster-node3 redis-cluster-init" ;;
    redis_to_redis_8_0) echo "redis-src-8-0 redis-dst-8-0" ;;
    redis_to_redis_cross_version) echo "redis-src-4-0 redis-src-5-0 redis-src-6-0 redis-src-6-2 redis-dst" ;;
    redis_to_redis_graph) echo "falkordb-src falkordb-dst" ;;
    redis_to_redis_rebloom) echo "redis-rebloom" ;;
    redis_to_redis_redisearch) echo "redis-redisearch" ;;
    redis_to_redis_rejson) echo "redis-rejson-src redis-rejson-dst" ;;
    redis_to_redis_precheck) echo "redis-src-8-0 redis-dst-8-0" ;;
    no_services) echo "" ;;
    *) die "unknown suite '${suite}'" ;;
  esac
}

service_wait_mode() {
  local _suite="$1"
  local service="$2"
  # Services whose names include "init" are treated as one-shot init containers
  # and are considered ready onl after they exit successfully.
  if service_is_init "${service}"; then
    echo "exit_0"
    return
  fi

  echo "default"
}

service_is_init() {
  local service="$1"
  [[ "${service}" == *init* ]]
}

resolve_service_container_id() {
  local service="$1"

  if service_is_init "${service}"; then
    # One-shot init containers may already be in exited state, and
    # `docker compose ps -q <service>` can return nothing for them.
    # Match by container name against `docker ps -a` so exited init
    # containers are still treated as created.
    docker ps -aq --filter "name=${service}" | head -n 1
    return
  fi

  compose_cmd ps -q "${service}"
}

suite_nextest_filter() {
  local suite="$1"
  case "${suite}" in
    mysql_to_clickhouse) echo "test(mysql_to_clickhouse::)" ;;
    mysql_to_doris) echo "test(mysql_to_doris::)" ;;
    mysql_to_foxlake) echo "test(mysql_to_foxlake::)" ;;
    mysql_to_kafka_to_mysql) echo "test(mysql_to_kafka_to_mysql::)" ;;
    mysql_to_mysql) echo "test(mysql_to_mysql::)" ;;
    mysql_to_mysql_5_7) echo "test(mysql_to_mysql::)" ;;
    mysql_to_mysql_case_sensitive) echo "test(mysql_to_mysql_case_sensitive::)" ;;
    mysql_to_mysql_lua) echo "test(mysql_to_mysql_lua::)" ;;
    mysql_to_redis) echo "test(mysql_to_redis::)" ;;
    mysql_to_starrocks) echo "test(mysql_to_starrocks::)" ;;
    mysql_to_tidb) echo "test(mysql_to_tidb::)" ;;
    pg_to_clickhouse) echo "test(pg_to_clickhouse::)" ;;
    pg_to_doris) echo "test(pg_to_doris::)" ;;
    pg_to_kafka_to_pg) echo "test(pg_to_kafka_to_pg::)" ;;
    pg_to_pg) echo "test(pg_to_pg::)" ;;
    pg_to_pg_lua) echo "test(pg_to_pg_lua::)" ;;
    pg_to_starrocks) echo "test(pg_to_starrocks::)" ;;
    mongo_to_mongo) echo "test(mongo_to_mongo::)" ;;
    redis_to_redis_2_8) echo "test(redis_to_redis::cdc_2_8_tests::) | test(redis_to_redis::snapshot_2_8_tests::)" ;;
    redis_to_redis_4_0) echo "test(redis_to_redis::cdc_4_0_tests::) | test(redis_to_redis::snapshot_4_0_tests::)" ;;
    redis_to_redis_5_0) echo "test(redis_to_redis::cdc_5_0_tests::) | test(redis_to_redis::snapshot_5_0_tests::)" ;;
    redis_to_redis_6_0) echo "test(redis_to_redis::cdc_6_0_tests::) | test(redis_to_redis::snapshot_6_0_tests::)" ;;
    redis_to_redis_6_2) echo "test(redis_to_redis::cdc_6_2_tests::) | test(redis_to_redis::snapshot_6_2_tests::)" ;;
    redis_to_redis_7_0) echo "test(redis_to_redis::cdc_7_0_tests::) | test(redis_to_redis::snapshot_7_0_tests::) | test(redis_to_redis::snapshot_and_cdc_7_0_tests::)" ;;
    redis_to_redis_8_0) echo "test(redis_to_redis::cdc_8_0_tests::) | test(redis_to_redis::snapshot_8_0_tests::)" ;;
    redis_to_redis_cross_version) echo "test(redis_to_redis::cdc_cross_version_tests::) | test(redis_to_redis::snapshot_cross_version_tests::)" ;;
    redis_to_redis_graph) echo "test(redis_to_redis::cdc_graph_tests::) | test(redis_to_redis::snapshot_graph_tests::)" ;;
    redis_to_redis_rebloom) echo "test(redis_to_redis::cdc_rebloom_tests::) | test(redis_to_redis::snapshot_rebloom_tests::)" ;;
    redis_to_redis_redisearch) echo "test(redis_to_redis::cdc_redisearch_tests::) | test(redis_to_redis::snapshot_redisearch_tests::)" ;;
    redis_to_redis_rejson) echo "test(redis_to_redis::cdc_rejson_tests::) | test(redis_to_redis::snapshot_rejson_tests::)" ;;
    redis_to_redis_precheck) echo "test(redis_to_redis::precheck_tests::)" ;;
    no_services) echo "test(log_reader::)" ;;
    *) die "unknown suite '${suite}'" ;;
  esac
}

compose_cmd() {
  (
    cd "${DT_TESTS_DIR}" &&
      docker compose --env-file "${ENV_FILE}" -f "${COMPOSE_FILE}" "$@"
  )
}

split_services() {
  local suite="$1"
  local services
  services="$(suite_services "${suite}")"
  if [[ -z "${services}" ]]; then
    return
  fi

  # shellcheck disable=SC2206
  local items=( ${services} )
  printf '%s\n' "${items[@]}"
}

collect_cleanup_services() {
  local target_suites=("$@")
  local seen=" "
  local suite

  for suite in "${target_suites[@]}"; do
    [[ -n "${suite}" ]] || continue

    local service
    while IFS= read -r service; do
      [[ -n "${service}" ]] || continue
      if [[ "${seen}" == *" ${service} "* ]]; then
        continue
      fi

      printf '%s\n' "${service}"
      seen="${seen}${service} "
    done < <(split_services "${suite}")
  done
}

list_suites() {
  local suite
  for suite in "${ALL_SUITES[@]}"; do
    printf '%s\n' "${suite}"
    printf '  services: %s\n' "$(suite_services "${suite}")"
    printf '  nextest filter: %s\n' "$(suite_nextest_filter "${suite}")"
  done
}

list_suites_json() {
  local suite
  printf '['
  for i in "${!ALL_SUITES[@]}"; do
    suite="${ALL_SUITES[$i]}"
    if (( i > 0 )); then
      printf ','
    fi
    printf '"%s"' "${suite}"
  done
  printf ']\n'
}

ensure_files_exist() {
  [[ -f "${COMPOSE_FILE}" ]] || die "compose file not found: ${COMPOSE_FILE}"
  [[ -f "${ENV_FILE}" ]] || die "env file not found: ${ENV_FILE}"
}

setup_logging() {
  if (( LOGGING_READY != 0 )); then
    return
  fi

  mkdir -p "${RUN_LOG_DIR}"
  GLOBAL_LOG_FILE="${RUN_LOG_DIR}/run.log"
  : > "${GLOBAL_LOG_FILE}"
  LOGGING_READY=1
  log "integration script logs: ${RUN_LOG_DIR}"
}

run_with_runner_log() {
  local log_file
  log_file="$(active_log_file)"

  if [[ -z "${log_file}" ]]; then
    "$@"
    return $?
  fi

  {
    "$@"
  } 2>&1 | tee -a "${log_file}"
  return "${PIPESTATUS[0]}"
}

run_with_test_log() {
  local log_file
  log_file="$(active_test_log_file)"

  if [[ -z "${log_file}" ]]; then
    "$@"
    return $?
  fi

  {
    "$@"
  } 2>&1 | tee -a "${log_file}"
  return "${PIPESTATUS[0]}"
}

dump_logs() {
  local suite="$1"
  local services
  local log_file
  services="$(suite_services "${suite}")"
  if [[ -z "${services}" ]]; then
    log "suite '${suite}' has no external services to log"
    return 0
  fi

  log "dumping logs for '${suite}'"
  log_file="$(active_log_file)"
  if [[ -n "${log_file}" ]]; then
    compose_cmd logs --tail="${LOG_TAIL}" \
      2>&1 | tee -a "$(suite_log_file "${suite}" "docker.log")" | tee -a "${log_file}"
    return "${PIPESTATUS[0]}"
  fi

  compose_cmd logs --tail="${LOG_TAIL}" | tee -a "$(suite_log_file "${suite}" "docker.log")"
  return "${PIPESTATUS[0]}"
}

cleanup_selected_services() {
  local context="$1"
  shift

  local -a target_suites=("$@")
  local -a services=()
  local service
  while IFS= read -r service; do
    [[ -n "${service}" ]] || continue
    services+=("${service}")
  done < <(collect_cleanup_services "${target_suites[@]}")

  if ((${#services[@]} == 0)); then
    return 0
  fi

  log "stopping selected integration docker services${context}: ${services[*]}"
  run_with_runner_log compose_cmd stop "${services[@]}" || true
  run_with_runner_log compose_cmd rm -f -s -v "${services[@]}" || true
}

cleanup_all_services() {
  local force="${1:-0}"
  if (( CLEANUP_DONE != 0 )); then
    return 0
  fi
  if (( force == 0 && CLEANUP_DOCKER == 0 )); then
    return 0
  fi
  if [[ ! -f "${COMPOSE_FILE}" ]]; then
    return 0
  fi

  CLEANUP_DONE=1
  log "stopping all integration docker services"
  cleanup_selected_services "" "${REQUESTED_SUITES[@]}"
  run_with_runner_log compose_cmd down -v --remove-orphans || true
}

cleanup_after_suite() {
  if (( DOWN_EACH_SUITE == 0 || CLEANUP_DOCKER == 0 )); then
    return 0
  fi
  if [[ ! -f "${COMPOSE_FILE}" ]]; then
    return 0
  fi

  log "stopping all integration docker services after suite"
  cleanup_selected_services " after suite" "${CURRENT_SUITE}"
  run_with_runner_log compose_cmd down -v --remove-orphans || true
}

on_exit() {
  cleanup_all_services 0
}

on_signal() {
  local signal_name="$1"
  local exit_code="$2"

  log "received ${signal_name}"
  if [[ -n "${CURRENT_SUITE}" ]]; then
    log "dumping docker logs for interrupted suite '${CURRENT_SUITE}'"
    dump_logs "${CURRENT_SUITE}" || true
  fi
  log "cleaning up integration docker services"
  cleanup_all_services 0
  trap - EXIT
  exit "${exit_code}"
}

start_services() {
  local suite="$1"
  local services
  services="$(suite_services "${suite}")"
  if [[ -z "${services}" ]]; then
    log "suite '${suite}' has no external services to start"
    return 0
  fi

  local args=(up -d --quiet-pull)
  local service
  while IFS= read -r service; do
    [[ -n "${service}" ]] || continue
    args+=("${service}")
  done < <(split_services "${suite}")

  log "starting services for '${suite}': ${services}"
  run_with_runner_log compose_cmd "${args[@]}"
}

wait_for_services() {
  local suite="$1"
  local services
  services="$(suite_services "${suite}")"
  if [[ -z "${services}" ]]; then
    log "suite '${suite}' has no external services to wait for"
    return 0
  fi

  local deadline=$((SECONDS + WAIT_TIMEOUT_SECS))
  local service
  while IFS= read -r service; do
    [[ -n "${service}" ]] || continue

    local wait_mode
    wait_mode="$(service_wait_mode "${suite}" "${service}")"

    local cid
    cid="$(resolve_service_container_id "${service}")"
    if [[ -z "${cid}" ]]; then
      echo "Service ${service} was not created" >&2
      return 1
    fi

    # Readiness is determined only by docker compose container state:
    # prefer explicit container healthchecks, otherwise fall back to running/exited.
    log "waiting for ${service}"
    while true; do
      local status health exit_code
      status="$(docker inspect -f '{{.State.Status}}' "${cid}")"
      health="$(docker inspect -f '{{if .State.Health}}{{.State.Health.Status}}{{end}}' "${cid}")"
      exit_code="$(docker inspect -f '{{.State.ExitCode}}' "${cid}")"

      if [[ "${status}" == "exited" ]]; then
        if [[ "${exit_code}" == "0" ]]; then
          log "${service} exited successfully"
          break
        fi
        echo "${service} exited with code ${exit_code}" >&2
        local log_file
        log_file="$(active_log_file)"
        if [[ -n "${log_file}" ]]; then
          compose_cmd logs --tail="${LOG_TAIL}" "${service}" \
            2>&1 | tee -a "$(suite_log_file "${suite}" "docker-${service}.log")" | tee -a "${log_file}"
        else
          compose_cmd logs --tail="${LOG_TAIL}" "${service}" | tee -a "$(suite_log_file "${suite}" "docker-${service}.log")"
        fi
        return 1
      fi

      if [[ -n "${health}" ]]; then
        if [[ "${health}" == "healthy" ]]; then
          log "${service} is healthy"
          break
        fi
      elif [[ "${wait_mode}" == "exit_0" ]]; then
        :
      elif [[ "${status}" == "running" ]]; then
        log "${service} is running"
        break
      fi

      if (( SECONDS >= deadline )); then
        echo "Timed out waiting for ${service}" >&2
        local log_file
        log_file="$(active_log_file)"
        if [[ -n "${log_file}" ]]; then
          compose_cmd logs --tail="${LOG_TAIL}" "${service}" \
            2>&1 | tee -a "$(suite_log_file "${suite}" "docker-${service}.log")" | tee -a "${log_file}"
        else
          compose_cmd logs --tail="${LOG_TAIL}" "${service}" | tee -a "$(suite_log_file "${suite}" "docker-${service}.log")"
        fi
        return 1
      fi

      sleep 3
    done
  done < <(split_services "${suite}")

  log "all services ready for '${suite}'"
}

run_nextest_suite() {
  local suite="$1"
  cd "${PROJECT_ROOT}" || return 1
  local output_args=(
    --failure-output immediate
    --success-output never
  )
  if (( SHOW_TEST_OUTPUT != 0 )); then
    output_args=(
      --failure-output immediate-final
      --success-output immediate-final
    )
  fi

  cargo nextest run \
    --package dt-tests \
    --test integration_test \
    --no-fail-fast \
    --test-threads 1 \
    "${output_args[@]}" \
    -E "$(suite_nextest_filter "${suite}")" \
    "${EXTRA_TEST_ARGS[@]}"
}

run_tests() {
  local suite="$1"
  resolve_runner >/dev/null

  log "running tests for '${suite}' with nextest"
  TEST_LOG_FILE="$(suite_log_file "${suite}" "tests.log")"
  : > "${TEST_LOG_FILE}"
  run_with_test_log run_nextest_suite "${suite}"
}

parse_args() {
  while (($# > 0)); do
    case "$1" in
      --suite)
        (($# >= 2)) || die "--suite requires a value"
        add_suite "$2"
        shift 2
        ;;
      --list-suites)
        ACTION_LIST=1
        shift
        ;;
      --list-suites-json)
        ACTION_LIST_JSON=1
        shift
        ;;
      --up)
        ACTION_UP=1
        shift
        ;;
      --wait)
        ACTION_WAIT=1
        shift
        ;;
      --test)
        ACTION_TEST=1
        shift
        ;;
      --logs)
        ACTION_LOGS=1
        shift
        ;;
      --down)
        ACTION_DOWN=1
        shift
        ;;
      --all)
        USE_ALL_ACTIONS=1
        shift
        ;;
      --runner)
        (($# >= 2)) || die "--runner requires a value"
        RUNNER="$2"
        shift 2
        ;;
      --env-file)
        (($# >= 2)) || die "--env-file requires a value"
        ENV_FILE="$2"
        shift 2
        ;;
      --compose-file)
        (($# >= 2)) || die "--compose-file requires a value"
        COMPOSE_FILE="$2"
        shift 2
        ;;
      --log-dir)
        (($# >= 2)) || die "--log-dir requires a value"
        RUN_LOG_DIR="$2"
        shift 2
        ;;
      --wait-timeout)
        (($# >= 2)) || die "--wait-timeout requires a value"
        WAIT_TIMEOUT_SECS="$2"
        shift 2
        ;;
      --log-tail)
        (($# >= 2)) || die "--log-tail requires a value"
        LOG_TAIL="$2"
        shift 2
        ;;
      --keep-docker)
        CLEANUP_DOCKER=0
        shift
        ;;
      --down-each-suite)
        DOWN_EACH_SUITE=1
        shift
        ;;
      --keep-going)
        KEEP_GOING=1
        shift
        ;;
      --logs-on-failure)
        AUTO_LOGS_ON_FAILURE=1
        shift
        ;;
      --show-test-output)
        SHOW_TEST_OUTPUT=1
        shift
        ;;
      --help|-h)
        print_usage
        exit 0
        ;;
      --)
        shift
        EXTRA_TEST_ARGS=("$@")
        return
        ;;
      *)
        die "unknown argument '$1'"
        ;;
    esac
  done
}

main() {
  parse_args "$@"

  if (( ACTION_LIST )); then
    list_suites
    exit 0
  fi

  if (( ACTION_LIST_JSON )); then
    list_suites_json
    exit 0
  fi

  trap on_exit EXIT
  trap 'on_signal SIGINT 130' INT
  trap 'on_signal SIGTERM 143' TERM
  trap 'on_signal SIGHUP 129' HUP
  trap 'on_signal SIGQUIT 131' QUIT

  if ((${#REQUESTED_SUITES[@]} == 0)); then
    REQUESTED_SUITES=("${ALL_SUITES[@]}")
  fi

  if (( USE_ALL_ACTIONS )); then
    ACTION_UP=1
    ACTION_WAIT=1
    ACTION_TEST=1
    AUTO_LOGS_ON_FAILURE=1
  fi

  if (( ACTION_UP == 0 && ACTION_WAIT == 0 && ACTION_TEST == 0 && ACTION_LOGS == 0 && ACTION_DOWN == 0 )); then
    ACTION_UP=1
    ACTION_WAIT=1
    ACTION_TEST=1
    AUTO_LOGS_ON_FAILURE=1
  fi

  ensure_files_exist
  setup_logging

  if (( ACTION_DOWN != 0 && ACTION_UP == 0 && ACTION_WAIT == 0 && ACTION_TEST == 0 && ACTION_LOGS == 0 )); then
    cleanup_all_services 1
    exit 0
  fi

  local suite
  local overall_status=0
  for suite in "${REQUESTED_SUITES[@]}"; do
    CURRENT_SUITE="${suite}"
    RUNNER_LOG_FILE="$(suite_runner_log_file "${suite}")"
    TEST_LOG_FILE=""
    : > "${RUNNER_LOG_FILE}"
    log "suite '${suite}' begin"

    if ! is_suite_supported_on_current_arch "${suite}"; then
      log "suite '${suite}' skipped: unsupported on ${DT_IT_ARCH} architecture"
      continue
    fi

    local suite_status=0
    if (( ACTION_UP )); then
      start_services "${suite}" || suite_status=$?
    fi

    if (( suite_status == 0 && ACTION_WAIT )); then
      wait_for_services "${suite}" || suite_status=$?
    fi

    if (( suite_status == 0 && ACTION_TEST )); then
      run_tests "${suite}" || suite_status=$?
      if (( suite_status != 0 && ACTION_LOGS == 0 && AUTO_LOGS_ON_FAILURE != 0 )); then
        dump_logs "${suite}" || true
      fi
    fi

    if (( ACTION_LOGS )); then
      dump_logs "${suite}" || suite_status=$?
    fi

    if (( suite_status == 0 )); then
      log "suite '${suite}' completed"
    else
      overall_status=${suite_status}
      log "suite '${suite}' failed"
    fi

    cleanup_after_suite

    if (( suite_status != 0 && KEEP_GOING == 0 )); then
      break
    fi
  done

  CURRENT_SUITE=""
  RUNNER_LOG_FILE=""
  TEST_LOG_FILE=""
  if (( ACTION_DOWN != 0 )); then
    cleanup_all_services 1
  fi

  exit "${overall_status}"
}

main "$@"
