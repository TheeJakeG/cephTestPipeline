#! /usr/bin/env bash
set -eo pipefail

source "$CEPH_ROOT/qa/standalone/ceph-helpers.sh"

function run() {
  local dir=$1
  shift

  export CEPH_MON="127.0.0.1:7149"
  export CEPH_ARGS=""
  CEPH_ARGS+=" --fsid=$(uuidgen)"
  CEPH_ARGS+=" --auth-supported=none"
  CEPH_ARGS+=" --mon-host=$CEPH_MON"
  CEPH_ARGS+=" --osd-op-queue=wpq"

  local funcs=${@:-$(set | sed -n -e 's/^\(TEST_[0-9a-z_]*\) .*/\1/p')}

  for func in $funcs; do
    echo "===== RUNNING $func ====="
    $func "$dir"
  done
}

function bootstrap_cluster() {
  local dir=$1
  local osds=${2:-4}

  setup "$dir"
  run_mon "$dir" a --osd_pool_default_size=1 --mon_allow_pool_size_one=true
  run_mgr "$dir" x

  for osd in $(seq 0 $((osds - 1))); do
    run_osd "$dir" "$osd" \
      --osd_scrub_during_recovery=true \
      --osd-scrub-interval-randomize-ratio=0 \
      --osd_scrub_backoff_ratio=0
  done

  sleep 3
}

function cleanup_cluster() {
  local dir=$1
  teardown "$dir"
}

function wait_for_pool_pg_clean() {
  local pool=$1
  local expected=${2:-2}
  local tries=${3:-60}
  local i=0

  while true; do
    local json
    json=$(ceph pg ls-by-pool "$pool" -f json 2>/dev/null || true)

    local total
    total=$(echo "$json" | jq 'length')

    local clean
    clean=$(echo "$json" | jq '[.[] | select(.state == "active+clean")] | length')

    if [ "$total" -ge "$expected" ] && [ "$clean" -eq "$expected" ]; then
      return 0
    fi

    i=$((i + 1))
    if [ "$i" -ge "$tries" ]; then
      echo "Timed out waiting for all PGs in pool $pool to become active+clean"
      ceph pg ls-by-pool "$pool" || true
      ceph -s || true
      return 1
    fi

    sleep 1
  done
}

function wait_for_health_recoverable() {
  local tries=${1:-40}
  local i=0
  while true; do
    local health
    health=$(ceph health 2>/dev/null || true)
    case "$health" in
      HEALTH_OK*|HEALTH_WARN*)
        return 0
        ;;
    esac
    i=$((i + 1))
    if [ "$i" -ge "$tries" ]; then
      echo "Cluster never reached recoverable health"
      ceph -s || true
      return 1
    fi
    sleep 1
  done
}

function wait_for_clean_fast() {
  local tries=${1:-50}
  local i=0
  while true; do
    local stat
    stat=$(ceph pg stat 2>/dev/null || true)
    if echo "$stat" | grep -Eq 'active\+clean'; then
      return 0
    fi
    i=$((i + 1))
    if [ "$i" -ge "$tries" ]; then
      echo "Timed out waiting for active+clean"
      ceph -s || true
      ceph pg stat || true
      return 1
    fi
    sleep 1
  done
}

function create_replicated_pool() {
  local name=$1
  local pgnum=${2:-1}
  local size=${3:-2}
  create_pool "$name" "$pgnum" "$pgnum"
  ceph osd pool set "$name" size "$size"
  sleep 2
}

function create_ec_pool() {
  local profile=${1:-ec21}
  local pool=${2:-ecpool}
  local pgnum=${3:-1}
  ceph osd erasure-code-profile set "$profile" k=2 m=1 crush-failure-domain=osd
  ceph osd pool create "$pool" "$pgnum" "$pgnum" erasure "$profile"
  sleep 2
}

function pool_pgids() {
  local pool=$1
  ceph pg ls-by-pool "$pool" | awk 'NR>1 && $1 ~ /^[0-9]+\.[0-9]+$/ {print $1}'
}

function require_first_pgid() {
  local pool=$1
  local pgid
  pgid=$(ceph pg ls-by-pool "$pool" | awk 'NR>1 && $1 ~ /^[0-9]+\.[0-9]+$/ {print $1; exit}')

  if [ -z "$pgid" ]; then
    echo "ERROR: could not determine PG ID for pool $pool"
    ceph osd pool ls || true
    ceph pg ls || true
    exit 1
  fi

  echo "$pgid"
}

function last_scrub_stamp() {
  local pgid=$1
  ceph pg "$pgid" query -f json | jq -r '.info.history.last_scrub_stamp // empty'
}

function last_deep_scrub_stamp() {
  local pgid=$1
  ceph pg "$pgid" query -f json | jq -r '.info.history.last_deep_scrub_stamp // empty'
}

function wait_for_stamp_change() {
  local pgid=$1
  local before=$2
  local kind=${3:-scrub}
  local tries=${4:-30}
  local i=0

  while true; do
    local now
    if [ "$kind" = "deep-scrub" ]; then
      now=$(last_deep_scrub_stamp "$pgid")
    else
      now=$(last_scrub_stamp "$pgid")
    fi

    if [ -n "$now" ] && [ "$now" != "$before" ]; then
      return 0
    fi

    i=$((i + 1))
    if [ "$i" -ge "$tries" ]; then
      echo "Timed out waiting for $kind on $pgid"
      ceph pg "$pgid" query || true
      return 1
    fi
    sleep 1
  done
}

function trigger_scrub_and_wait() {
  local pgid=$1
  local before
  before=$(last_scrub_stamp "$pgid")
  ceph pg scrub "$pgid"
  wait_for_stamp_change "$pgid" "$before" scrub
}

function trigger_deep_scrub_and_wait() {
  local pgid=$1
  local before
  before=$(last_deep_scrub_stamp "$pgid")
  ceph pg deep-scrub "$pgid"
  wait_for_stamp_change "$pgid" "$before" deep-scrub
}

function trigger_repair_and_wait() {
  local pgid=$1
  ceph pg repair "$pgid"
  sleep 2
  wait_for_health_recoverable
}

function populate_pool() {
  local pool=$1
  local count=${2:-8}
  local tmpfile="/tmp/${pool}_payload.txt"

  echo "ceph-test-payload" > "$tmpfile"
  for i in $(seq 1 "$count"); do
    rados -p "$pool" put "obj_$i" "$tmpfile"
  done
  rm -f "$tmpfile"
}

function start_background_io() {
  local pool=$1
  local seconds=${2:-6}
  local fanout=${3:-1}
  local stamp=$$

  for worker in $(seq 1 "$fanout"); do
    (
      local end=$((SECONDS + seconds))
      local i=0
      while [ "$SECONDS" -lt "$end" ]; do
        local base="/tmp/${pool}_${stamp}_${worker}_${i}"
        echo "payload-$stamp-$worker-$i" > "${base}.txt"
        rados -p "$pool" put "bg_${stamp}_${worker}_${i}" "${base}.txt"
        rados -p "$pool" get "bg_${stamp}_${worker}_${i}" "${base}.out"
        rm -f "${base}.txt" "${base}.out"
        i=$((i + 1))
      done
    ) &
  done

  jobs -p
}

function wait_for_pids() {
  local p
  for p in "$@"; do
    wait "$p"
  done
}

function stop_osd() {
  local dir=$1
  local osd_id=$2
  kill_daemons "$dir" TERM "osd.$osd_id"
  ceph osd down "osd.$osd_id" || true
  sleep 2
}

function restart_osd() {
  local dir=$1
  local osd_id=$2
  activate_osd "$dir" "$osd_id"
  sleep 3
}

function assert_cluster_ok() {
  ceph -s || true
  wait_for_health_recoverable
}

function TEST_normal_scrub() {
  local dir=$1
  bootstrap_cluster "$dir" 4
  create_replicated_pool replA 1 2
  populate_pool replA 6
  wait_for_clean_fast
  local pgid
  pgid=$(require_first_pgid replA)
  trigger_scrub_and_wait "$pgid"
  assert_cluster_ok
  cleanup_cluster "$dir"
}

function TEST_deep_scrub_operation() {
  local dir=$1
  bootstrap_cluster "$dir" 4
  create_replicated_pool replA 1 2
  populate_pool replA 8
  wait_for_clean_fast
  local pgid
  pgid=$(require_first_pgid replA)
  trigger_deep_scrub_and_wait "$pgid"
  assert_cluster_ok
  cleanup_cluster "$dir"
}

function TEST_replica_repair() {
  local dir=$1
  bootstrap_cluster "$dir" 4
  create_replicated_pool replA 1 2
  populate_pool replA 8
  wait_for_clean_fast
  local pgid
  pgid=$(require_first_pgid replA)
  trigger_deep_scrub_and_wait "$pgid"
  trigger_repair_and_wait "$pgid"
  trigger_scrub_and_wait "$pgid"
  assert_cluster_ok
  cleanup_cluster "$dir"
}

function TEST_scrub_with_moderate_io() {
  local dir=$1
  bootstrap_cluster "$dir" 4
  create_replicated_pool replA 1 2
  populate_pool replA 8
  wait_for_clean_fast
  local pgid
  pgid=$(require_first_pgid replA)
  mapfile -t pids < <(start_background_io replA 5 1)
  trigger_scrub_and_wait "$pgid"
  wait_for_pids "${pids[@]}"
  assert_cluster_ok
  cleanup_cluster "$dir"
}

function TEST_scrub_with_heavy_io() {
  local dir=$1
  bootstrap_cluster "$dir" 4
  create_replicated_pool replA 1 2
  populate_pool replA 10
  wait_for_clean_fast
  local pgid
  pgid=$(require_first_pgid replA)
  mapfile -t pids < <(start_background_io replA 7 3)
  trigger_scrub_and_wait "$pgid"
  wait_for_pids "${pids[@]}"
  assert_cluster_ok
  cleanup_cluster "$dir"
}

function TEST_scrub_with_single_osd_failure() {
  local dir=$1
  bootstrap_cluster "$dir" 4
  create_replicated_pool replA 1 2
  populate_pool replA 8
  wait_for_clean_fast
  local pgid
  pgid=$(require_first_pgid replA)
  mapfile -t pids < <(start_background_io replA 4 1)
  stop_osd "$dir" 3
  wait_for_health_recoverable
  restart_osd "$dir" 3
  wait_for_clean_fast
  trigger_scrub_and_wait "$pgid"
  wait_for_pids "${pids[@]}"
  assert_cluster_ok
  cleanup_cluster "$dir"
}

function TEST_scrub_with_multiple_osd_failures() {
  local dir=$1
  bootstrap_cluster "$dir" 4
  create_replicated_pool replA 1 2
  populate_pool replA 8
  wait_for_clean_fast
  local pgid
  pgid=$(require_first_pgid replA)
  stop_osd "$dir" 2
  stop_osd "$dir" 3
  wait_for_health_recoverable
  restart_osd "$dir" 2
  restart_osd "$dir" 3
  wait_for_clean_fast
  trigger_scrub_and_wait "$pgid"
  assert_cluster_ok
  cleanup_cluster "$dir"
}

function TEST_multi_pool_scrub() {
  local dir=$1
  bootstrap_cluster "$dir" 4
  create_replicated_pool replA 1 2
  create_replicated_pool replB 1 2
  create_ec_pool ec21 ecpool 1
  populate_pool replA 5
  populate_pool replB 5
  populate_pool ecpool 5
  wait_for_clean_fast
  local pgid_a pgid_b pgid_ec
  pgid_a=$(require_first_pgid replA)
  pgid_b=$(require_first_pgid replB)
  pgid_ec=$(require_first_pgid ecpool)
  mapfile -t pids_a < <(start_background_io replA 5 1)
  mapfile -t pids_b < <(start_background_io replB 5 1)
  trigger_scrub_and_wait "$pgid_a"
  trigger_scrub_and_wait "$pgid_b"
  trigger_deep_scrub_and_wait "$pgid_ec"
  wait_for_pids "${pids_a[@]}"
  wait_for_pids "${pids_b[@]}"
  assert_cluster_ok
  cleanup_cluster "$dir"
}

function TEST_long_duration_io_with_scrub() {
  local dir=$1
  bootstrap_cluster "$dir" 4
  create_replicated_pool replA 1 2
  populate_pool replA 10
  wait_for_clean_fast
  local pgid
  pgid=$(require_first_pgid replA)
  mapfile -t pids < <(start_background_io replA 12 2)
  trigger_scrub_and_wait "$pgid"
  wait_for_pids "${pids[@]}"
  assert_cluster_ok
  cleanup_cluster "$dir"
}

function TEST_simultaneous_pg_scrub() {
  local dir=$1
  bootstrap_cluster "$dir" 4

  create_replicated_pool replA 2 2
  populate_pool replA 12
  wait_for_pool_pg_clean replA 2

  local pgids
  mapfile -t pgids < <(pool_pgids replA)

  echo "Detected PGs: ${pgids[*]}"

  if [ "${#pgids[@]}" -lt 2 ]; then
    echo "ERROR: expected multiple PGs for simultaneous scrub test"
    ceph pg ls-by-pool replA || true
    exit 1
  fi

  local pgid
  for pgid in "${pgids[@]}"; do
    ceph pg scrub "$pgid" &
  done
  wait

  sleep 2
  assert_cluster_ok
  cleanup_cluster "$dir"
}

main student-multipool-scrub "$@"
