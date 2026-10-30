#!/usr/bin/env bash
# Integration smoke for dfkv_server + dfkv_smoke + dfkvctl + dfkv_bench. arg1 = build dir.
set -e
BUILD="${1:?build dir}"

# --version: each binary prints its name + a version string and exits 0 (must NOT
# fall through to running the daemon).
for b in dfkv_server dfkv_mds dfkvctl dfkv_bench dfkv_smoke \
         dfkv_discover_smoke dfkv_gpu_dedup_repro; do
  out=$("$BUILD/$b" --version)
  echo "$out" | grep -qE "^$b [0-9]+\.[0-9]+" || { echo "$b --version bad: '$out'"; exit 1; }
done
echo "version smoke OK"
"$BUILD/dfkv_server" --help | grep -q \
  'storage backend: slab | file (default slab)'
"$BUILD/dfkv_server" --help | grep -q -- '--slab-eviction-policy'
rc=0
help_error=$("$BUILD/dfkv_server" --help 2>&1 >/dev/full) || rc=$?
[ "$rc" = 1 ] || { echo "help write failure: expected exit 1, got $rc"; exit 1; }
[ -z "$help_error" ] || { echo "unexpected help error: $help_error"; exit 1; }

D=$(mktemp -d)
mkdir "$D/cache"
: > "$D/srv.out"   # pre-create so the awk below can't fail on a missing file under `set -e`
env -u DFKV_STORE_ENGINE -u DFKV_SLAB_WRITE -u DFKV_SLAB_EVICTION_POLICY "$BUILD/dfkv_server" \
  --dir "$D/cache" --port 0 --cap 1073741824 --slab-granularity 1048576 \
  >>"$D/srv.out" 2>&1 &
PID=$!
trap 'if [ -n "$PID" ]; then kill "$PID" 2>/dev/null || true; wait "$PID" 2>/dev/null || true; fi; rm -rf "$D"' EXIT
P=""
# `|| true`: a transient awk read must never trip `set -e` before the server prints PORT.
for i in $(seq 1 100); do P=$(awk '/PORT/{print $2}' "$D/srv.out" 2>/dev/null || true); [ -n "$P" ] && break; sleep 0.05; done
[ -n "$P" ] || { echo "server did not report PORT"; cat "$D/srv.out"; exit 1; }
[ -f "$D/cache/slots.tbl" ] || {
  echo "default server did not create slab metadata"
  cat "$D/srv.out"
  exit 1
}
[ ! -d "$D/cache/blocks" ] || {
  echo "default server silently selected the file engine"
  exit 1
}
"$BUILD/dfkv_smoke" --members "n=127.0.0.1:$P" --size 4096
"$BUILD/dfkvctl" --members "n=127.0.0.1:$P" --namespace "dfkv/smoke" put k v12345
got=$("$BUILD/dfkvctl" --members "n=127.0.0.1:$P" --namespace "dfkv/smoke" get k)
[ "$got" = "v12345" ] || { echo "get mismatch: '$got'"; exit 1; }
"$BUILD/dfkvctl" --members "n=127.0.0.1:$P" --namespace "dfkv/smoke" exist k | grep -q true
"$BUILD/dfkvctl" stat "127.0.0.1:$P" | grep -q dfkv_cache_put_total
echo "tool_smoke OK (port $P)"

# Strict arg parsing: an unknown flag / bad number / bad --advertise must fail
# fast (exit 2), not run with silent defaults. (`set -e` is on, so guard the
# expected-nonzero calls.)
rc=0; "$BUILD/dfkv_server" --dir "$D" --capp 5 --port 0 >/dev/null 2>&1 || rc=$?
[ "$rc" = 2 ] || { echo "dfkv_server unknown flag: expected exit 2, got $rc"; exit 1; }
rc=0; "$BUILD/dfkv_server" --dir "$D" --cap 5TiB --port 0 >/dev/null 2>&1 || rc=$?
[ "$rc" = 2 ] || { echo "dfkv_server bad --cap: expected exit 2, got $rc"; exit 1; }
rc=0; "$BUILD/dfkv_server" --dir "$D" --advertise no-colon --port 0 >/dev/null 2>&1 || rc=$?
[ "$rc" = 2 ] || { echo "dfkv_server bad --advertise: expected exit 2, got $rc"; exit 1; }
rc=0; DFKV_MDS_REGISTRATION_TIMEOUT_MS=garbage "$BUILD/dfkv_server" \
  --dir "$D" --port 0 --cap 5 >/dev/null 2>&1 || rc=$?
[ "$rc" = 2 ] || {
  echo "dfkv_server bad MDS registration timeout env: expected exit 2, got $rc"
  exit 1
}
rc=0; "$BUILD/dfkv_server" --dir "$D" --port 0 --cap 5 \
  --mds-registration-timeout-ms 600001 >/dev/null 2>&1 || rc=$?
[ "$rc" = 2 ] || {
  echo "dfkv_server out-of-range MDS registration timeout: expected exit 2, got $rc"
  exit 1
}
echo "strict-args smoke OK"
# Invalid slab geometry must refuse startup, and the explicit flag must beat a
# conflicting file environment. A hidden fallback would run until timeout 124.
mkdir "$D/invalid-slab"
rc=0
DFKV_STORE_ENGINE=file timeout 3s "$BUILD/dfkv_server" \
  --dir "$D/invalid-slab" --port 0 --cap 1073741825 \
  --store-engine slab >/dev/null 2>&1 || rc=$?
[ "$rc" = 1 ] || {
  echo "invalid default slab geometry: expected exit 1, got $rc"
  exit 1
}
[ ! -d "$D/invalid-slab/blocks" ] || {
  echo "invalid slab geometry silently fell back to file"
  exit 1
}
echo "slab-default fail-closed smoke OK"

# A cache node configured for MDS must exit fail-closed instead of remaining an
# active, permanently-unready data listener when its first registration cannot
# succeed. exit 124 would mean GNU timeout killed a hung daemon.
mkdir "$D/unregistered"
rc=0
DFKV_STORE_ENGINE=file timeout 5s "$BUILD/dfkv_server" \
  --dir "$D/unregistered" --port 0 --cap 1048576 \
  --mds 127.0.0.1:1 --group deadline-smoke --id deadline-node \
  --advertise 127.0.0.1:28000 --mds-registration-timeout-ms 1000 \
  >"$D/unregistered.out" 2>&1 || rc=$?
[ "$rc" = 1 ] || {
  echo "dfkv_server first-registration deadline: expected exit 1, got $rc"
  exit 1
}
grep -qE 'DFKV_STORE_ENGINE[[:space:]]*= file[[:space:]]+[(]env[)]' \
  "$D/unregistered.out" || {
  echo "DFKV_STORE_ENGINE=file did not select the diagnostic file engine"
  exit 1
}
[ ! -f "$D/unregistered/slots.tbl" ] || {
  echo "explicit file engine unexpectedly created slab metadata"
  exit 1
}
echo "MDS first-registration deadline smoke OK"

# dfkv_bench has automation-facing exit semantics: malformed CLI is 2, a
# completed phase with failed operations is 1, and only zero-failure phases are 0.
expect_bench_usage_error() {
  label=$1
  shift
  rc=0
  "$BUILD/dfkv_bench" "$@" >/dev/null 2>&1 || rc=$?
  [ "$rc" = 2 ] || {
    echo "dfkv_bench $label: expected exit 2, got $rc"
    exit 1
  }
}
expect_bench_usage_error "unknown flag" --members "n=127.0.0.1:$P" --bogus x
expect_bench_usage_error "dangling flag" --members "n=127.0.0.1:$P" --size
expect_bench_usage_error "bad number" --members "n=127.0.0.1:$P" --count nope
expect_bench_usage_error "overflowing number" --members "n=127.0.0.1:$P" \
  --count 184467440737095516160
expect_bench_usage_error "invalid op" --members "n=127.0.0.1:$P" --op delete
expect_bench_usage_error "zero size" --members "n=127.0.0.1:$P" --size 0
expect_bench_usage_error "zero count" --members "n=127.0.0.1:$P" --count 0
expect_bench_usage_error "zero threads" --members "n=127.0.0.1:$P" --threads 0
expect_bench_usage_error "zero batch" --members "n=127.0.0.1:$P" --batch 0

out=$("$BUILD/dfkv_bench" --members "n=127.0.0.1:$P" --size 4096 --count 4 \
  --threads 2 --batch 2 --op both --key-seed "tool-smoke-$$")
echo "$out" | grep -qE '^PUT[[:space:]].*fails=0$' || {
  echo "dfkv_bench successful PUT report missing: '$out'"
  exit 1
}
echo "$out" | grep -qE '^GET[[:space:]].*fails=0$' || {
  echo "dfkv_bench successful GET report missing: '$out'"
  exit 1
}
echo "$out" | grep -qE '^GET_SETUP registered_arena_bytes=[1-9][0-9]* ' || {
  echo "dfkv_bench registered GET arena report missing: '$out'"
  exit 1
}
echo "$out" | grep -qE '^GET[[:space:]].*goodput [0-9.]+ GB/s.*ok=4 fails=0$' || {
  echo "dfkv_bench fail-adjusted goodput report missing: '$out'"
  exit 1
}
echo "$out" | grep -qE '^DIAG phase=GET .*ring=1 .*io_errors=0 .*completion_timeouts=0 ' || {
  echo "dfkv_bench GET diagnostics missing: '$out'"
  exit 1
}

rc=0
out=$("$BUILD/dfkv_bench" --members "n=127.0.0.1:$P" --size 4096 --count 2 \
  --threads 1 --batch 1 --op get --key-seed "tool-smoke-missing-$$") || rc=$?
[ "$rc" = 1 ] || { echo "dfkv_bench misses: expected exit 1, got $rc"; exit 1; }
echo "$out" | grep -qE '^GET[[:space:]].*fails=2$' || {
  echo "dfkv_bench failed GET report missing: '$out'"
  exit 1
}
echo "dfkv_bench parser/exit smoke OK"

# One-shot CLI failures must include native ring/transport diagnostics rather
# than only printing FAIL. Port 1 is intentionally unreachable.
rc=0
"$BUILD/dfkvctl" --members "n=127.0.0.1:1" --namespace "dfkv/smoke" \
  put unavailable value >"$D/ctl-fail.out" 2>"$D/ctl-fail.err" || rc=$?
[ "$rc" = 1 ] || {
  echo "dfkvctl failed PUT: expected exit 1, got $rc"
  exit 1
}
grep -qE 'dfkvctl put diagnostics: ring=1 io_errors=[1-9][0-9]*' \
  "$D/ctl-fail.err" || {
  echo "dfkvctl failed PUT diagnostics missing"
  cat "$D/ctl-fail.err"
  exit 1
}

# MDS must fail loud (exit 1) when etcd is unreachable, within the probe window,
# instead of running "healthy" while every registration silently fails.
rc=0; DFKV_MDS_ETCD_PROBE_MS=500 "$BUILD/dfkv_mds" --listen 0 --etcd 127.0.0.1:9 \
  >/dev/null 2>&1 || rc=$?
[ "$rc" = 1 ] || { echo "dfkv_mds bad etcd: expected exit 1, got $rc"; exit 1; }
echo "mds etcd-probe smoke OK"

# CLI policy selection must reach the existing library checks, not a second
# parser or an env rewrite. Conflicting invalid env must not win over the flag.
for engine in slab file; do
  rc=0
  DFKV_SLAB_EVICTION_POLICY=unknown DFKV_SLAB_WRITE=buffered \
    timeout --kill-after=2s 3s "$BUILD/dfkv_server" \
    --dir "$D/policy-$engine" --port 0 --cap 1073741824 \
    --store-engine "$engine" --slab-eviction-policy LeGaCy \
    --slab-granularity 1048576 \
    >"$D/policy-$engine.out" 2>&1 || rc=$?
  [ "$rc" = 124 ] || { cat "$D/policy-$engine.out"; exit 1; }
  grep -q '^PORT ' "$D/policy-$engine.out"
  grep -qE 'DFKV_SLAB_EVICTION_POLICY[[:space:]]*= LeGaCy[[:space:]]+[(]flag[)]' \
    "$D/policy-$engine.out"
done
[ -f "$D/policy-slab/slots.tbl" ]
[ ! -e "$D/policy-file/slots.tbl" ]

rc=0
"$BUILD/dfkv_server" --dir "$D/missing-policy-value" \
  --slab-eviction-policy >"$D/missing-policy-value.out" 2>&1 || rc=$?
[ "$rc" = 2 ]
grep -q 'missing value for --slab-eviction-policy' "$D/missing-policy-value.out"
[ ! -e "$D/missing-policy-value" ]

for engine in slab file; do
  i=0
  for policy in '' unknown lifetime; do
    target="$D/reject-$engine-$i"
    rc=0
    DFKV_SLAB_EVICTION_POLICY=legacy timeout --kill-after=2s 3s "$BUILD/dfkv_server" \
      --dir "$target" --port 0 --cap 1073741824 --store-engine "$engine" \
      --slab-eviction-policy "$policy" >"$D/reject.out" 2>&1 || rc=$?
    [ "$rc" = 1 ] || { cat "$D/reject.out"; exit 1; }
    grep -q 'ERROR.*--slab-eviction-policy' "$D/reject.out"
    grep -qF -- "='$policy'" "$D/reject.out"
    [ ! -e "$target" ]
    i=$((i + 1))
  done
done

# The timed-out slab process handled SIGTERM and closed cleanly. Abruptly stop
# the original serving process only after its last use to obtain a real dirty
# store, with committed data, without fabricating epoch records.
# In v3 slab_state, the native uint32 at byte 16 is clean=1 or dirty=2.
[ "$(od -An -tu4 -j16 -N4 "$D/policy-slab/slab_state" | tr -d ' \n')" = 1 ]
kill -KILL "$PID"
rc=0; wait "$PID" 2>/dev/null || rc=$?
PID=""
[ "$rc" = 137 ]
[ "$(od -An -tu4 -j16 -N4 "$D/cache/slab_state" | tr -d ' \n')" = 2 ]

policy_metadata() {
  ( cd "$1"
    # Hash the required metadata, not the 1 GiB payload extent contents.
    find . -printf '%P %y %s\n' | sort
    for f in slots.tbl slab_state slab_meta slab_initializing; do
      if [ -e "$f" ]; then sha256sum "$f"; else echo "missing $f"; fi
    done
  )
}
for policy in '' unknown lifetime; do
  policy_metadata "$D/policy-slab" >"$D/clean.before"
  policy_metadata "$D/cache" >"$D/dirty.before"
  rc=0
  DFKV_SLAB_EVICTION_POLICY=legacy timeout --kill-after=2s 3s "$BUILD/dfkv_server" \
    --dir "$D/policy-slab,$D/cache" --port 0 --cap 2147483648 --store-engine slab \
    --slab-eviction-policy "$policy" >"$D/metadata-reject.out" 2>&1 || rc=$?
  [ "$rc" = 1 ] || { cat "$D/metadata-reject.out"; exit 1; }
  grep -q 'ERROR.*--slab-eviction-policy' "$D/metadata-reject.out"
  policy_metadata "$D/policy-slab" >"$D/clean.after"
  policy_metadata "$D/cache" >"$D/dirty.after"
  cmp "$D/clean.before" "$D/clean.after"
  cmp "$D/dirty.before" "$D/dirty.after"
done
echo "eviction-policy CLI and clean/dirty refusal smoke OK"

# B is a real clean store with a different persisted slot quantum. Detect its
# incompatible slab_meta before A can increment its epoch or reset dirty data.
rc=0
timeout --kill-after=2s 3s "$BUILD/dfkv_server" \
  --dir "$D/different-geometry" --port 0 --cap 1073741824 \
  --store-engine slab --slab-write buffered --slab-granularity 2097152 \
  --slab-eviction-policy legacy >"$D/different-geometry.out" 2>&1 || rc=$?
[ "$rc" = 124 ]
grep -q '^PORT ' "$D/different-geometry.out"
for first in "$D/policy-slab" "$D/cache"; do
  policy_metadata "$first" >"$D/first.before"
  policy_metadata "$D/different-geometry" >"$D/second.before"
  rc=0
  timeout --kill-after=2s 3s "$BUILD/dfkv_server" \
    --dir "$first,$D/different-geometry" --port 0 --cap 2147483648 \
    --store-engine slab --slab-write buffered --slab-granularity 1048576 \
    --slab-eviction-policy legacy >"$D/layout-reject.out" 2>&1 || rc=$?
  [ "$rc" = 1 ] || { cat "$D/layout-reject.out"; exit 1; }
  grep -qF "$D/different-geometry: existing slab layout differs" \
    "$D/layout-reject.out"
  policy_metadata "$first" >"$D/first.after"
  policy_metadata "$D/different-geometry" >"$D/second.after"
  cmp "$D/first.before" "$D/first.after"
  cmp "$D/second.before" "$D/second.after"
done
echo "all-disk slab_meta preflight smoke OK"

# A non-regular metadata file must fail promptly, not block in open(FIFO).
mkdir "$D/fifo-meta"
mkfifo "$D/fifo-meta/slab_meta"
rc=0
timeout --kill-after=2s 3s "$BUILD/dfkv_server" --dir "$D/fifo-meta" \
  --store-engine slab --slab-write buffered --slab-eviction-policy legacy \
  >"$D/fifo-meta.out" 2>&1 || rc=$?
[ "$rc" = 1 ]
grep -q 'slab_meta size/type invalid' "$D/fifo-meta.out"
echo "non-regular slab_meta refusal smoke OK"

# Keep B's layout compatible; corrupt only its epoch CRC-covered padding so
# slab_meta preflight alone cannot mask a missing all-disk epoch check.
rc=0
timeout --kill-after=2s 3s "$BUILD/dfkv_server" --dir "$D/bad-epoch" \
  --cap 1073741824 --store-engine slab --slab-write buffered \
  --slab-granularity 1048576 --slab-eviction-policy legacy \
  >"$D/epoch-create.out" 2>&1 || rc=$?
[ "$rc" = 124 ]
grep -q '^PORT ' "$D/epoch-create.out"
printf '\001' | dd of="$D/bad-epoch/slab_state" bs=1 seek=24 conv=notrunc status=none
for first in "$D/policy-slab" "$D/cache"; do
  policy_metadata "$first" >"$D/epoch-first.before"
  policy_metadata "$D/bad-epoch" >"$D/epoch-second.before"
  rc=0
  timeout --kill-after=2s 3s "$BUILD/dfkv_server" \
    --dir "$first,$D/bad-epoch" --cap 2147483648 --port 0 \
    --store-engine slab --slab-write buffered --slab-granularity 1048576 \
    --slab-eviction-policy legacy >"$D/epoch-reject.out" 2>&1 || rc=$?
  [ "$rc" = 1 ] || { cat "$D/epoch-reject.out"; exit 1; }
  grep -qF "$D/bad-epoch: slab_state checksum or format mismatch" \
    "$D/epoch-reject.out"
  policy_metadata "$first" >"$D/epoch-first.after"
  policy_metadata "$D/bad-epoch" >"$D/epoch-second.after"
  cmp "$D/epoch-first.before" "$D/epoch-first.after"
  cmp "$D/epoch-second.before" "$D/epoch-second.after"
done
echo "all-disk epoch preflight smoke OK"
mkdir "$D/fifo-epoch"
mkfifo "$D/fifo-epoch/slab_state"
rc=0
timeout --kill-after=2s 3s "$BUILD/dfkv_server" --dir "$D/fifo-epoch" \
  --store-engine slab --slab-write buffered --slab-granularity 1048576 \
  --slab-eviction-policy legacy >"$D/fifo-epoch.out" 2>&1 || rc=$?
[ "$rc" = 1 ]
grep -q 'slab_state is truncated or has unexpected size/type' "$D/fifo-epoch.out"
echo "non-regular slab_state refusal smoke OK"
