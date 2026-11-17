#!/usr/bin/env bash
# Run INSIDE the disposable VM, after build and real RXE/ring probes.
set -euo pipefail
B="${1:?build directory required}"
E="${2:?new evidence directory required}"
mkdir -p "$(dirname "$E")"
mkdir "$E" # Never accept stale XML from an earlier run.
export TMPDIR="$E/tmp"
mkdir "$TMPDIR"
export DFKV_RDMA_DEV="${DFKV_RDMA_DEV:-rxe0}"
export DFKV_SLAB_WRITE=buffered
ulimit -l unlimited
check_xml() {
  python3 - "$@" <<'CHECK_GTEST_XML'
import sys, xml.etree.ElementTree as ET
cases=list(ET.parse(sys.argv[1]).getroot().iter('testcase'))
if len(cases)!=int(sys.argv[2]) or not cases:
    raise SystemExit(f'wrong selected count: {len(cases)}')
allow_skip=len(sys.argv)>3 and sys.argv[3]=='allow-skip'
skipped=0
for case in cases:
    is_skip=case.find('skipped') is not None
    if case.get('status')=='notrun' and not is_skip:
        raise SystemExit(f'required case did not run: {case.attrib}')
    if is_skip:
        if not allow_skip:
            raise SystemExit(f'required case skipped: {case.attrib}')
        skipped+=1
    if case.find('failure') is not None or case.find('error') is not None:
        raise SystemExit(f'required case failed: {case.attrib}')
print(sys.argv[1],len(cases)-skipped,'passes,',skipped,'skips')
CHECK_GTEST_XML
}
check() {
  local name="$1" expected="$2" binary="$3" filter="$4"
  DFKV_STORE_ENGINE=slab timeout 120 "$B/$binary" \
    "--gtest_filter=$filter" "--gtest_output=xml:$E/$name.xml" \
    > "$E/$name.log" 2>&1
  check_xml "$E/$name.xml" "$expected"
}
check rxe-probe 1 rdma_loopback_test 'RdmaLoopback.PutGetExistMissOverRdma'
check kernel-uring 5 uring_reader_test 'UringReader.SubmitAndReapFillEveryDescAtItsOwnOffset:UringReader.PerReadErrorIsRecordedNotInfraFailure:UringReader.EofYieldsBytesUpToEof:UringReader.DrainOnIdleRingIsNoop:UringReader.RepeatedBatchesReuseTheRingCleanly'
# Real QP/Serve; async test backend controls disk CQEs, not kernel disk reads.
check pull-serve 9 rdma_loopback_test 'RdmaPullTerminalServeTest.*:RdmaLoopback.DynamicPullTeardownAbortsAccountingAndReleasesPin:RdmaLoopback.DynamicPullFallbackAccountsOneAccess:RdmaLoopback.AsyncReservedReadDefersRemoveAndLaterPut:RdmaLoopback.FailedAsyncReservedReadDefersRemoveAndLaterPut:RdmaLoopback.WaitingReservedReadDefersSameBatchMutations:RdmaLoopback.ShortAsyncReservedReadReleasesWriterBeforeMutations'
if [[ "${RUN_FULL_CTEST:-1}" == 1 ]]; then
  ctest --test-dir "$B" --show-only=json-v1 > "$E/discovery.json"
  expected=$(python3 -c 'import json,sys; print(len(json.load(open(sys.argv[1]))["tests"]))' "$E/discovery.json")
  ctest_status=0
  DFKV_STORE_ENGINE=file ctest --test-dir "$B" --no-tests=error \
    --output-on-failure --timeout 120 --output-junit "$E/ctest.xml" \
    > "$E/ctest.log" 2>&1 || ctest_status=$?
  check_xml "$E/ctest.xml" "$expected" allow-skip
  exit "$ctest_status"
fi
