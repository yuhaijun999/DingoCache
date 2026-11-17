# Docker/VM 诊断：建立环境与应用复跑命令

以下是**命令配方，不是新增通过记录**。只依赖当前仓库、Docker、已可用的 `/dev/kvm`、
三个现有宿主挂载点和网络；不依赖备份仓库或 `.omx` 指针。宿主执行 Bash，使用有 Docker
权限的非 root 用户；三个 `/mnt/diskN/dfkv-validation` 父目录须已存在且该用户可写。
不要为此加载宿主模块、改 sysctl、开放 host network、使用 privileged 或 `:Z`。
已记录宿主为 `4.18.0-553.33.1.el8_10.x86_64`，guest 实际启动为 `6.8.0-147-generic`；
重下 `current` 不能保证同一内核。Ubuntu 24 guest 产物不可当作 Ubuntu 22/EL8 可移植包。

需要快速制造淘汰压力时，环境就绪后使用[快速淘汰冒烟配方](DISK_LIFETIME_QUICK_VALIDATION.md)，
无需等待默认保护期自然结束。

## A. 快速复用本轮仍运行的 VM（与 B 二选一）

本段只连接，不重建、不格式化、不替换已冻结源码。容器不存在/已停止就停止本段，
不要自动启动另一份访问相同磁盘的 QEMU；改用 B 创建新 RUN_ID。

```bash
set -euo pipefail
RUN_ID=20261117t035613z-d69e126
C=dfkv-validation-$RUN_ID
D1=/mnt/disk1/dfkv-validation/$RUN_ID
[[ $(docker inspect -f '{{.State.Running}}' "$C") == true ]]
[[ $(docker inspect -f '{{index .Config.Labels "dfkv.validation.run"}}' "$C") == "$RUN_ID" ]]
vmssh() { docker exec -i "$C" ssh -i /disk1/seed/id_ed25519 -p 2222 -o BatchMode=yes -o StrictHostKeyChecking=accept-new -o UserKnownHostsFile=/disk1/seed/known_hosts validator@127.0.0.1 "$@"; }
vmssh 'uname -a; test -f /etc/dfkv-validation-guest; test -x /opt/dfkv-build/dfkv_server'
```

已有 `/opt/dfkv` 和 `/opt/dfkv-build` 可直接到 D，以新 CASE 重跑。不要执行 C 的首次格式化。
`run.json`、原始日志和旧 XML 可辅助定位，但复跑只接受新输出目录。

## B. 从当前仓库创建全新环境

从仓库根执行；`mkdir` 遇到既有 RUN_ID 就失败，不复用旧盘。下载可在同一个新 run 内
续传；必须同时通过准确文件名的 SHA256SUMS 和下面记录的 SHA。若 `current` 已变化，
停止并获取对应已归档镜像，或明确登记新 SHA 后作为新环境重验，不能忽略校验失败。

```bash
set -euo pipefail
SRC=$(pwd -P); test -f "$SRC/deploy/validation/Dockerfile"
U=$(id -u); G=$(id -g); test "$U" -ne 0; test -c /dev/kvm
RUN_ID=$(date -u +%Y%m%dt%H%M%Sz)-$(python3 -c 'import secrets; print(secrets.token_hex(4))')
C=dfkv-validation-$RUN_ID
D1=/mnt/disk1/dfkv-validation/$RUN_ID
for i in 1 2 3; do
  P=/mnt/disk$i/dfkv-validation; test -d "$P"; test -w "$P"; mkdir "$P/$RUN_ID"
  printf '{"run_id":"%s","owner_uid":%s}\n' "$RUN_ID" "$U" > "$P/$RUN_ID/.dfkv-validation-owned"
done
printf 'RUN_ID=%q\nC=%q\nD1=%q\n' "$RUN_ID" "$C" "$D1" > "$D1/host.env"
docker build -t dfkv-validation-vm:ubuntu22.04 -f deploy/validation/Dockerfile deploy/validation
# Dockerfile pins Ubuntu 22: sha256:9abcaffc92a5279fff3a6a41635c0800ea33e528425fb389658806476e7defde.
docker image inspect dfkv-validation-vm:ubuntu22.04 > "$D1/wrapper-image.json"
docker run -d --name "$C" --label "dfkv.validation.run=$RUN_ID" \
  --user "$U:$G" --group-add "$(stat -c %g /dev/kvm)" --device /dev/kvm \
  --security-opt label=disable --memory 12g --cpus 8 \
  --mount "type=bind,source=$SRC,target=/src,readonly" \
  --mount "type=bind,source=$D1,target=/disk1" \
  --mount "type=bind,source=/mnt/disk2/dfkv-validation/$RUN_ID,target=/disk2" \
  --mount "type=bind,source=/mnt/disk3/dfkv-validation/$RUN_ID,target=/disk3" \
  --workdir /disk1 dfkv-validation-vm:ubuntu22.04 > "$D1/container.id"
# OpenSSH needs a passwd entry for the dynamic host UID; only container /etc changes.
docker exec -u 0 "$C" bash -ceu 'getent group "$2" >/dev/null || groupadd -g "$2" dfkvhost; getent passwd "$1" >/dev/null || useradd -M -u "$1" -g "$2" -s /bin/bash dfkvhost' -- "$U" "$G"
docker inspect "$C" > "$D1/container-inspect.json"
ce() { docker exec -i "$C" "$@"; }
ce bash -se <<'WRAPPER'
set -euo pipefail
cd /disk1; umask 077; mkdir seed
URL=https://cloud-images.ubuntu.com/noble/current
FILE=noble-server-cloudimg-amd64.img
curl -fL --retry 5 -C - -o noble-base.img "$URL/$FILE"
curl -fL --retry 5 -o SHA256SUMS "$URL/SHA256SUMS"
SHA=$(awk -v f="$FILE" '$2==f || $2=="*"f {print $1}' SHA256SUMS)
test "$SHA" = 04e8f2461a16a44e78a370284471f29616cdd9baf1893df25807b71f985b6c94
printf '%s  noble-base.img\n' "$SHA" | sha256sum -c -
ssh-keygen -q -t ed25519 -N '' -f seed/id_ed25519
cat > seed/user-data <<YAML
#cloud-config
users:
  - name: validator
    groups: [sudo]
    shell: /bin/bash
    sudo: ALL=(ALL) NOPASSWD:ALL
    lock_passwd: true
    ssh_authorized_keys:
      - $(cat seed/id_ed25519.pub)
ssh_pwauth: false
disable_root: true
YAML
printf 'instance-id: %s\nlocal-hostname: dfkv-validation\n' "$(hostname)" > seed/meta-data
cloud-localds seed/seed.img seed/user-data seed/meta-data
qemu-img create -f qcow2 -F qcow2 -b /disk1/noble-base.img guest.qcow2
qemu-img resize guest.qcow2 32G
for i in 1 2 3; do qemu-img create -f raw /disk$i/data.raw 64G; done
qemu-system-x86_64 -name dfkv-validation -machine q35,accel=kvm -cpu host -m 8192 -smp 6 \
  -display none -daemonize -pidfile /disk1/qemu.pid -serial file:/disk1/console.log \
  -monitor unix:/disk1/monitor.sock,server=on,wait=off \
  -drive file=/disk1/guest.qcow2,format=qcow2,if=none,id=root,aio=threads \
  -device virtio-blk-pci,drive=root,serial=dfkv-root,bootindex=1 \
  -drive file=/disk1/seed/seed.img,format=raw,media=cdrom,readonly=on \
  -netdev user,id=net0,hostfwd=tcp:127.0.0.1:2222-:22 -device virtio-net-pci,netdev=net0 \
  -drive file=/disk1/data.raw,format=raw,if=none,id=data1,aio=threads -device virtio-blk-pci,drive=data1,serial=dfkv-data1 \
  -drive file=/disk2/data.raw,format=raw,if=none,id=data2,aio=threads -device virtio-blk-pci,drive=data2,serial=dfkv-data2 \
  -drive file=/disk3/data.raw,format=raw,if=none,id=data3,aio=threads -device virtio-blk-pci,drive=data3,serial=dfkv-data3
python3 - <<'PYQ'
import json,pathlib
pid=pathlib.Path('/disk1/qemu.pid').read_text().strip()
args=pathlib.Path('/proc',pid,'cmdline').read_bytes().rstrip(b'\0').split(b'\0')
pathlib.Path('/disk1/qemu-command.json').write_text(json.dumps([a.decode() for a in args],indent=2)+'\n')
PYQ
qemu-system-x86_64 --version; dpkg-query -W > /disk1/wrapper-packages.txt
WRAPPER
vmssh() { ce ssh -i /disk1/seed/id_ed25519 -p 2222 -o BatchMode=yes -o ConnectTimeout=5 -o StrictHostKeyChecking=accept-new -o UserKnownHostsFile=/disk1/seed/known_hosts validator@127.0.0.1 "$@"; }
ready=0; for n in $(seq 1 180); do if vmssh true; then ready=1; break; fi; sleep 5; done
test "$ready" = 1; vmssh 'sudo cloud-init status --wait'
# Snapshot tracked working-tree files (including modifications) plus these declared new files only.
python3 - "$D1" <<'PY'
import os,pathlib,subprocess,sys
p=pathlib.Path(sys.argv[1]); names=set(subprocess.check_output(['git','ls-files','-z']).split(b'\0'))
names.update(os.fsencode(n) for n in ['deploy/validation/Dockerfile','deploy/validation/disk_scenarios.py','deploy/validation/guest_checks.sh','test/python/test_disk_validation.py','docs/DISK_LIFETIME_DOCKER_VALIDATION.md','docs/DISK_LIFETIME_DOCKER_SETUP.md'])
names=sorted(n for n in names if n and (os.path.isfile(n) or os.path.islink(n)))
p.joinpath('snapshot-files.nul').write_bytes(b'\0'.join(names)+b'\0')
PY
git rev-parse HEAD > "$D1/source-commit.txt"; git diff --binary HEAD > "$D1/source.diff"
tar --null -T "$D1/snapshot-files.nul" -czf "$D1/source.tgz"
sha256sum "$D1/source.tgz" > "$D1/source.sha256"
ce scp -i /disk1/seed/id_ed25519 -P 2222 -o BatchMode=yes -o StrictHostKeyChecking=yes -o UserKnownHostsFile=/disk1/seed/known_hosts /disk1/source.tgz validator@127.0.0.1:/tmp/dfkv-source.tgz
vmssh 'sudo mkdir /opt/dfkv; sudo tar -xzf /tmp/dfkv-source.tgz -C /opt/dfkv; sudo find /opt/dfkv -type f -exec sha256sum {} + | sudo tee /opt/dfkv-source.sha256 >/dev/null'
```

## C. 仅首次新建 guest：安装、核对虚拟盘后格式化、构建

下面的 `mkfs` 只通过 `vmssh` 在独立 guest 执行。逐盘先核对 root 和数据盘 serial、
确认无文件系统且未挂载；任何检查失败立即停止，不能删检查强行重跑。

```bash
vmssh 'sudo bash -se' <<'GUEST'
set -euo pipefail
apt-get update
apt-get install -y --no-install-recommends cmake ninja-build g++ git ca-certificates libibverbs-dev liburing-dev rdma-core ibverbs-providers ibverbs-utils iproute2 python3 python3-numpy python3-psutil python3-pytest pkg-config linux-modules-extra-$(uname -r)
test "$(lsblk -dn -o SERIAL /dev/vda)" = dfkv-root
[[ $(findmnt -n -o SOURCE /) == /dev/vda* ]]
for i in 1 2 3; do
  dev=/dev/disk/by-id/virtio-dfkv-data$i
  test -b "$dev"; test "$(lsblk -dn -o SERIAL "$dev")" = "dfkv-data$i"
  test -z "$(blkid -o value -s TYPE "$dev" || true)"
  test -z "$(lsblk -nro MOUNTPOINTS "$dev")"
  mkfs.ext4 -q -F "$dev"; mkdir -p /data/disk$i; mount "$dev" /data/disk$i
  chown validator:validator /data/disk$i
  printf 'dfkv-disposable-volume-%s\n' "$i" > /data/disk$i/.dfkv-validation-volume
  printf 'UUID=%s /data/disk%s ext4 defaults,nofail 0 2\n' "$(blkid -s UUID -o value "$dev")" "$i" >> /etc/fstab
done
touch /etc/dfkv-validation-guest
modprobe rdma_rxe; rdma link add rxe0 type rxe netdev lo
uname -a; rdma link show; ibv_devinfo -d rxe0
cmake -S /opt/dfkv -B /opt/dfkv-build -G Ninja -DCMAKE_BUILD_TYPE=Debug -DDFKV_BUILD_TESTS=ON -DDFKV_WITH_RDMA=ON -DDFKV_WITH_URING=ON
cmake --build /opt/dfkv-build -j4
E=/data/disk1/results/on-$(date -u +%Y%m%dT%H%M%S)-$$
on_status=0
bash /opt/dfkv/deploy/validation/guest_checks.sh /opt/dfkv-build "$E" || on_status=$?
# Preserve the ON failure and still execute the independent OFF configuration.
printf 'ON diagnostic exit=%s; inspect its new XML\n' "$on_status"
cmake -S /opt/dfkv -B /opt/dfkv-build-off -G Ninja -DCMAKE_BUILD_TYPE=Debug -DDFKV_BUILD_TESTS=ON -DDFKV_WITH_RDMA=OFF -DDFKV_WITH_URING=OFF
cmake --build /opt/dfkv-build-off -j4
E=/data/disk1/results/off-$(date -u +%Y%m%dT%H%M%S)-$$; mkdir -p "${E%/*}"; mkdir "$E" "$E/tmp"
off_status=0
TMPDIR="$E/tmp" ctest --test-dir /opt/dfkv-build-off --no-tests=error --output-on-failure --timeout 120 --output-junit "$E/ctest.xml" || off_status=$?
printf 'ON exit=%s OFF exit=%s\n' "$on_status" "$off_status"
(( on_status == 0 && off_status == 0 ))
GUEST
```

构建需要网络获取 GoogleTest；离线来源须另行固定并登记，不能借旧 build 冒充本次编译。
本轮全 ON 已发现两个 FIFO 关闭用例失败，C 段应保留非零退出，不得解释为环境全通过。
只做快速 15 项硬件探针时可运行 `sudo env RUN_FULL_CTEST=0 bash /opt/dfkv/deploy/validation/guest_checks.sh /opt/dfkv-build <新证据目录>`；这不包含全量关闭回归。
ON 的真实 ring/RXE probe 及全量 CTest 使用 root 和同一新 TMPDIR；OFF 也使用 root，
但自己的新 TMPDIR。检查新 XML 的实际执行、失败、跳过数；CTest 退出 0 不代表零 skip。

## D. 最小应用复跑：实际 server + C ABI + RXE + io_uring

本例只复跑一个 lifetime/M=2/RAM-off/buffered/read-coalesce-off 用例，不声称覆盖完整矩阵。
三盘 64 GiB 虚拟文件上设置总缓存 48 GiB，关闭后台 reclaim，避免将紧凑几何的合法驱逐
误判为无驱逐基线。每次自动新 CASE；stdout 是新结果位置，保留日志，不打印私钥。
接收池总预算256MiB、分块8MiB：本轮重复运行后默认256MiB块曾触发RXE的
高阶页分配失败（MR注册ENOMEM），并非磁盘容量不足。小分块只调整诊断配置，
不修改生产默认，也不将原36项矩阵追溯称作8MiB分块运行。

```bash
vmssh 'sudo timeout --kill-after=5 600 bash -se' <<'GUEST'
set -euo pipefail; test -f /etc/dfkv-validation-guest; ulimit -l unlimited
CASE=repro-$(date -u +%Y%m%dT%H%M%S)-$$; OUT=/data/disk1/results/$CASE
mkdir -p "${OUT%/*}"; mkdir "$OUT"
for i in 1 2 3; do test -f /data/disk$i/.dfkv-validation-volume; mkdir -p /data/disk$i/cases; mkdir /data/disk$i/cases/$CASE; done
export DFKV_RAM_TIER=0 DFKV_PUT_ACK_MODE=disk DFKV_SLAB_TABLE_SYNC_MS=0 DFKV_SLAB_RECLAIM_MS=0
export DFKV_SLAB_BUCKET_SEC=1000000 DFKV_SLAB_BUCKET_COUNT=64 DFKV_SLAB_LIFE_MAX_READS=2
export DFKV_RDMA_DEV=rxe0 DFKV_RDMA_DEPTH=4 DFKV_RDMA_RECV_SEGMENT_SIZE=268435456
# Keep the hard pool budget, but avoid high-order RXE MR bookkeeping allocations.
export DFKV_RDMA_RECV_CHUNK_BYTES=8388608
export DFKV_RDMA_MAX_PAYLOAD_BYTES=2097152 DFKV_RDMA_MAX_BLOCK_BYTES=2097152 DFKV_SERVER_URING=1
export PYTHONPATH=/opt/dfkv/integration/common/src
/opt/dfkv-build/dfkv_server --dir /data/disk1/cases/$CASE,/data/disk2/cases/$CASE,/data/disk3/cases/$CASE \
  --cap 51539607552 --store-engine slab --slab-write buffered --slab-eviction-policy lifetime \
  --port 12000 --rdma-port 12001 --rdma-dev rxe0 --max-msg 2097152 --read-coalesce 0 \
  --metrics-port 12010 --metrics-bind 127.0.0.1 > "$OUT/server.log" 2>&1 &
pid=$!
cleanup() {
  status=$?; trap - EXIT
  kill -TERM "$pid" 2>/dev/null || true
  for n in $(seq 1 30); do kill -0 "$pid" 2>/dev/null || break; sleep 1; done
  if kill -0 "$pid" 2>/dev/null; then kill -KILL "$pid"; status=1; fi
  server_status=0; wait "$pid" || server_status=$?
  printf '%s\n' "$server_status" > "$OUT/server.exit"
  (( server_status == 0 )) || status=1
  exit "$status"
}
trap cleanup EXIT
python3 - "$pid" "$OUT" <<'PY'
import os,pathlib,sys,time,urllib.request
for _ in range(300):
    os.kill(int(sys.argv[1]),0)
    try:
        if urllib.request.urlopen('http://127.0.0.1:12010/readyz',timeout=1).status==200: break
    except Exception: pass
    time.sleep(.1)
else: raise SystemExit('server readiness timeout')
pathlib.Path(sys.argv[2],'before.prom').write_bytes(urllib.request.urlopen('http://127.0.0.1:12010/metrics',timeout=3).read())
PY
DFKV_RDMA=1 timeout 300 python3 /opt/dfkv/deploy/validation/disk_scenarios.py --lib /opt/dfkv-build/libdfkv.so --members n=127.0.0.1:12001 --output "$OUT/trace" --scenario roundtrip --readers 8 > "$OUT/client.log" 2>&1
python3 - "$OUT" <<'PY'
import json,pathlib,re,sys,urllib.request
p=pathlib.Path(sys.argv[1]); after=urllib.request.urlopen('http://127.0.0.1:12010/metrics',timeout=3).read().decode()
p.joinpath('after.prom').write_text(after)
before=p.joinpath('before.prom').read_text()
def values(text,name):
    rows=[(line.rsplit(' ',1)[0],float(line.rsplit(' ',1)[1])) for line in text.splitlines() if re.match(re.escape(name)+r'(?: |\{)',line)]
    if not rows: raise SystemExit('missing metric '+name)
    return rows
def total(text,name): return sum(v for _,v in values(text,name))
def require(ok,why):
    if not ok: raise SystemExit(why)
result=json.loads(p.joinpath('trace/result.json').read_text())
require(result['status']=='passed' and result['transport_mode']==['rdma','rdma'],'client failed or non-RDMA')
for name in ['dfkv_storage_healthy','dfkv_slab_healthy']:
    require(total(after,name)==1,'unhealthy '+name)
for name in ['dfkv_errors_total','dfkv_evictions_total','dfkv_uring_inflight']:
    require(total(after,name)==0,'unexpected '+name)
for name in ['dfkv_uring_reads_total','dfkv_uring_completions_total']:
    require(total(after,name)-total(before,name)==94,'wrong real read count '+name)
require(total(after,'dfkv_uring_init_fallbacks_total')-total(before,'dfkv_uring_init_fallbacks_total')==0,'ring fallback')
objects=values(after,'dfkv_disk_objects')
require(len(objects)==3 and all(v>0 for _,v in objects),'three disks did not participate')
require(total(after,'dfkv_rdma_pull_requests_dispatched_total')==98,'wrong dispatch count')
require(total(after,'dfkv_rdma_pull_requests_terminal_total')==98,'unsettled PULL')
for labels,value in values(after,'dfkv_rdma_pull_requests_terminal_total'):
    require(value==0 or 'result="success_nonempty"' in labels or 'result="not_found"' in labels,'unexpected PULL terminal '+labels)
for name in ['dfkv_rdma_pull_read_notification_notified_total','dfkv_slab_lifetime_read_applied_total']:
    require(total(after,name)==94,'wrong logical heat '+name)
p.joinpath('metrics-check.json').write_text(json.dumps({'status':'passed','real_reads':94,'logical_heat':94})+'\n')
print(p); print(json.dumps(result))
PY
GUEST
```

必须另核 `trace/result.json` 的两个 namespace 都是 RDMA、零错字节/长度，三个
`dfkv_disk_objects` 都有对象，health=1、errors=0、evictions=0；比较 before/after，要求
`dfkv_uring_reads_total` 和 completions 正增量、fallback 增量为 0、inflight 最终为 0、
PULL dispatched=terminal。仅客户端退出 0 不是这些路径的完整证明。

结果留在 guest `/data/disk1/results`，可用相同 SSH key 和容器内 `scp -r` 拉到
`/disk1/results-<新名称>`（对应宿主 `$D1`），不要拉 `/etc` 或 seed 私钥。
此配方不自动清理；关闭前先归档结果。清理须精确核对 container ID、run label、三个
RUN_ID 子目录及各 ownership 标记，只处理本轮完整子目录；不得 prune 或删除共享父目录。

**验证边界：**本附录依据已记录 run.json、ssh.sh、guest-prepare.sh、app-smoke.sh 和
当前源码整理；原环境的实际执行结果见[诊断报告](DISK_LIFETIME_DOCKER_VALIDATION.md#8-本轮实际结果2026-11-17)。A+D 已在保留 VM 中实际复跑成功（8MiB接收分块，94次真实读/94次计热、三盘参与、终局核对通过、服务退出0）；B+C 经 shell 语法核对，未再次从空盘逐行重建，不能把配方文本当作第二次环境验收。

## E. 重跑完整应用矩阵和恢复场景（本轮证据包）

矩阵 supervisor 是本轮诊断原始脚本，不作为生产工具；保存在宿主 disk1 的
`evidence-complete.tgz`（解压后 `app-matrix-final.py`、`lifecycle.py`）。先读脚本，
只在本轮/新建的私有 guest 使用；两者均固定使用12000/12001/12010端口，必须串行。
`post_audit.py` 是原结果的复核，复跑时须同步其输入路径，不是可直接验证任意 run 的工具。

复跑前确认没有 dfkv_server，保留原脚本不变，**生成新的派生脚本**：
将 matrix 脚本中所有 `app-matrix-final` 替换为唯一新名字（例如 `repeat-matrix-<UTC>`）；
将 lifecycle 脚本中 `/results/lifecycle` 和 `/cases/lifecycle/` 的 `lifecycle` 都换成
同一个唯一新名字。除此之外不要修改断言、读写数量和环境参数。若 RXE 注册大块失败，
明确记录并给派生脚本进程加 `DFKV_RDMA_RECV_CHUNK_BYTES=8388608`，不能事后隐瞒配置变化。
先检查替换后的输出/三盘路径不存在，再通过容器 `scp` 将派生脚本复制到 guest `/tmp`。

分别在 guest 用 `sudo env DFKV_RDMA_RECV_CHUNK_BYTES=8388608 timeout --kill-after=5 1800 python3 <派生脚本>`
执行；任一非零/超时都保留失败现场并停止，不以重试覆盖旧目录。原脚本自身不接受命令行
参数，不能靠向它传 `--output` 改路径。执行后检查36项矩阵/6项恢复的summary、每个服务
退出码、bytes/namespace、实际ring参与和健康/错误/终局指标；压力miss必须与not_found
对应，不能由C ABI的hit=0直接推定合法淘汰。饱和1260次读分别校验M=1/2/4/63，
clean三次重启和dirty SIGKILL冷重置不能合并成普通roundtrip。

不要累计无限case：保留需比较的证据后，可以只清理本轮确认闲置、已归档的旧case目录，
或者用B新建全新RUN_ID；不要对占满的虚拟盘反复重试或删除其他测试/宿主数据。
此节说明原 supervisor 的再执行方法，本轮未新增第二轮完整36+6通过记录。
