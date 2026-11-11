#!/usr/bin/env bash
# =============================================================================
# ETLer 端到端测试 —— 只负责流程编排
#
# 固定文件（与脚本同目录，不由本脚本生成）：
#   e2etest.sh / config.json / sink.py / check.py / initdb.d/01-keyfile.sh
#
# 运行产物全部落在 <脚本同目录>/run/（非隐藏），每次重跑清空重建。
# config.json 会被复制进 run/（exe.AbsDir 要求它与 etler 同目录），原文件不动。
#
# 运行：
#   cd <repo>/e2etest && bash e2etest.sh
#   cd <repo>/e2etest && bash e2etest.sh --full
#
# 本地只需要：Docker（daemon 已启动）、Go >= 1.25、Python3。
# macOS 已适配：自动补 docker CLI 的 PATH、等待 daemon、不用 seq。
# =============================================================================

set -uo pipefail

FULL=0
[ "${1:-}" = "--full" ] && FULL=1

MONGO_CT="etl-mongo-e2e"
MUSER="root"
MPASS="e2epass123"
MONGO_PORT=27017
API_PORT=8080
SINK_PORT=9000

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
WORK="$SCRIPT_DIR"
RUN="$WORK/run"
CHECK="$WORK/check.py"
SINKLOG="$RUN/sink.jsonl"
ETLER_PID=""
SINK_PID=""

REPO=""
d="$WORK"
for _ in 1 2 3 4 5 6; do
  if [ -f "$d/go.mod" ] && grep -q '^module github.com/xpwu/ETLer' "$d/go.mod"; then REPO="$d"; break; fi
  [ "$d" = "/" ] && break
  d="$(dirname "$d")"
done
[ -n "$REPO" ] || { printf '没找到 ETLer 仓库：请把 e2etest.sh 放在项目内的 e2etest 文件夹里\n' >&2; exit 1; }

RED='\033[31m'; GRN='\033[32m'; YLW='\033[33m'; NC='\033[0m'
info() { printf '%b\n' "${YLW}[e2e]${NC} $*"; }
ok()   { printf '%b\n' "${GRN}[e2e]${NC} $*"; }

# ---------- 诊断：失败时把所有线索一次打全 ----------
die() {
  printf '%b\n' "${RED}[e2e] FAILED:${NC} $*" >&2

  printf '%s\n' "---------- sink.jsonl 全部内容 ----------" >&2
  if [ -s "$SINKLOG" ]; then
    cat "$SINKLOG" >&2
  else
    printf '%s\n' "(空文件)" >&2
  fi

  printf '%s\n' "---------- etler.log ----------" >&2
  if [ -s "$RUN/etler.log" ]; then
    cat "$RUN/etler.log" >&2
  else
    printf '%s\n' "(空文件：go-log 可能写到了别处，见下面 run/ 目录树)" >&2
  fi

  printf '%s\n' "---------- sink.log / mongosh.err ----------" >&2
  cat "$RUN/sink.log" 2>/dev/null >&2 || true
  tail -20 "$RUN/mongosh.err" 2>/dev/null >&2 || true

  printf '%s\n' "---------- 进程存活 ----------" >&2
  if [ -n "$ETLER_PID" ]; then
    ps -p "$ETLER_PID" -o pid=,stat=,command= >&2 2>/dev/null || printf 'etler 进程已退出\n' >&2
  fi
  if [ -n "$SINK_PID" ]; then
    ps -p "$SINK_PID" -o pid=,stat=,command= >&2 2>/dev/null || printf 'sink.py 进程已退出\n' >&2
  fi

  printf '%s\n' "---------- mongod 上挂起的 getMore（判断 watcher 是否建了 change stream） ----------" >&2
  docker exec "$MONGO_CT" mongosh --quiet \
    -u "$MUSER" -p "$MPASS" --authenticationDatabase admin \
    --eval 'printjson(db.currentOp({"op":"getmore"}).inprog.map(x=>({ns:x.ns, secs:x.secs_running, cmd:x.command})))' \
    2>&1 | tail -30 >&2 || true

  printf '%s\n' "---------- run/ 目录树 ----------" >&2
  ls -laR "$RUN" >&2 2>/dev/null || true

  printf '%s\n' "---------- docker logs (tail 20) ----------" >&2
  docker logs --tail 20 "$MONGO_CT" >&2 2>&1 || true
  printf '%s\n' "---------- 产物目录: $RUN ----------" >&2
  exit 1
}

cd "$WORK" || exit 1
info "仓库: $REPO"
info "固定文件目录: $WORK"
info "运行产物目录: $RUN"

# ---------- 1. 前置检查 ----------
DOCKER_CANDS=(
  /opt/homebrew/bin
  /usr/local/bin
  "$HOME/.docker/bin"
  "/Applications/Docker.app/Contents/Resources/bin"
  "/usr/local/opt/docker/bin"
)
for cand in "${DOCKER_CANDS[@]}"; do
  [ -d "$cand" ] || continue
  case ":$PATH:" in
    *":$cand:"*) ;;
    *) PATH="$PATH:$cand" ;;
  esac
done
export PATH

if ! command -v docker >/dev/null 2>&1; then
  printf '%s\n' "[e2e] 找不到 docker 命令（PATH 里没有 CLI，与 Docker Desktop 是否启动无关）" >&2
  printf '%s\n' '       1) export PATH="$HOME/.docker/bin:/Applications/Docker.app/Contents/Resources/bin:/usr/local/bin:/opt/homebrew/bin:$PATH"' >&2
  printf '%s\n' "       2) 把上面这行写进 ~/.zshrc，重开终端" >&2
  exit 1
fi
command -v go      >/dev/null 2>&1 || { printf '%s\n' "缺 go，需要 Go >= 1.25" >&2; exit 1; }
command -v python3 >/dev/null 2>&1 || { printf '%s\n' "缺 python3" >&2; exit 1; }

info "检查 Docker daemon（首次可能要等几十秒）..."
DAEMON_OK=0
for ((k=1; k<=90; k++)); do
  if docker info >/dev/null 2>&1; then DAEMON_OK=1; break; fi
  [ "$k" -eq 1 ] && info "daemon 尚未就绪，等待（最多 90s）..."
  sleep 1
done
if [ "$DAEMON_OK" -ne 1 ]; then
  printf '%s\n' "[e2e] docker 命令可用，但 daemon 连不上，请确认 Docker Desktop 状态为 Running" >&2
  docker info 2>&1 | tail -20 >&2 || true
  exit 1
fi
ok "Docker daemon 已就绪"

GOV="$(go version | sed -E 's/.*go([0-9]+)\.([0-9]+).*/\1 \2/')"
GOMAJ="${GOV%% *}"; GOMIN="${GOV##* }"
if [ "$GOMAJ" -lt 1 ] || { [ "$GOMAJ" -eq 1 ] && [ "$GOMIN" -lt 25 ]; }; then
  printf '%s\n' "Go 版本过低：$(go version)，需要 >= 1.25" >&2; exit 1
fi
info "Go: $(go version)"

FIXED_FILES=("config.json" "sink.py" "check.py" "initdb.d/01-keyfile.sh")
MISSING=0
for f in "${FIXED_FILES[@]}"; do
  if [ ! -f "$WORK/$f" ]; then
    printf '%s\n' "[e2e] 缺固定文件: $WORK/$f" >&2
    MISSING=1
  fi
done
[ "$MISSING" -eq 0 ] || { printf '%s\n' "       请把四个固定文件放在 e2etest.sh 同路径下" >&2; exit 1; }
ok "固定文件齐全"

# ---------- 1.1 清理上一轮 e2e 环境（必须在端口检查之前） ----------
stop_previous_run() {
  info "清理上一轮 e2e 残留（旧容器 / 旧 etler / 旧 sink）..."

  # 1) 同名 Mongo 容器必须先停掉，否则 27017 仍被占用
  if docker ps -a --format '{{.Names}}' 2>/dev/null | grep -Fxq "$MONGO_CT"; then
    info "发现旧容器 ${MONGO_CT}，停止并删除"
    docker rm -f "$MONGO_CT" >/dev/null 2>&1 || true
  else
    docker rm -f "$MONGO_CT" >/dev/null 2>&1 || true
  fi

  # 2) 读取上一轮写入的 pid 文件；只杀命令名匹配 e2e 的进程，避免误杀
  for pidfile in "$RUN/etler.pid" "$RUN/sink.pid"; do
    [ -f "$pidfile" ] || continue
    pid="$(tr -dc '0-9' < "$pidfile" 2>/dev/null || true)"
    [ -n "$pid" ] || continue

    cmd="$(ps -p "$pid" -o command= 2>/dev/null || true)"
    case "$cmd" in
      *etler*|*sink.py*)
        info "停止上一轮进程 $pid: $cmd"
        kill "$pid" 2>/dev/null || true
        sleep 1
        kill -9 "$pid" 2>/dev/null || true
        ;;
      *)
        info "pid $pid 已不是 e2e 进程，跳过"
        ;;
    esac
  done

  # 3) 兼容旧版本没有 pid 文件：按监听端口找，仍然只杀 e2e 进程
  if command -v lsof >/dev/null 2>&1; then
    for p in "$API_PORT" "$SINK_PORT"; do
      for pid in $(lsof -tiTCP:"$p" -sTCP:LISTEN 2>/dev/null || true); do
        cmd="$(ps -p "$pid" -o command= 2>/dev/null || true)"
        case "$cmd" in
          *etler*|*sink.py*)
            info "停止占用 $p 的旧 e2e 进程 $pid: $cmd"
            kill "$pid" 2>/dev/null || true
            sleep 1
            kill -9 "$pid" 2>/dev/null || true
            ;;
        esac
      done
    done
  fi
}

stop_previous_run

# 给 Docker Desktop 一点时间释放 27017；若仍占用，把占用者列出来
PORT_OK=0
for ((k=1; k<=10; k++)); do
  if python3 "$CHECK" --check-ports "$MONGO_PORT" "$API_PORT" "$SINK_PORT" >/dev/null 2>&1; then
    PORT_OK=1
    break
  fi
  sleep 1
done

if [ "$PORT_OK" -ne 1 ]; then
  printf '%s\n' "[e2e] 端口仍被占用，且不是本脚本残留（或旧容器删除后端口未释放）" >&2
  python3 "$CHECK" --check-ports "$MONGO_PORT" "$API_PORT" "$SINK_PORT" >&2 || true
  if command -v lsof >/dev/null 2>&1; then
    for p in "$MONGO_PORT" "$API_PORT" "$SINK_PORT"; do
      printf '%s\n' "--- 占用 $p 的进程 ---" >&2
      lsof -nP -iTCP:"$p" -sTCP:LISTEN >&2 2>/dev/null || true
    done
  fi
  exit 1
fi
info "端口 $MONGO_PORT / $API_PORT / $SINK_PORT 可用"

# 只清运行产物，固定文件一个不动
rm -rf "$RUN"
mkdir -p "$RUN"
cp "$WORK/config.json" "$RUN/config.json"

# ---------- 2. MongoDB 7 单节点副本集 ----------
KEY_B64="$(python3 -c 'import base64,os;print(base64.b64encode(os.urandom(756)).decode())')"

info "启动 MongoDB 7 副本集（首次会拉镜像，可能 1-3 分钟）..."
docker run -d --name "$MONGO_CT" -p "$MONGO_PORT:27017" \
  -e MONGO_INITDB_ROOT_USERNAME="$MUSER" \
  -e MONGO_INITDB_ROOT_PASSWORD="$MPASS" \
  -e E2E_KEY_B64="$KEY_B64" \
  -v "$WORK/initdb.d:/docker-entrypoint-initdb.d:ro" \
  mongo:7 --replSet rs0 --bind_ip_all --keyFile /tmp/mongo.key >/dev/null \
  || die "docker run 失败"

mongo_eval() {
  docker exec "$MONGO_CT" mongosh --quiet \
    -u "$MUSER" -p "$MPASS" --authenticationDatabase admin \
    --eval "$1" 2>>"$RUN/mongosh.err"
}

: > "$RUN/mongosh.err"
for ((k=1; k<=150; k++)); do
  if mongo_eval 'db.runCommand({ping:1}).ok' 2>/dev/null | grep -q 1; then break; fi
  sleep 1
done
mongo_eval 'db.runCommand({ping:1}).ok' 2>/dev/null | grep -q 1 \
  || die "MongoDB 启动/认证超时"
info "MongoDB 已就绪，初始化 rs0..."

mongo_eval 'try{rs.initiate({_id:"rs0",members:[{_id:0,host:"127.0.0.1:27017"}]})}catch(e){print(e.message)}' >/dev/null
for ((k=1; k<=60; k++)); do
  if mongo_eval 'db.hello().isWritablePrimary' 2>/dev/null | grep -q true; then break; fi
  sleep 1
done
mongo_eval 'db.hello().isWritablePrimary' 2>/dev/null | grep -q true \
  || die "副本集没能选出 PRIMARY"
ok "副本集 rs0 已 PRIMARY"

# 启动前写种子：让 oplog 非空，initWatching 才拿得到 resumeToken；
# 这 3 条只能经 Sync 送达，不能作为 ChangeStream 事件重放
mongo_eval 'db.getSiblingDB("demo").items.insertMany([{_id:"a",v:1},{_id:"b",v:2},{_id:"c",v:3}])' >/dev/null \
  || die "写种子数据失败"
ok "种子数据 a/b/c 已写入 demo.items"

# ---------- 3. 编译 + 启动 ----------
info "编译 ETLer..."
(cd "$REPO" && go build -o "$RUN/etler" ./main) || die "go build 失败"
ok "编译完成: $RUN/etler"

: > "$SINKLOG"; : > "$RUN/etler.log"; : > "$RUN/sink.log"

(cd "$RUN" && export SINK_LOG="$SINKLOG" SINK_FAIL_FILE="$RUN/sink.fail" \
  SINK_PORT="$SINK_PORT" && exec python3 "$WORK/sink.py" > "$RUN/sink.log" 2>&1) &
SINK_PID=$!
printf '%s\n' "$SINK_PID" > "$RUN/sink.pid"

(cd "$RUN" && exec ./etler > "$RUN/etler.log" 2>&1) &
ETLER_PID=$!
printf '%s\n' "$ETLER_PID" > "$RUN/etler.pid"

cleanup() {
  kill "$ETLER_PID" 2>/dev/null || true
  kill "$SINK_PID" 2>/dev/null || true
  wait "$ETLER_PID" 2>/dev/null || true
  wait "$SINK_PID" 2>/dev/null || true
  rm -f "$RUN/etler.pid" "$RUN/sink.pid" 2>/dev/null || true
}
trap cleanup EXIT

sleep 2
kill -0 "$SINK_PID" 2>/dev/null || die "sink.py 启动失败"
kill -0 "$ETLER_PID" 2>/dev/null || die "etler 启动失败"
info "服务已启动，等待 initWatching + 全量同步..."

wait_until() {  # wait_until <超时秒> <说明> <命令...>
  local timeout="$1" label="$2" i
  shift 2
  for ((i=1; i<=timeout; i++)); do
    if "$@" >/dev/null 2>&1; then return 0; fi
    sleep 1
  done
  printf '%s\n' "[e2e] 等待超时: $label" >&2
  return 1
}

# ---------- 4. 等全量同步 ----------
wait_until 90 "Sync 全量同步请求" \
  python3 "$CHECK" --wait-sync "$SINKLOG" || die "90s 内没收到 Sync 全量同步请求"
ok "收到 Sync 全量同步"

# ---------- 5. 就绪探针 ----------
# 【关键】sink 收到 T=0 只说明全量同步跑完，并不说明 watcher 已退出
# initWatching / 已建立 watch()。必须等到 sink 收到一条真实的 T=1 事件，
# 才能确认 change stream 链路已通；在此之前写入的业务事件会被吞掉。
# 探针用不同 _id 低频重试，给 initWatching 留出结束窗口。
PROBE_N=0
PROBE_READY=0
for ((p=1; p<=20; p++)); do
  PROBE_ID="__e2e_probe_${p}__"
  PROBE_N=$p
  info "就绪探针 #$p: $PROBE_ID"
  mongo_eval "db.getSiblingDB(\"demo\").items.insertOne({_id:\"$PROBE_ID\",v:0})" >/dev/null \
    || die "写就绪探针失败"

  if wait_until 4 "探针 #$p 到达 sink" \
       python3 "$CHECK" --wait-event-id "$PROBE_ID" "$SINKLOG"; then
    ok "探针 #$p 已到达 sink —— change stream 链路就绪"
    PROBE_READY=1
    break
  fi
  # 进程提前退出就不用继续试了
  kill -0 "$ETLER_PID" 2>/dev/null || die "etler 在探针阶段退出"
done

if [ "$PROBE_READY" -ne 1 ]; then
  die "20 次就绪探针都没到达 sink：watcher 未进入 watch()（见上面的 etler.log / currentOp / run 目录树）"
fi

# ---------- 6. 正式增量 ----------
mongo_eval '
  var d=db.getSiblingDB("demo");
  d.items.insertOne({_id:"d",v:4});
  d.items.updateOne({_id:"b"},{$set:{v:99}});
  d.items.deleteOne({_id:"c"});
  d.other.insertOne({_id:"x",v:1});
' >/dev/null || die "触发增量写入失败"
info "已触发 insert(d) / update(b) / delete(c) / 非监听集合 other.insert(x)"

# 应到事件数 = 已到达的探针数(PROBE_N 中最多 1 条会真正到达，这里用实际计数) + 3
PROBE_ARRIVED="$(python3 "$CHECK" --count-probes "$SINKLOG")"
NEED=$((PROBE_ARRIVED + 3))
info "等待增量事件（总 T=1 事件数 >= $NEED，其中探针 $PROBE_ARRIVED 条）"
wait_until 60 "增量事件到齐 (>= $NEED)" \
  python3 "$CHECK" --wait-events "$NEED" "$SINKLOG" || die "60s 内增量事件没到齐"

python3 "$CHECK" "$SINKLOG" || die "主闭环判定未通过"

# ---------- 7. 可选：故障重试 + 重启续传 ----------
if [ "$FULL" -eq 1 ]; then
  info "=== --full：下游故障重试 ==="
  touch "$RUN/sink.fail"
  mongo_eval 'db.getSiblingDB("demo").items.insertOne({_id:"e",v:5})' >/dev/null
  sleep 5
  rm -f "$RUN/sink.fail"
  info "下游已恢复，等待自动续发..."
  wait_until 45 "故障恢复后 e 事件送达" \
    python3 "$CHECK" --wait-event-id e "$SINKLOG" || die "下游恢复后 e 事件没有送达"
  python3 "$CHECK" --check-fail-seen "$SINKLOG" \
    || die "故障注入期间没有出现 500"
  ok "故障重试通过"

  info "=== --full：停服重启续传 ==="
  kill "$ETLER_PID" 2>/dev/null || true
  wait "$ETLER_PID" 2>/dev/null || true
  mongo_eval 'db.getSiblingDB("demo").items.insertOne({_id:"f",v:6})' >/dev/null
  sleep 1
  (cd "$RUN" && exec ./etler >> "$RUN/etler.log" 2>&1) &
  ETLER_PID=$!
  printf '%s\n' "$ETLER_PID" > "$RUN/etler.pid"

  wait_until 60 "重启后 f 事件送达" \
    python3 "$CHECK" --wait-event-id f "$SINKLOG" || die "重启后 f 事件没有送达"
  python3 "$CHECK" --assert-sync-once "$SINKLOG" \
    || die "重启后重复做了全量同步"
  ok "重启续传通过"
fi

# ---------- 8. 结果 ----------
printf '\n%s\n' "==================== etler.log 关键行 ===================="
grep -E "init watching|SyncTask|send:|saved resumeToken|ChangeStream end|succeed|will watch" "$RUN/etler.log" | tail -30 || true
printf '%s\n' "==================== sink.log ===================="
cat "$RUN/sink.log" || true
printf '\n'
ok "全部通过 ✅"
printf '%s\n' "  运行产物目录: $RUN"
printf '%s\n' "  原始报文: $SINKLOG"
printf '%s\n' "  Mongo 容器: $MONGO_CT （重跑脚本会自动重建）"