#!/usr/bin/env bash
# =============================================================================
# 运行：bash e2etest.sh --scenarios（主闭环 + --full 之后追加执行）
#
# 默认不影响 `bash e2etest.sh` 与 `bash e2etest.sh --full` 的任何行为。
# 所有产物落在 $RUN/，无隐藏文件。
#
# 中文文案里的变量一律写 ${VAR}（macOS bash 3.2 会吞掉 $VAR 后的多字节字符）。
# =============================================================================

API_BASE="http://127.0.0.1:${API_PORT}/api"

api_post() {  # api_post <路径> <json 字符串> [输出文件]
  local path="$1" body="$2" out="${3:-/dev/null}"
  curl -s -XPOST "${API_BASE}/${path}" -d "$body" > "$out" || return 1
}

json_get() {  # json_get <json 字符串> <python 表达式>
  # 解析失败必须显式失败：旧版静默返回空串，调用处会拿着空值做错误断言
  printf '%s' "${1}" | python3 -c "import json,sys; d=json.load(sys.stdin); print(${2})" 2>/dev/null || {
    printf '%s\n' "[e2e] json_get 解析失败，响应: ${1}" >&2
    return 1
  }
}

# 事件流排空：连续 3 次采样（间隔 1s）T=1 事件总数不变，才认为积压已投完。
# s_large_sync 插入 1200 条后会产生 1200 个 change stream 事件；若不等排空，
# 后续场景的 before/target 取到的是中间快照，判定会变成假 PASS，
# 且 watcher 正忙于消费积压 batch，配置变更的 cancel 会被延迟近 20s 才生效。
wait_events_stable() {
  local last="-1" cur same=0 i
  for ((i=1; i<=120; i++)); do
    cur="$(python3 "$CHECK" --count-events "$SINKLOG" 2>/dev/null || printf '%s' "-1")"
    if [ "$cur" != "-1" ] && [ "$cur" = "$last" ]; then
      same=$((same + 1))
      if [ "$same" -ge 3 ]; then
        ok "事件流已排空（T=1 事件数稳定在 ${cur}）"
        return 0
      fi
    else
      same=0
      last="$cur"
    fi
    sleep 1
  done
  return 1
}

# ---------------------------------------------------------------- E9
s_getwatchcols() {
  info "=== E9: GetWatchCols 版本回读 ==="
  local res ver
  res="$(curl -s -XPOST "${API_BASE}/GetWatchCols" -d '{}')"
  [ -n "$res" ] || die "GetWatchCols 无响应"
  ver="$(json_get "$res" 'd.get("version", d.get("Version", -1))')" || die "JSON 响应解析失败: ${res}"
  [ "${ver}" != "-1" ] || die "GetWatchCols 响应缺 version: ${res}"
  printf '%s\n' "$res" > "$RUN/getwatchcols.res"
  ok "GetWatchCols 正常（version=${ver}）"
}

# ---------------------------------------------------------------- E6
s_forcefullsync() {
  info "=== E6: ForceFullSync API ==="
  local before
  before="$(python3 "$CHECK" --count-sync "$SINKLOG")"
  api_post "ForceFullSync" '{}' || die "ForceFullSync 请求失败"
  wait_until 40 "ForceFullSync 触发新一次 Sync（>= $((before+1))）" \
    python3 "$CHECK" --count-sync-ge "$((before+1))" "$SINKLOG" \
    || die "ForceFullSync 没有触发新的全量同步"
  ok "ForceFullSync 通过（Sync 次数已增加）"
}

# ---------------------------------------------------------------- E7
s_forcesynccoll() {
  info "=== E7: ForceSyncColl API（指定子集） ==="
  local before res succeed
  before="$(python3 "$CHECK" --count-sync "$SINKLOG")"
  res="$(curl -s -XPOST "${API_BASE}/ForceSyncColl" \
    -d '{"colls":[{"DB":"demo","Collection":"items"}]}')"
  printf '%s\n' "$res" > "$RUN/forcesynccoll.res"
  succeed="$(json_get "$res" 'd.get("Succeed", d.get("succeed", False))')" || die "JSON 响应解析失败: ${res}"
  [ "${succeed}" = "True" ] || die "ForceSyncColl 未受理: ${res}"
  wait_until 40 "ForceSyncColl 触发新一次 Sync（>= $((before+1))）" \
    python3 "$CHECK" --count-sync-ge "$((before+1))" "$SINKLOG" \
    || die "ForceSyncColl 没有触发全量同步"
  ok "ForceSyncColl 通过"
}

# ---------------------------------------------------------------- E8
s_backupdb() {
  info "=== E8: BackupDB API ==="
  local res fn
  res="$(curl -s -XPOST "${API_BASE}/BackupDB" -d '{}')"
  printf '%s\n' "$res" > "$RUN/backupdb.res"
  fn="$(json_get "$res" 'd.get("file_name", d.get("FileName", ""))')" || die "JSON 响应解析失败: ${res}"
  [ -n "${fn}" ] || die "BackupDB 返回空 file_name: ${res}"
  # etler 在 ${RUN} 里运行，exe.AbsDir = ${RUN}，file_name 是相对该目录的路径
  [ -f "$RUN/${fn}" ] || die "备份文件不存在: $RUN/${fn}"
  ok "BackupDB 通过（${fn}）"
}

# ---------------------------------------------------------------- E11
s_full_document() {
  info "=== E11: update 事件的 fullDocument 内容 ==="
  python3 "$CHECK" --check-full-document b v 99 "$SINKLOG" \
    || die "update(b) 的 fullDocument.v 不是 99（UpdateLookup 未生效？）"
  ok "fullDocument.v == 99"
}

# ---------------------------------------------------------------- E14
s_other_filtered() {
  info "=== E14: 非监听集合 demo.other 全程不投递 ==="
  if python3 "$CHECK" --wait-event-id x "$SINKLOG"; then
    die "非监听集合 demo.other 的 insert(x) 被误投递了"
  fi
  ok "demo.other 未投递"
}

# ---------------------------------------------------------------- E12（慢）
s_large_sync() {
  info "=== E12: 大批量（> batch=1000）分页 Sync ==="
  local before target
  before="$(python3 "$CHECK" --count-docs 0 "$SINKLOG")"
  target=$((before + 1200))

  mongo_eval '
    var d = db.getSiblingDB("demo");
    var bulk = [];
    for (var i = 0; i < 1200; i++) { bulk.push({_id:"bulk_"+i, v:i}); }
    d.items.insertMany(bulk, {ordered:true});
  ' >/dev/null || die "批量写入 1200 条失败"

  api_post "ForceFullSync" '{}' || die "ForceFullSync 请求失败"
  wait_until 90 "大批量 Sync 文档数达到 ${target}" \
    python3 "$CHECK" --count-docs-ge 0 "${target}" "$SINKLOG" \
    || die "分页 Sync 没有把 1200 条全部送达"
  ok "大批量分页 Sync 通过（T=0 文档累计 >= ${target}）"
}

# ---------------------------------------------------------------- E13
# v1 用「总事件数 >= before+20」判定；在 1200 条 bulk 事件仍在投递时，
# before 是中间快照，后台持续增长就能满足 target，属于假 PASS。
# v2 改为逐个判 20 个具体 _id，依赖上面的 wait_events_stable 排除积压干扰。
s_concurrent_writes() {
  info "=== E13: 批量写入的事件完整性 ==="
  mongo_eval '
    var d = db.getSiblingDB("demo");
    var a = [];
    for (var i = 0; i < 20; i++) { a.push({_id:"par_"+i, v:i}); }
    d.items.insertMany(a, {ordered:false});
  ' >/dev/null || die "批量写入 20 条失败"

  local i id
  for ((i=0; i<20; i++)); do
    id="par_${i}"
    wait_until 60 "批量写入的 ${id} 事件送达" \
      python3 "$CHECK" --wait-event-id "${id}" "$SINKLOG" \
      || die "批量写入的 ${id} 没有投递"
  done
  ok "批量写入 20 条事件全部到达"
}

# ---------------------------------------------------------------- E4（P0 判定）
s_setwatchcols() {
  info "=== E4: SetWatchCols 动态新增监听集合（暴露 P0） ==="
  local cur next res now id

  # 【v2 关键】先给新集合预置一条数据。
  # sender.go:229 是 `if len(all) != 0 { Send }`，空集合的 delta sync 只会
  # 打印 "SyncTask:demo.items2 send..." 后直接 Del，不产生任何 /etl 请求。
  # v1 因此硬等 --wait-sync-ns demo items2 必然超时，且 dyn_z 从未被写入。
  # 预置发生在 items2 成为监听集合之前：watcher 只会保存 resumeToken、不投递，
  # 不会污染后面的 change stream 判定。
  mongo_eval 'db.getSiblingDB("demo").items2.insertOne({_id:"dyn_preseed",v:0})' >/dev/null \
    || die "预置 items2 数据失败"

  # 覆盖：items2 尚未成为监听集合时，watcher 只保存 resumeToken，不投递
  sleep 1
  if python3 "$CHECK" --wait-event-id dyn_preseed "$SINKLOG"; then
    die "dyn_preseed 在 items2 成为监听集合之前被投递了"
  fi

  res="$(curl -s -XPOST "${API_BASE}/GetWatchCols" -d '{}')"
  [ -n "${res}" ] || die "GetWatchCols 无响应"
  cur="$(json_get "$res" 'd.get("version", d.get("Version", 0))')" || die "JSON 响应解析失败: ${res}"
  next=$((cur + 1))

  api_post "SetWatchCols" \
    "{\"version\":${next},\"watchInfos\":[{\"DB\":\"demo\",\"Collection\":\"items\"},{\"DB\":\"demo\",\"Collection\":\"items2\"}]}" \
    "$RUN/setwatchcols.res" || die "SetWatchCols 请求失败"

  # api_post 只校验 curl 退出码，不校验业务结果；补一次 nowver 回读
  res="$(cat "$RUN/setwatchcols.res")"
  now="$(json_get "$res" 'd.get("nowver", d.get("NowVersion", -1))')" || die "JSON 响应解析失败: ${res}"
  [ "${now}" = "${next}" ] \
    || die "SetWatchCols 未生效: ${res}（期望 nowver=${next}）"
  ok "SetWatchCols 已提交（version ${cur} -> ${now}）"

  # task 侧 deltaSyncTaskify 对 items2 做全量 Sync；因为已有 dyn_preseed，
  # 这次会真的发出 Ns={"DB":"demo","Coll":"items2"} 的 T=0 请求
  wait_until 40 "demo.items2 的全量 Sync 到达" \
    python3 "$CHECK" --wait-sync-ns demo items2 "$SINKLOG" \
    || die "新增集合没有触发全量同步（scheduler 侧 WatchCollectionUpdated 未生效）"
  ok "demo.items2 的全量 Sync 已到达"

  # P0 判定：配置变更后 watcher 必须能重新建流并投递新集合的事件。
  # 若 watcher 被 cancel 后卡在 for 循环里每秒刷 "stream err, retry server"，
  # dyn_z 永远不会到 sink → 这一步超时 FAIL。
  id="dyn_z"
  mongo_eval 'db.getSiblingDB("demo").items2.insertOne({_id:"dyn_z",v:1})' >/dev/null \
    || die "写 dyn_z 失败"
  wait_until 40 "新增集合的 dyn_z 事件送达" \
    python3 "$CHECK" --wait-event-id "${id}" "$SINKLOG" \
    || die "新增集合的事件没有送达：watcher 在 WatchCollectionUpdated 后未恢复（P0）"

  # 版本应已回读为新值
  res="$(curl -s -XPOST "${API_BASE}/GetWatchCols" -d '{}')"
  printf '%s\n' "$res" > "$RUN/getwatchcols.after.res"
  ok "SetWatchCols 通过（dyn_z 已投递，配置变更即时生效）"
}

# ---------------------------------------------------------------- E10
# 注意：sink.py 启动时 open(LOG,"w") 会清空 sink.jsonl，所以本场景只判定
# 「恢复后新写入的 sinkdown_1」，不依赖历史日志；必须放在所有需要历史日志的场景之后。
s_sink_down() {
  info "=== E10: 下游进程不可达（连接拒绝，非 500） -> 恢复 ==="
  kill "$SINK_PID" 2>/dev/null || true
  wait "$SINK_PID" 2>/dev/null || true

  mongo_eval 'db.getSiblingDB("demo").items.insertOne({_id:"sinkdown_1",v:1})' >/dev/null \
    || die "写 sinkdown_1 失败"
  sleep 2

  (cd "$RUN" && export SINK_LOG="$SINKLOG" SINK_FAIL_FILE="$RUN/sink.fail" \
    SINK_PORT="$SINK_PORT" && exec python3 "$WORK/sink.py" >> "$RUN/sink.log" 2>&1) &
  SINK_PID=$!
  printf '%s\n' "$SINK_PID" > "$RUN/sink.pid"

  kill -0 "$SINK_PID" 2>/dev/null || die "sink.py 重启失败（看 sink.log）"
  # retry = 15s，给足 60s
  wait_until 60 "下游恢复后 sinkdown_1 送达" \
    python3 "$CHECK" --wait-event-id sinkdown_1 "$SINKLOG" \
    || die "下游恢复后事件没有续发（SendFailed 重试路径异常）"
  ok "下游不可达 -> 恢复续发通过"
}

# ---------------------------------------------------------------- E5（必须最后）
# 不只是查 API 响应：清空之后重新 SetWatchCols 恢复配置，再验证 watcher 能重新投递。
# 修 P0 之前，ClearWC 触发的 cancel() 会让 watcher 永久卡在 for 循环里，
# recover_1 永远不会到 sink → 本场景超时 FAIL。
s_clearwc() {
  info "=== E5: ClearWC 清空监听集合 + 恢复配置 ==="
  local res cnt cur next now
  api_post "ClearWC" '{}' "$RUN/clearwc.res" || die "ClearWC 请求失败"
  res="$(curl -s -XPOST "${API_BASE}/GetWatchCols" -d '{}')"
  [ -n "${res}" ] || die "ClearWC 后 GetWatchCols 无响应"
  printf '%s\n' "$res" > "$RUN/getwatchcols.cleared.res"
  # 【v3 根因】ClearWC 删掉整个 bucket 并重建，LatestVersion=0、Get 返回 nil slice，
  # Go 把 nil slice 编码成 JSON null：响应是 {"version":0,"watchInfos":null}。
  # v2 的 len(d.get("watchInfos", ...)) 因键存在而取到 None → TypeError →
  # 命令替换得到空串 → 误判 "仍有监听集合" 并立即 die。
  printf '%s' "${res}" | python3 -c 'import json,sys; d=json.load(sys.stdin); v=d.get("watchInfos"); assert v is None or isinstance(v,list), "watchInfos 既不是 null 也不是数组"' \
    || die "GetWatchCols 响应结构异常: ${res}"
  cnt="$(json_get "$res" 'len(d.get("watchInfos") or d.get("WatchInfos") or [])')" || die "JSON 响应解析失败: ${res}"
  [ -n "${cnt}" ] || die "GetWatchCols 响应解析为空: ${res}"
  [ "${cnt}" = "0" ] || die "ClearWC 后仍有 ${cnt} 个监听集合: ${res}"
  ok "ClearWC 已清空监听集合"

  cur="$(json_get "$res" 'd.get("version", d.get("Version", 0))')" || die "JSON 响应解析失败: ${res}"
  next=$((cur + 1))
  api_post "SetWatchCols" \
    "{\"version\":${next},\"watchInfos\":[{\"DB\":\"demo\",\"Collection\":\"items\"},{\"DB\":\"demo\",\"Collection\":\"items2\"}]}" \
    "$RUN/setwatchcols.recover.res" || die "恢复配置请求失败"

  res="$(cat "$RUN/setwatchcols.recover.res")"
  now="$(json_get "$res" 'd.get("nowver", d.get("NowVersion", -1))')" || die "JSON 响应解析失败: ${res}"
  [ "${now}" = "${next}" ] \
    || die "恢复配置未生效: ${res}（期望 nowver=${next}）"

  # 覆盖：恢复配置后两个集合都要真的回到监听列表（不只信 SetWatchCols 的响应）
  res="$(curl -s -XPOST "${API_BASE}/GetWatchCols" -d '{}')"
  [ -n "${res}" ] || die "恢复后 GetWatchCols 无响应"
  printf '%s\n' "${res}" > "$RUN/getwatchcols.recovered.res"
  cnt="$(json_get "$res" 'len(d.get("watchInfos") or d.get("WatchInfos") or [])')" || die "JSON 响应解析失败: ${res}"
  [ "${cnt}" = "2" ] || die "恢复配置后监听集合数 = ${cnt}, want 2: ${res}"

  mongo_eval 'db.getSiblingDB("demo").items.insertOne({_id:"recover_1",v:1})' >/dev/null \
    || die "写 recover_1 失败"
  wait_until 40 "恢复配置后 recover_1 送达" \
    python3 "$CHECK" --wait-event-id recover_1 "$SINKLOG" \
    || die "清空并恢复配置后 watcher 仍未恢复投递（P0）"
  ok "ClearWC 通过（恢复配置后事件重新投递）"
}

# ---------------------------------------------------------------- 入口
run_scenarios() {
  command -v curl >/dev/null 2>&1 || die "场景需要 curl，但当前 PATH 里没有"
  [ -f "$WORK/scenarios.sh" ] || die "缺固定文件: $WORK/scenarios.sh"

  info "========== 开始扩展场景（--scenarios） =========="
  s_getwatchcols
  s_forcefullsync
  s_forcesynccoll
  s_backupdb
  s_full_document
  s_other_filtered
  s_large_sync
  # 1200 条 bulk 的 change stream 事件需要约 20s 才投完；排空后再进入
  # 后续场景，避免积压干扰计数、也避免配置变更的 cancel 被延迟生效。
  wait_events_stable \
    || die "1200 条 bulk 事件在 120s 内没有投递完（事件流未排空）"
  s_concurrent_writes
  s_setwatchcols        # P0 判定：修之前会在 dyn_z 超时 FAIL
  s_sink_down
  s_clearwc             # 必须最后：清空监听后 watcher 不再投递任何事件
  info "========== 扩展场景全部完成 =========="
}
