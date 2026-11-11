# 由 mongo:7 官方 entrypoint 在 initdb 阶段 source（不是执行，644 即可）。
# 此时临时 mongod 尚未带 --keyFile，生成的密钥会在 entrypoint 重启正式 mongod 后生效。
# E2E_KEY_B64 由 e2etest.sh 通过 -e 传入容器。
#
# 关键：MongoDB 要求 keyFile 内容是【base64 字符集中的文本】，长度 6~1024 个字符，
# 不能是解码后的二进制（二进制里含非 base64 字符，正式 mongod 会拒绝启动）。
# 等价于官方推荐的 openssl rand -base64 756 > keyfile && chmod 400 keyfile。
if [ -z "${E2E_KEY_B64:-}" ]; then
  echo "[e2e] E2E_KEY_B64 未传入容器，无法生成 keyFile" >&2
  exit 1
fi
if [ "${#E2E_KEY_B64}" -lt 6 ] || [ "${#E2E_KEY_B64}" -gt 1024 ]; then
  echo "[e2e] E2E_KEY_B64 长度 ${#E2E_KEY_B64} 不在 6~1024 范围内" >&2
  exit 1
fi
printf '%s' "$E2E_KEY_B64" > /tmp/mongo.key
chmod 400 /tmp/mongo.key