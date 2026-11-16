#!/usr/bin/env python3
"""扫描 shell 脚本里「裸 $VAR 紧贴非 ASCII 字符」的写法。

macOS 自带 bash 3.2 在 UTF-8 locale 下会把高位字节当合法变量名字符，
所以 info "...$NEED，其中..." 会被展开成 $NEED<乱码>，set -u 下直接
报 `NEED\\ufffd: unbound variable`。改成 ${NEED} 即可。

用法：
  python3 check_vars.py e2etest.sh          # 检查一个文件
  python3 check_vars.py *.sh                # 检查多个

退出码：0 = 无问题；1 = 发现问题；2 = 用法/读文件错误。
"""
import re
import sys

# 只抓「$ + 标识符 + 紧跟非 ASCII」；${VAR} / $(...) / $VAR 后跟空格都放过
PAT = re.compile(r"\$[A-Za-z_]\w*[^\x00-\x7f]")


def main(argv):
    if len(argv) < 2:
        print("用法: python3 check_vars.py <shell 脚本> [更多脚本 ...]")
        return 2

    total = 0
    for path in argv[1:]:
        try:
            with open(path, encoding="utf-8") as f:
                lines = f.readlines()
        except OSError as e:
            print(f"读取失败 {path}: {e}")
            return 2

        bad = [(i, l.rstrip("\n")) for i, l in enumerate(lines, 1)
               if PAT.search(l)]
        if bad:
            print(f"--- {path} 发现 {len(bad)} 处裸 $VAR 紧贴中文字符 ---")
            for i, l in bad:
                print(f"  {i}: {l}")
            print("    改法：把 $VAR 写成 ${VAR}，用 } 划断变量名边界")
            total += len(bad)
        else:
            print(f"OK  {path}：无裸 $VAR 紧贴中文字符")

    if total:
        print(f"共 {total} 处需改成 ${{VAR}}")
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))