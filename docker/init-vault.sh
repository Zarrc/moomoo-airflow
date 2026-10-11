#!/usr/bin/env bash
# ============================================================================
# 建「合成 vault」—— 容器里所有路径契约的落点。
#
# core 的路径解析链（R1，**最经典的静默坑**）：
#   find_vault_root() 从 core/config.py 往上找含 `.claude` 的目录 = vault 根
#   `summarize.run` 还要求 data/outbox/bundle 都在 vault 之下（.relative_to(vault_root)）
# 所以容器里必须手工造出一棵「像 vault 一样」的树：
#
#   /vault/                         ← 宿主 airflow/.sandbox/vault（RW）
#   ├── .claude/agents/portfolio-watch.md   ← 锚点：find_vault_root() 找的就是它
#   ├── Self/投资/{日评,周报,月报,季报,年报,预测台账}/   ← rollup 写命中率页的落点（沙箱）
#   ├── scratch/{config.yaml,.env,data,outbox,logs,rollup,interim}/
#   └── project/                    ← 宿主 <repo> 只读挂载
#
# 脚本结尾**自证**：把 find_vault_root() 与各条解析出的路径打出来
# （`docker compose logs airflow-init | grep find_vault_root` 就是看这个）。
# ============================================================================
set -euo pipefail

VAULT="${MOOMOO_VAULT:-/vault}"
PROJECT="${MOOMOO_PROJECT_ROOT:-/vault/project}"
CFG_DST="${MOOMOO_WATCH_CONFIG:-/vault/scratch/config.yaml}"
ENV_DST="${MOOMOO_WATCH_ENV:-/vault/scratch/.env}"

echo "[init-vault] 合成 vault：${VAULT}"

# 1) vault 根锚点 —— find_vault_root() 靠它
mkdir -p "${VAULT}/.claude/agents"
# 2) 人读报告的沙箱落点（rollup 的命中率页写进 Self/投资/预测台账/）
for d in 日评 周报 月报 季报 年报 预测台账; do
  mkdir -p "${VAULT}/Self/投资/${d}"
done
# 3) scratch：所有机器产物
for d in data outbox logs rollup interim; do
  mkdir -p "${VAULT}/scratch/${d}"
done

# 4) 子代理定义（锚点目录里也放一份 —— 将来切真 agent 时它就在位）
cp -f "${PROJECT}/agents/portfolio-watch.md" "${VAULT}/.claude/agents/portfolio-watch.md"

# 5) 容器专用配置：paths.* 本就是绝对路径（/vault/scratch/...），直接拷，无需改写
mkdir -p "$(dirname "${CFG_DST}")"
cp -f "${PROJECT}/airflow/config.airflow.yaml" "${CFG_DST}"
# 密钥文件：simulate 模式不需要任何密钥，留空即可（真实密钥**永不进仓库**）
: > "${ENV_DST}"

echo "[init-vault] 自证：解析出的 vault 根与各路径"
python - <<'PY'
import os, sys
sys.path.insert(0, os.environ.get("MOOMOO_PROJECT_ROOT", "/vault/project"))
from pathlib import Path
from core.config import Config, PROJECT_ROOT, find_vault_root

root = find_vault_root()
print("find_vault_root        ->", root)
print("PROJECT_ROOT           ->", PROJECT_ROOT)

cfg = Config(Path(os.environ.get("MOOMOO_WATCH_CONFIG", "/vault/scratch/config.yaml")))
for key in ("fixtures", "data", "outbox", "logs", "rollup", "state"):
    print(f"paths.{key:<16} ->", cfg.path_for(key))
print("paths.vault_invest     ->", cfg.vault_path_for("vault_invest"))

expected = os.environ.get("MOOMOO_VAULT", "/vault")
assert str(root) == expected, f"vault 根应解析为 {expected}，实际 {root} —— 检查 .claude 锚点与挂载"
print("[init-vault] OK：vault 根解析正确，所有路径都在沙箱之内")
PY
