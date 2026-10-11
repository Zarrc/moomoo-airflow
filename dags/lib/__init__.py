"""airflow/dags/lib —— DAG 与 core 之间的**轻适配层**。

为什么单独一层（而不是直接写进 DAG 文件）：
- DAG 文件在 **parse 期**（调度器每 ~30s 扫一遍）就会被 import —— 那里面**不能**
  import core（拖慢扫描，且 core 出问题会让整个 DAG 解析失败）。所以 core 的 import
  一律**写进各 stage 的函数体**里。
- 适配层不依赖 airflow（`as_callable` 惰性导入 airflow）→ **可脱离 Airflow 冒烟测试**
  （见 airflow/README.md「无 Docker 冒烟自检」）。

一行 core / main.py 都不改：所有路径重定向都靠给 `cfg.raw` 赋绝对路径实现，
详见 lib/env.py 顶部三条不变量。
"""

from __future__ import annotations

from lib import env as _env

# 🔒 导入本包即把**仓库根**塞进 sys.path —— 这是 `from core import ...` 能工作的前提。
#
# 为什么放在这里（而不是只靠各 stage 自己调）：踩过一次 —— 在 stage 顶部写
# `from core import market`（早于 `env.cfg_for()`），于是 `core` 由 CWD 解析：
# 本地跑时 CWD 是**真仓库**（看着还「能跑」，其实读错了 fixture、还会把命中率
# 写进真 vault）；容器里 CWD 是 /opt/airflow（没有 core）→ 直接 ImportError。
# 两类症状都很难查，所以用「导入即 bootstrap + stage 内 cfg 先行」两道保险。
_env.bootstrap()
