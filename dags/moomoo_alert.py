"""DAG ② moomoo_alert —— 每 30 分钟跑一次高危事件预警。

对应用户原来的 **Windows 计划任务「moomoo-watch 预警」**（`run_brief.ps1 -Trigger alert`）。

与 DAG ① 的三点不同：
1. `requested_trigger="alert"` —— **不**做 brief→alert 的自动切换（本来就是 alert）。
2. **没有 `rollup`** —— 🚫 `-Rollup` 绝不挂 30 分钟任务（`SETUP.md:131` / `README.md:220`）。
3. 多了一个 `gate`（`ShortCircuitOperator`）：预警窗口里没有高险事件就**短路掉整条链路**。

⚠️ `gate` 是**唯一一处刻意的行为增强**：`main.py` 的 alert 模式总是跑完子代理、靠
   `send:false` 不发 —— 30 分钟频率下这是纯浪费。由 `airflow.shortcircuit_alert` 开关
   控制（容器 demo 默认开）；设成 false 即回到与 `main.py` 完全一致的行为。
"""

from __future__ import annotations

import pendulum
from airflow import DAG
from airflow.operators.python import PythonOperator, ShortCircuitOperator

from lib import stages

POOL = "moomoo_serial"      # 与 DAG ① 同一个池 → 跨 DAG 的共享写者（data/outbox/state）也互斥

with DAG(
    dag_id="moomoo_alert",
    description="moomoo-watch 高危事件预警（每 30 分钟；离线 fixture 版）",
    schedule="*/30 * * * *",
    start_date=pendulum.datetime(2026, 9, 1, tz="Asia/Shanghai"),
    catchup=False,
    max_active_runs=1,
    default_args={"owner": "moomoo-watch", "retries": 0, "depends_on_past": False},
    tags=["moomoo", "fixture", "alert"],
    doc_md=__doc__,
) as dag:
    preflight = PythonOperator(task_id="preflight",
                               python_callable=stages.as_callable(stages.t_preflight))
    calendar = PythonOperator(task_id="calendar",
                              python_callable=stages.as_callable(stages.t_calendar,
                                                                 requested_trigger="alert"))
    # 读 calendar.json：窗口内没有高险事件 → 之后全部 skip（UI 里显示为 skipped）
    gate = ShortCircuitOperator(task_id="gate",
                                python_callable=stages.as_callable(stages.gate_should_run))

    collect = PythonOperator(task_id="collect",
                             python_callable=stages.as_callable(stages.t_collect))
    enrich = PythonOperator(task_id="enrich",
                            python_callable=stages.as_callable(stages.t_enrich))
    news = PythonOperator(task_id="news",
                          python_callable=stages.as_callable(stages.t_news))
    macro = PythonOperator(task_id="macro",
                           python_callable=stages.as_callable(stages.t_macro))
    packet = PythonOperator(task_id="packet", pool=POOL,      # 写共享的 data/latest.json → 进池
                            python_callable=stages.as_callable(stages.t_packet))

    summarize_alert = PythonOperator(
        task_id="summarize_alert", pool=POOL,
        python_callable=stages.as_callable(stages.t_summarize_brief_gated,
                                           requested_trigger="alert"))

    push = PythonOperator(
        task_id="push", pool=POOL,
        python_callable=stages.as_callable(stages.t_push))

    # --- 依赖 ---------------------------------------------------------------
    preflight >> calendar >> gate                       # 先算日历，再由 gate 决定放不放行
    gate >> [collect, macro]
    collect >> enrich
    enrich >> news
    [enrich, news, macro] >> packet
    packet >> summarize_alert
    summarize_alert >> push
