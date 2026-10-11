"""DAG ① moomoo_brief —— 每天 06:30（+08:00）跑常规简报 + 多周期回滚。

对应用户原来的 **Windows 计划任务「moomoo-watch 简报+回滚」**（`run_brief.ps1 -Rollup`）。
把它从一条「线性脚本」改写成一张**有依赖的图**：取数、日历、宏观可以并行，
出稿要等数据包齐了，回滚要等简报出完（**同进程内串行**，躲 outbox mtime 竞态）。

依赖图：

    preflight ─┬─ collect ─ enrich ─┬─ news ─────────┐
               ├─ calendar ─────────┼────────────────┤
               └─ macro ────────────┴─ packet ─ summarize_brief ─ rollup ─ push

⚠️ 为什么 06:30：此刻**上一美股交易日已收盘**（美股 16:00 ET ≈ 次日 04:00–05:00 +08:00），
   距当晚 21:30 开盘还有约 15h —— 唯一「数据完整 + 时间充裕」的窗口。
"""

from __future__ import annotations

import pendulum
from airflow import DAG
from airflow.operators.python import PythonOperator

from lib import stages

# 串行池：写共享产物（data/latest.json、outbox/、state.json）的任务必须互斥
# （slots=1，在 airflow-init 里创建）。两个 DAG 用**同一个池** → 跨 DAG 也互斥。
POOL = "moomoo_serial"

with DAG(
    dag_id="moomoo_brief",
    description="moomoo-watch 每日简报 + 多周期回滚（离线 fixture 版）",
    schedule="30 6 * * *",
    start_date=pendulum.datetime(2026, 9, 1, tz="Asia/Shanghai"),
    catchup=False,
    max_active_runs=1,
    default_args={"owner": "moomoo-watch", "retries": 0, "depends_on_past": False},
    tags=["moomoo", "fixture", "brief"],
    doc_md=__doc__,
) as dag:
    preflight = PythonOperator(task_id="preflight",
                               python_callable=stages.as_callable(stages.t_preflight))

    collect = PythonOperator(task_id="collect",
                             python_callable=stages.as_callable(stages.t_collect))
    enrich = PythonOperator(task_id="enrich",
                            python_callable=stages.as_callable(stages.t_enrich))
    calendar = PythonOperator(task_id="calendar",
                              python_callable=stages.as_callable(stages.t_calendar,
                                                                 requested_trigger="brief"))
    news = PythonOperator(task_id="news",
                          python_callable=stages.as_callable(stages.t_news))
    macro = PythonOperator(task_id="macro",
                           python_callable=stages.as_callable(stages.t_macro))
    packet = PythonOperator(task_id="packet", pool=POOL,      # 写共享的 data/latest.json → 进池
                            python_callable=stages.as_callable(stages.t_packet))

    summarize_brief = PythonOperator(
        task_id="summarize_brief", pool=POOL,
        python_callable=stages.as_callable(stages.t_summarize_brief_gated))

    rollup = PythonOperator(
        task_id="rollup", pool=POOL,
        python_callable=stages.as_callable(stages.t_rollup))

    push = PythonOperator(
        task_id="push", pool=POOL,
        python_callable=stages.as_callable(stages.t_push))

    # --- 依赖 ---------------------------------------------------------------
    preflight >> [collect, calendar, macro]
    collect >> enrich
    enrich >> news
    [enrich, calendar, news, macro] >> packet
    packet >> summarize_brief
    summarize_brief >> rollup          # 简报先出，再出各周期报告（同池串行）
    [summarize_brief, rollup] >> push
