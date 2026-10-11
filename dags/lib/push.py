"""推送闸门 —— 逐字复刻 `main.py` 的 `_push_draft()`（84-108）。

单独成文件的两个理由：
1. **是承重逻辑**：`state.json` 是读-改-写，两个 task 并发跑会**丢记录** → 所以
   调用它的 `t_push` 必须挂 `pool="moomoo_serial"`（slots=1）+ DAG `max_active_runs=1`。
2. 便于对着 `main.py` 逐行核对（parity）。

⚠️ 所有失败只记日志、**绝不抛** —— 与 main.py 一致：推送失败不该让整条管道变红。
"""

from __future__ import annotations

import logging
from typing import Any, Mapping

log = logging.getLogger("moomoo-watch.airflow")


def push_draft(cfg, draft: Mapping[str, Any]) -> bool:
    """把一份推送稿过闸门并发送。返回是否真发成功。所有失败只记日志、不抛。

    与 `main.py:_push_draft` 的唯一差别：去掉了 `--no-push` 分支（DAG 里没有这个开关；
    要「不推送」直接不跑 `t_push` 就行）。
    """
    from core import notifier
    from core.state import StateManager

    if not draft.get("send"):
        log.info("子代理判定「没重要事」→ 不推送（合格产出）。")
        return False

    channel = notifier.resolve_channel(cfg, draft["priority"])
    state = StateManager(cfg.path_for("state"))
    allowed, reason = state.can_push(draft["title"], draft["priority"], cfg, channel)
    if not allowed:
        log.warning("拦截推送：%s", reason)
        return False
    log.info("推送闸门：%s（priority=%s → 渠道 %s，该渠道今日已用 %d 条）",
             reason, draft["priority"], channel, state.daily_count(channel))

    res = notifier.send(cfg, draft["title"], draft["body"], draft["priority"])
    state.record_push(draft["title"], draft["priority"],
                      str(res.get("channel") or channel), bool(res.get("ok")),
                      simulated=bool(res.get("simulated")))
    if res.get("ok"):
        log.info("推送成功%s", "（simulate，未真发）" if res.get("simulated") else "")
    else:
        log.error("推送失败（不影响主流程）：%s", res.get("error"))
    return bool(res.get("ok"))


def push_all(cfg, drafts) -> int:
    """按顺序推一批稿，返回真发成功条数。"""
    return sum(1 for d in drafts if push_draft(cfg, d))
