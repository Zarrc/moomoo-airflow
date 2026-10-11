"""逐阶段调 core —— 把 `main.py` 那条线性流水线拆成 DAG 的 task。

**这就是「中等粒度 · 不动 core」的落点**：每个 `t_*` 只是
`main.py` 里对应几行的搬运 + 落一份中间产物 JSON，`core/**` 一行不改。

`main.py` 行号对照（改 core 后回来对一遍）：

| stage            | main.py        |
|------------------|----------------|
| t_preflight      | 114-117, 129, 152-161 |
| t_collect        | 164-165        |
| t_enrich         | 169            |
| t_calendar       | 172-178        |
| t_news           | 181-182        |
| t_macro          | 185            |
| t_packet         | 188-191        |
| t_summarize_brief| 200-208 / 224-232 |
| t_rollup         | 210-221        |
| t_push           | 84-108, 214-220, 232 |

⚠️ 每个 stage 的**函数体**里才 import core —— DAG parse 期（调度器每 ~30s 扫一遍）
   绝不 import core（拖慢扫描 / 一错全崩）。见 README §风险 R10。
⚠️ 而且 `from core import ...` 必须排在 `env.cfg_for(context)` **之后** ——
   `cfg_for` 会先 `bootstrap()`（把仓库根塞进 sys.path）。顺序反了就由 CWD 解析 core：
   本地会「看起来能跑」却读错目录（甚至写进真 vault），容器里直接 ImportError。
"""

from __future__ import annotations

import logging
from typing import Any, Callable, Mapping

from lib import env
from lib import io

log = logging.getLogger("moomoo-watch.airflow")


# ===========================================================================
# Airflow 胶水 —— 惰性导入，保证本模块可脱离 Airflow 冒烟测试
# ===========================================================================
def as_callable(fn: Callable[..., Any], **kwargs: Any) -> Callable[[], Any]:
    """把 `fn(context, **kwargs)` 包成**零参** callable 交给 `PythonOperator`。

    为什么零参：PythonOperator 到底会不会把 context 传进来，历史上变过好几版
    （`provide_context` → 自动探测）。**零参 + 内部 `get_current_context()`** 是
    唯一不依赖那套探测的写法（`get_current_context` 是 Airflow ≥2.1 的正式 API）。
    """
    def _run() -> Any:
        from airflow.operators.python import get_current_context   # 惰性
        return fn(get_current_context(), **kwargs)

    _run.__name__ = getattr(fn, "__name__", "callable")
    return _run


# ===========================================================================
# 0) 启动自检 —— 把静默失败变显式（R1）
# ===========================================================================
def t_preflight(context: Mapping[str, Any]) -> dict:
    cfg = env.cfg_for(context)          # ← 必须最先：bootstrap 把**仓库根**塞进 sys.path
    now = env.resolve_now(context)
    from core import notifier
    from core.config import find_vault_root

    vault = find_vault_root()          # ← 若这里就崩，说明挂载布局错了（好在**早崩**）
    info = {
        "now": now.isoformat(),
        "source": cfg.source,
        "mode": cfg.mode,
        "agent_enabled": bool(cfg.get("agent.enabled", True)),
        "shortcircuit_alert": bool(cfg.get("airflow.shortcircuit_alert", True)),
        "project_root": str(env.project_root()),
        "vault_root": str(vault),
        "paths.fixtures": str(cfg.path_for("fixtures")),
        "paths.data": str(cfg.path_for("data")),
        "paths.outbox": str(cfg.path_for("outbox")),
        "paths.rollup": str(cfg.path_for("rollup")),
        "paths.state": str(cfg.path_for("state")),
        "paths.vault_invest": str(cfg.vault_path_for("vault_invest")),
        "declared_channels": notifier.declared_channels(cfg),
    }
    log.info("=" * 62)
    log.info("preflight | source=%s mode=%s agent=%s", cfg.source, cfg.mode, info["agent_enabled"])
    for k, v in info.items():
        log.info("  %-22s = %s", k, v)
    if str(vault) != str(env.vault_root_path()):
        log.warning("vault 根（%s）与预期沙箱（%s）不一致 —— 检查挂载与 .claude 锚点",
                    vault, env.vault_root_path())

    # 启动自检（复刻 main.py:152-161）：live 才需要密钥；simulate 缺密钥只警告不拦
    if cfg.live_push:
        missing = cfg.missing_secrets()
        ready = [c for c in notifier.declared_channels(cfg) if notifier.channel_ready(cfg, c)]
        if not ready:
            log.error("live 模式：没有任何渠道配好密钥，推送会全部失败。缺失 %s", missing)
        elif missing:
            log.warning("以下渠道缺密钥，会被降级跳过：%s；本次可用渠道：%s", missing, ready)

    io.write_json(env.interim_path(context, "preflight.json"), info)
    return info


# ===========================================================================
# 1) 取数 + 补算
# ===========================================================================
def t_collect(context: Mapping[str, Any]) -> dict:
    cfg = env.cfg_for(context)
    now = env.resolve_now(context)
    from core import market

    raw = market.collect(cfg, now)
    log.info("数据源状态：%s", raw.get("status"))
    if cfg.source == "futu" and not (raw.get("status") or {}).get("ok"):
        log.error("OpenD 不可用：%s", (raw.get("status") or {}).get("detail"))
    io.write_json(env.interim_path(context, "raw.json"), raw)
    return {"status": raw.get("status"),
            "positions": len(raw.get("positions") or []),
            "watchlist": len(raw.get("watchlist") or [])}


def t_enrich(context: Mapping[str, Any]) -> dict:
    cfg = env.cfg_for(context)
    from core import market

    raw = io.read_json(env.interim_path(context, "raw.json"), default={})
    enriched = market.enrich(cfg, raw)
    io.write_json(env.interim_path(context, "enriched.json"), enriched)
    acct = enriched.get("account") or {}
    return {"positions": len(enriched.get("positions") or []),
            "watchlist": len(enriched.get("watchlist") or []),
            "stale_klines": len(enriched.get("stale_klines") or []),
            "mixed_currency": bool(acct.get("mixed_currency"))}


# ===========================================================================
# 2) 日历 —— 顺带算出 `resolved_trigger`（复刻 main.py:172-178 的自动切换）
# ===========================================================================
def t_calendar(context: Mapping[str, Any], requested_trigger: str = "brief") -> dict:
    cfg = env.cfg_for(context)
    now = env.resolve_now(context)
    from core import calendar as cal_mod

    events = cal_mod.upcoming(cfg, now)
    window = cal_mod.in_alert_window(events, cfg)
    resolved = requested_trigger
    if window and requested_trigger == "brief":
        resolved = "alert"                      # 高危窗口优先走 alert（与 main.py 一致）
        log.info("⚠️ 有 %d 个高危事件落在预警窗口内 → 自动切换到 alert 模式", len(window))
    payload = {"events": events, "window_count": len(window),
               "requested_trigger": requested_trigger, "resolved_trigger": resolved}
    io.write_json(env.interim_path(context, "calendar.json"), payload)
    return {"events": len(events), "window_count": len(window), "resolved_trigger": resolved}


# ===========================================================================
# 3) 新闻 / 宏观
# ===========================================================================
def t_news(context: Mapping[str, Any]) -> dict:
    cfg = env.cfg_for(context)
    from core import news

    enriched = io.read_json(env.interim_path(context, "enriched.json"), default={})
    kws = news.keywords_for(enriched.get("positions") or [], enriched.get("watchlist") or [], cfg)
    items = news.fetch(cfg, kws)
    io.write_json(env.interim_path(context, "news.json"), {"keywords": kws, "items": items})
    return {"keywords": len(kws), "items": len(items)}


def t_macro(context: Mapping[str, Any]) -> dict:
    cfg = env.cfg_for(context)
    now = env.resolve_now(context)
    from core import macro as macro_mod

    snap = macro_mod.snapshot(cfg, now)                 # 永不抛，取不到给空壳
    io.write_json(env.interim_path(context, "macro.json"), snap)
    return {"ok": snap.get("ok"), "_fixture": snap.get("_fixture"),
            "indicators": len(snap.get("indicators") or {})}


# ===========================================================================
# 4) 数据包 —— 脚本与子代理之间唯一的接口
# ===========================================================================
def t_packet(context: Mapping[str, Any]) -> dict:
    cfg = env.cfg_for(context)
    now = env.resolve_now(context)
    from core import packet

    cal = io.read_json(env.interim_path(context, "calendar.json"), default={})
    enriched = io.read_json(env.interim_path(context, "enriched.json"), default={})
    news_payload = io.read_json(env.interim_path(context, "news.json"), default={})
    macro = io.read_json(env.interim_path(context, "macro.json"), default={})
    trigger = cal.get("resolved_trigger") or "brief"

    pkt = packet.build(cfg, trigger=trigger, market=enriched,
                       events=cal.get("events") or [], news_items=news_payload.get("items") or [],
                       macro=macro, now=now)
    latest = packet.write(cfg, pkt)
    return {"path": str(latest), "trigger": trigger,
            "alerts": len(pkt.get("alerts") or []), "data_asof": pkt.get("data_asof")}


# ===========================================================================
# 5) 出稿（子代理）—— 退出码语义保 main.py:224-227
# ===========================================================================
def _slim_draft(draft: dict | None) -> dict | None:
    """给 XCom 的瘦身版（正文 body 可能很长，只留在共享卷 JSON 里）。"""
    if not draft:
        return None
    return {k: draft.get(k) for k in ("file", "send", "priority", "title")}


def brief_is_hard_failure(res: Mapping[str, Any]) -> bool:
    """是否算**硬失败**（要让 task 变红）。

    复刻 main.py:227 `return 0 if "超时" in str(result["error"]) else 1`：
      - 「超时」→ **绿**（合格产出，下次再跑）
      - 其余失败 → 红
    但**刻意偏离一处**：容器演示常态 `agent.enabled=false`（我们主动关掉子代理），
    main.py 会把它判成 exit 1；这里记**绿并标注 disabled** —— 否则演示必然是红的，
    而那条流水线本身是好的。
    """
    if res.get("ok") or res.get("disabled"):
        return False
    return "超时" not in str(res.get("error"))


def t_summarize_brief(context: Mapping[str, Any], requested_trigger: str | None = None) -> dict:
    cfg = env.cfg_for(context, slot="brief")            # ← outbox 隔离到 .../brief/
    now = env.resolve_now(context)
    from core import summarize

    cal = io.read_json(env.interim_path(context, "calendar.json"), default={})
    trigger = requested_trigger or cal.get("resolved_trigger") or "brief"

    res = summarize.run(cfg, trigger=trigger,
                        data_path=cfg.path_for("data") / "latest.json", now=now)
    out = {
        "ok": bool(res.get("ok")),
        "trigger": trigger,
        "error": res.get("error"),
        "disabled": res.get("error") == "agent.enabled = false",
        "draft": res.get("draft"),                       # 全量（含 body）落盘
    }
    if out["ok"]:
        d = res["draft"]
        log.info("简报稿：%s | send=%s priority=%s | 标题：%s",
                 d.get("file"), d.get("send"), d["priority"], d["title"])
    elif out["disabled"]:
        log.info("agent.enabled=false → 不出稿（容器演示刻意关掉子代理，记绿并标注 disabled）")
    else:
        log.error("简报子代理未产出：%s", out["error"])
    io.write_json(env.interim_path(context, "draft_brief.json"), out)
    return {k: out[k] for k in ("ok", "trigger", "error", "disabled")} | {"draft": _slim_draft(out["draft"])}


def t_summarize_brief_gated(context: Mapping[str, Any], requested_trigger: str | None = None) -> dict:
    """`t_summarize_brief` + 退出码语义：硬失败才 `raise AirflowException`（task 变红）。"""
    res = t_summarize_brief(context, requested_trigger=requested_trigger)
    if brief_is_hard_failure(res):
        from airflow.exceptions import AirflowException         # 惰性
        raise AirflowException(f"简报子代理硬失败：{res.get('error')}")
    return res


def t_rollup(context: Mapping[str, Any]) -> dict:
    cfg = env.cfg_for(context, slot="rollup")           # ← outbox 隔离到 .../rollup/
    now = env.resolve_now(context)
    from core import rollup

    macro = io.read_json(env.interim_path(context, "macro.json"), default={})
    seed = bool(env.conf(context, "seed_fixtures", False))
    if seed:
        log.warning("seed_fixtures=true（DAG conf）→ 跑 rollup 前播种 fixtures/rollup/*（离线验证）")

    # packet=None ⇒ 让 core 自己读 data/latest.json（与 main.py 传对象等价，且更少耦合）
    rr = rollup.run(cfg, packet=None, macro=macro, now=now, seed_fixtures=seed)

    if rr["errors"]:
        log.error("rollup 有周期失败（水位未推进，下次重试）：%s", rr["errors"])
    io.write_json(env.interim_path(context, "drafts_rollup.json"),
                  {"drafts": rr.get("drafts") or [], "errors": rr.get("errors") or [],
                   "periods": rr.get("periods") or [], "day_record": rr.get("day_record")})
    # ⚠️ t_rollup **永不抛**（与 main.py 一致）：周期失败只记错误、水位不推进。
    return {"errors": rr.get("errors") or [],
            "periods": rr.get("periods") or [],
            "drafts": len(rr.get("drafts") or []),
            "day_record": rr.get("day_record")}


# ===========================================================================
# 6) 推送 —— 去重 / 冷却 / 分渠道限额
# ===========================================================================
def t_push(context: Mapping[str, Any]) -> dict:
    cfg = env.cfg_for(context)
    drafts: list[dict] = []
    brief = io.read_json(env.interim_path(context, "draft_brief.json"), default={})
    if brief.get("ok") and brief.get("draft"):
        drafts.append(brief["draft"])
    # alert DAG 本就没有 rollup 阶段 → 缺失是正常的，别刷警告
    rr = io.read_json(env.interim_path(context, "drafts_rollup.json"), default={}, quiet=True)
    for d in (rr.get("drafts") or []):
        period = d.get("period")
        if period and not (cfg.get(f"rollup.periods.{period}") or {}).get("push", True):
            log.info("周期 %s 配置为不推送 → 只写全文", period)
            continue
        drafts.append(d)

    pushed = 0
    for d in drafts:                                    # push_draft 内部所有失败只记日志、不抛
        if push_draft_is_ok(cfg, d):
            pushed += 1
    if not drafts:
        log.info("没有可推的稿（agent 关 / 无新周期）→ 无事发生（合格产出）")
    return {"drafts": len(drafts), "pushed": pushed}


def push_draft_is_ok(cfg, draft: dict) -> bool:
    """转发到 lib/push.py（单独一层，便于单测与复刻 main.py:84-108）。"""
    from lib import push
    return push.push_draft(cfg, draft)


# ===========================================================================
# alert DAG 的闸门 —— 唯一一处**刻意的行为增强**
# ===========================================================================
def gate_should_run(context: Mapping[str, Any]) -> bool:
    """`ShortCircuitOperator` 的判据：预警窗口里有高险事件才放行。

    为什么这是「刻意的增强」：main.py 的 alert 模式**总是**跑完子代理、靠 `send:false`
    不发 —— 30 分钟一次的频率下这是纯浪费。这里提前短路掉整条链路。
    由 `airflow.shortcircuit_alert` 开关控制（容器 demo 默认开）；关掉即回到 parity。
    """
    cfg = env.cfg_for(context)
    if not cfg.get("airflow.shortcircuit_alert", True):
        log.info("airflow.shortcircuit_alert=false → 不短路（回到 main.py 的 parity 行为）")
        return True
    cal = io.read_json(env.interim_path(context, "calendar.json"), default={})
    n = int(cal.get("window_count") or 0)
    log.info("gate：预警窗口内高险事件 %d 个 → %s", n, "放行" if n > 0 else "短路")
    return n > 0
