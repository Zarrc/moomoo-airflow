"""适配层地基：环境引导 · 配置装配 · 可复现时钟。

三条不变量（每一条都对应一类**静默**失败）：

1) **一行 core 都不改** —— 所有重定向靠「给 `cfg.raw` 赋绝对路径」实现。
   pathlib 语义 `Path("/a") / "/b" == "/b"`：绝对路径会**丢弃**左边的 PROJECT_ROOT。
   于是仓库可以**只读挂载**（产物永不写回仓库目录），且全部落进 vault 沙箱。

2) **合成 vault 根** —— `/vault` 放 `.claude/` 锚点，`/vault/project` 挂仓库（:ro）。
   `core.config.find_vault_root()` 从 `/vault/project/core/config.py` 往上找到 `/vault`。
   ⚠️ 仓库**绝不能**直接挂到 `/opt/airflow/dags` 之类的地方：那样 vault 根会解析到
   容器根、`summarize` 里一串 `.relative_to(vault_root)` 直接崩 —— 而且**失败点在
   summarize 内部，不在 preflight**（最经典的静默坑，见 README §风险 R1）。

3) **时钟只有一个入口** —— `context["data_interval_end"]`。绝不 `datetime.now()` 造 now：
   周界（session_date）/ 打分 / 水位全依赖它，用真实墙钟 ⇒ backfill 不可复现。

本模块**不 import airflow**（`as_callable` 除外，它做惰性导入）—— 因此本地无 Docker
也能直接 `import lib.env` 跑冒烟自检。
"""

from __future__ import annotations

import logging
import os
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Mapping

log = logging.getLogger("moomoo-watch.airflow")

CST = timezone(timedelta(hours=8))

# --- 容器内布局（与 docker-compose.yaml / docker/init-vault.sh 一致）---------
# 默认值就是容器默认 ⇒ 不带任何环境变量也能跑；本地冒烟时用环境变量指到临时沙箱。
DEFAULT_PROJECT_ROOT = "/vault/project"
DEFAULT_VAULT = "/vault"
DEFAULT_CONFIG = "/vault/scratch/config.yaml"


def project_root() -> Path:
    return Path(os.environ.get("MOOMOO_PROJECT_ROOT", DEFAULT_PROJECT_ROOT))


def vault_root_path() -> Path:
    return Path(os.environ.get("MOOMOO_VAULT", DEFAULT_VAULT))


def config_file() -> Path:
    return Path(os.environ.get("MOOMOO_WATCH_CONFIG", DEFAULT_CONFIG))


def interim_root() -> Path:
    return Path(os.environ.get("MOOMOO_INTERIM", str(vault_root_path() / "scratch" / "interim")))


# --- 引导 ---------------------------------------------------------------------
def bootstrap() -> Path:
    """把仓库根插进 `sys.path`，让 `from core import ...` 可用。幂等。"""
    root = str(project_root())
    if root not in sys.path:
        sys.path.insert(0, root)
    return project_root()


def load_cfg():
    """装配一份指向沙箱的 Config。**不改 core** —— 只读它、只覆写 `cfg.raw`。"""
    bootstrap()
    from core.config import Config            # 惰性：DAG parse 期绝不 import core
    return Config(config_file())


# --- 可复现时钟 ---------------------------------------------------------------
def resolve_now(context: Mapping[str, Any]) -> datetime:
    """`now ← context["data_interval_end"]`（tz-aware，带 +08:00）。

    为什么是它而不是 `logical_date`：
      - **定时运行**：`logical_date` 是数据区间的**起点**，运行在区间**终点**触发
        （D+1 06:30 CST），所以 `data_interval_end` == 触发墙钟 —— 正是 main.py 的 now。
        用 `logical_date` 会**整体差一天**（session_date 落到前一交易日）。
      - **手动 trigger**（Airflow 2.x，维护者确认的既定行为）：区间是**往前倒推**的，
        `data_interval_end` == 你传的 `--exec-date`。（不是 exec-date + interval！）
      - **backfill**：按时间表算整区间，`data_interval_end` == 区间终点。
      - 🚫 **不要用 `{{ ds }}`** —— 那是 UTC 日期，踩时区坑。
    三种入口统一走这里 ⇒ 语义一致。
    """
    end = context["data_interval_end"]
    s = end.isoformat() if isinstance(end, datetime) else str(end)
    dt = datetime.fromisoformat(s)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=CST)
    return dt.astimezone(CST)


def dag_id(context: Mapping[str, Any]) -> str:
    """当前 DAG 的 id（容错取值：`dag` 对象 → `dag_run` → "unknown"）。"""
    for key, attr in (("dag", "dag_id"), ("dag_run", "dag_id")):
        obj = context.get(key)
        val = getattr(obj, attr, None)
        if val:
            return str(val)
    return "unknown"


def run_key(context: Mapping[str, Any]) -> str:
    """本次运行的稳定键 = `<dag_id>__<data_interval_end>`。

    同一个数据区间重复跑 → 复用同一 interim 目录（幂等）。
    ⚠️ **必须带 dag_id**：`*/30` 与 `30 6 * * *` 会在 **06:30 那个区间重合** ——
    不带 dag_id 的话两个 DAG 会共用 `interim/` 与 `outbox/<run>/…`，互相踩文件。
    """
    return f"{dag_id(context)}__{resolve_now(context).strftime('%Y-%m-%dT%H%M%S%z')}"


# --- 中间产物 / 隔离落点 -------------------------------------------------------
def interim_dir(context: Mapping[str, Any], create: bool = True) -> Path:
    p = interim_root() / run_key(context)
    if create:
        p.mkdir(parents=True, exist_ok=True)
    return p


def interim_path(context: Mapping[str, Any], name: str) -> Path:
    """阶段之间的中间产物落点（共享卷 JSON）—— 不用 XCom 传大对象。"""
    return interim_dir(context) / name


def outbox_for(context: Mapping[str, Any], slot: str) -> Path:
    """每个写者一个**隔离的 outbox 子目录**（slot ∈ {brief, rollup, push}）。

    ⚠️ 承重约束：`summarize._newest_outbox()` 在 `outbox/*.md` 里**按 mtime 取最新** ——
    两个写者共用同一 outbox 会**静默取错标题**（不报错，见 core/summarize.py:83-93,174）。
    隔离后仍在 vault 之下 ⇒ 满足 summarize.py:146-147 的 `outbox.relative_to(vault_root)`。
    """
    return vault_root_path() / "scratch" / "outbox" / run_key(context) / slot


def moomoo_cfg(context: Mapping[str, Any], slot: str):
    """给某个「写者」装配 cfg：把 outbox 覆写到隔离子目录（core 一行不改，纯 `cfg.raw` 赋值）。"""
    cfg = load_cfg()
    paths = cfg.raw.setdefault("paths", {})
    paths["outbox"] = str(outbox_for(context, slot))
    return cfg


def cfg_for(context: Mapping[str, Any], slot: str | None = None):
    """load_cfg / moomoo_cfg + 挂上沙箱任务日志。各 stage 统一用它。"""
    cfg = moomoo_cfg(context, slot) if slot else load_cfg()
    setup_task_logging(cfg)
    return cfg


# --- DAG conf -----------------------------------------------------------------
def conf(context: Mapping[str, Any], name: str, default: Any = None) -> Any:
    """读 DAG conf（`dags trigger --conf` 才有；backfill 没有）。兼容 dict 形式的假 context。"""
    dr = context.get("dag_run")
    if dr is None:
        return default
    c = getattr(dr, "conf", None)
    if c is None and isinstance(dr, Mapping):
        c = dr.get("conf")
    return c.get(name, default) if isinstance(c, Mapping) else default


# --- 日志 ---------------------------------------------------------------------
_LOG_READY: set = set()


def setup_task_logging(cfg) -> None:
    """挂上沙箱里的 `logs/watch.log`（复刻 main.py 的 setup_logging，
    但**不替换** Airflow 已装的 handler）。幂等。

    为什么不照抄 main.py 的 `root.handlers[:] = [fh, sh]`：那会**拔掉 Airflow 自己的
    日志 handler**，任务日志就丢了。这里只**追加**。

    ⚠️ **Airflow 任务进程里绝不挂 stdout 的 StreamHandler**（2026-10-12 容器首跑实测踩的）：
    Airflow 把任务的 stdout/stderr 换成「回灌给 logger 的假流」（StreamLogWriter），
    root 上再挂一个写 stdout 的 handler 就成了 记录→stdout→logger→记录 的**无限递归**
    （RecursionError；handleError 打的 traceback 又写 stderr 假流，二次放大成巨型日志行）。
    任务里 root 的记录本来就经 Airflow 的 root→stderr 假流→task handler 进任务日志。
    stdout handler 只在**非 Airflow 环境**（本地直跑）保留。
    """
    logdir = cfg.path_for("logs")
    logdir.mkdir(parents=True, exist_ok=True)
    target = str((logdir / "watch.log").resolve())
    if target in _LOG_READY:
        return

    fmt = logging.Formatter("%(asctime)s %(levelname)-7s %(name)s | %(message)s", "%Y-%m-%d %H:%M:%S")
    root = logging.getLogger()
    try:
        from logging.handlers import TimedRotatingFileHandler
        fh = TimedRotatingFileHandler(logdir / "watch.log", when="midnight",
                                      backupCount=30, encoding="utf-8")
        fh.setFormatter(fmt)
        root.addHandler(fh)
    except OSError as exc:                             # 只读挂载等 → 不阻断任务
        log.warning("任务日志文件挂载失败（继续跑）：%s", exc)
    if not os.environ.get("AIRFLOW_CTX_DAG_ID"):       # 本地直跑才挂 console handler
        sh = logging.StreamHandler(sys.stdout)
        sh.setFormatter(fmt)
        root.addHandler(sh)
    if root.level == logging.NOTSET or root.level > logging.INFO:
        root.setLevel(logging.INFO)
    _LOG_READY.add(target)
