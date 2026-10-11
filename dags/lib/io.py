"""共享卷上的 JSON 读写 —— 阶段之间传**大对象**用它，不用 XCom。

为什么要这样分工：
- **小标量**（路径、计数、`resolved_trigger`）走 XCom：UI 里一眼能看见。
- **大对象**（数据包、富集后的持仓、报告包草稿）落 JSON：XCom 是给元数据用的，
  塞几十 KB 的 dict 进去会撑爆 metadata 库、还会让序列化变慢。
"""

from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import Any, Optional

log = logging.getLogger("moomoo-watch.airflow")


def write_json(path, obj: Any) -> Path:
    """原子写：先写 `.tmp` 再 replace（半截文件不会被下游读到）。"""
    p = Path(path)
    p.parent.mkdir(parents=True, exist_ok=True)
    tmp = p.with_suffix(p.suffix + ".tmp")
    tmp.write_text(json.dumps(obj, ensure_ascii=False, indent=2), encoding="utf-8")
    tmp.replace(p)
    return p


def read_json(path, default: Optional[Any] = None, quiet: bool = False) -> Any:
    """读 JSON；缺失/损坏返回 `default`（并记日志，不抛）。

    `quiet=True`：**缺失是正常情况**（如 alert DAG 里本就没有 rollup 草稿）→ 不记警告。
    """
    p = Path(path)
    if not p.is_file():
        if not quiet:
            log.warning("中间产物不存在：%s（默认 %r）", p, default)
        return default
    try:
        return json.loads(p.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, OSError) as exc:
        log.warning("中间产物读取失败 %s：%s（默认 %r）", p, exc, default)
        return default
