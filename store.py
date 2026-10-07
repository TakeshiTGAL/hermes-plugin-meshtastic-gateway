"""Append-only record of node ids. Message text is not stored."""
from __future__ import annotations

import json
import os
import threading
from pathlib import Path

_LOCK = threading.Lock()

MAX_RECORDS = 200


def data_dir() -> Path | None:
    try:
        from plugins.plugin_storage import plugin_data_dir
    except Exception:
        return None
    try:
        path = Path(plugin_data_dir("meshtastic-gateway"))
    except Exception:
        return None
    return path


def remember(node: str, direction: str, nbytes: int, now: float) -> None:
    root = data_dir()
    if root is None:
        return
    try:
        root.mkdir(parents=True, exist_ok=True)
        path = root / "nodes.json"
        with _LOCK:
            rows: list = []
            if path.exists():
                try:
                    loaded = json.loads(path.read_text(encoding="utf-8"))
                    if isinstance(loaded, list):
                        rows = loaded
                except (OSError, json.JSONDecodeError):
                    return
            rows.append({
                "node": node,
                "direction": direction,
                "bytes": int(nbytes),
                "at": now,
            })
            tmp = path.with_name("nodes.json.tmp")
            tmp.write_text(json.dumps(rows[-MAX_RECORDS:]), encoding="utf-8")
            os.replace(tmp, path)
    except OSError:
        return
