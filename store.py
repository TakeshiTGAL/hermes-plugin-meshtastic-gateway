"""Append-only record of node ids. Message text is not stored."""
from __future__ import annotations

import json
import logging
import os
import threading
from pathlib import Path

_LOCK = threading.Lock()
_corrupt_warned = False

MAX_RECORDS = 200


def data_dir() -> Path | None:
    try:
        from plugins.plugin_storage import plugin_data_dir
    except Exception:
        return None
    try:
        path = Path(plugin_data_dir("radio-dm-gateway"))
    except Exception:
        return None
    return path


def _warn_corrupt_once() -> None:
    """Say once that a corrupt nodes.json was left unchanged and recording stopped."""
    global _corrupt_warned
    if _corrupt_warned:
        return
    _corrupt_warned = True
    logging.getLogger(__name__).warning(
        "nodes.json could not be read as a list of rows. It was left unchanged, and new rows are not recorded until you delete it."
    )


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
                except (OSError, json.JSONDecodeError):
                    _warn_corrupt_once()
                    return
                if not isinstance(loaded, list):
                    _warn_corrupt_once()
                    return
                rows = loaded
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
