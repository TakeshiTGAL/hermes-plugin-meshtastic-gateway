"""Plugin entry. Heavy imports stay inside register()."""
from __future__ import annotations


def register(ctx) -> None:
    if __package__:
        from .adapter import register as _register
    else:
        from adapter import register as _register
    _register(ctx)
