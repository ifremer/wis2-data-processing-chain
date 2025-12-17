# plugins/mqtt/utils.py
"""
Small utility helpers for MQTT configuration handling.
"""

from __future__ import annotations


def as_bool(v, default: bool = False) -> bool:
    """
    Convert a value to boolean, using a fallback default.
    """
    if isinstance(v, bool):
        return v
    if v is None:
        return default
    return str(v).strip().lower() in {"1", "true", "yes", "on", "y", "t"}


def norm_transport(v: str | None, default: str = "tcp") -> str:
    """
    Normalize a transport string ('tcp' or 'websockets').
    """
    v = (v or default).lower()
    return "websockets" if v in {"ws", "websocket", "websockets"} else "tcp"
