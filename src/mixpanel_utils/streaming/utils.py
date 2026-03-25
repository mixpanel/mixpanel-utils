"""Utility helpers for mixpanel_import."""

import time
from datetime import datetime


def bytes_human(b: int | float) -> str:
    """Convert bytes to human-readable string."""
    for unit in ["B", "KB", "MB", "GB", "TB"]:
        if abs(b) < 1024:
            return f"{b:.1f} {unit}"
        b /= 1024
    return f"{b:.1f} PB"


def comma(n: int | float) -> str:
    """Format number with commas."""
    return f"{n:,.0f}"


def rename_keys(obj: dict, aliases: dict) -> dict:
    """Rename keys in a dict according to aliases mapping."""
    if not aliases:
        return obj
    result = {}
    for k, v in obj.items():
        new_key = aliases.get(k, k)
        result[new_key] = v
    return result


class Timer:
    """Simple timer for tracking job duration."""

    def __init__(self):
        self.start_time: float | None = None
        self.end_time: float | None = None

    def start(self) -> "Timer":
        self.start_time = time.time()
        self.end_time = None
        return self

    def stop(self) -> "Timer":
        self.end_time = time.time()
        return self

    def elapsed(self) -> float:
        """Return elapsed seconds since start."""
        end = self.end_time or time.time()
        start = self.start_time or end
        return end - start

    @property
    def delta_ms(self) -> int:
        end = self.end_time or time.time()
        start = self.start_time or end
        return int((end - start) * 1000)

    @property
    def human(self) -> str:
        delta = self.delta_ms
        if delta < 1000:
            return f"{delta}ms"
        elif delta < 60000:
            return f"{delta / 1000:.1f}s"
        elif delta < 3600000:
            minutes = delta // 60000
            seconds = (delta % 60000) // 1000
            return f"{minutes}m {seconds}s"
        else:
            hours = delta // 3600000
            minutes = (delta % 3600000) // 60000
            return f"{hours}h {minutes}m"

    def report(self) -> dict:
        return {
            "delta": self.delta_ms,
            "human": self.human,
            "start_time": datetime.fromtimestamp(self.start_time).isoformat() if self.start_time else None,
            "end_time": datetime.fromtimestamp(self.end_time).isoformat() if self.end_time else None,
        }
