from __future__ import annotations

"""json_path.py — единая реализация доступа по dot-path.

Dot-path:
  - "a.b.c" для dict
  - "arr.0.id" для list (цифровой сегмент = индекс)
"""

from typing import Any


def get_by_path(obj: Any, path: str) -> Any:
    """Вернуть значение по dot-path или None, если путь не существует."""
    cur = obj
    for seg in path.split("."):
        if cur is None:
            return None

        if isinstance(cur, list) and seg.isdigit():
            idx = int(seg)
            if 0 <= idx < len(cur):
                cur = cur[idx]
            else:
                return None
            continue

        if isinstance(cur, dict):
            if seg in cur:
                cur = cur[seg]
            else:
                return None
            continue

        return None
    return cur


def coalesce_by_paths(obj: Any, paths: list[str]) -> Any:
    """Первое непустое (не None и не пустая строка) значение по списку путей."""
    for p in paths:
        v = get_by_path(obj, p) if "." in p else (obj.get(p) if isinstance(obj, dict) else None)
        if v is not None and v != "":
            return v
    return None