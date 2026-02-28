from __future__ import annotations

"""storage_jsonl.py — утилиты ключа/ID для сохранения items.

Раньше тут была собственная реализация dot-path/id/key.
Теперь это вынесено в:
  - json_path.py (get_by_path)
  - keying.py (extract_item_id / make_item_key)

Этот модуль оставлен как тонкая совместимая обёртка,
чтобы не ломать импорты и чтобы тесты/CLI оставались простыми.
"""

from typing import Any, Optional

from .site_profile import ExtractSpec
from .keying import extract_item_id as _extract_item_id
from .keying import make_item_key as _make_item_key


def extract_item_id(item: dict[str, Any], spec: ExtractSpec) -> Optional[str]:
    return _extract_item_id(item, spec)


def make_item_key(item: dict[str, Any], spec: ExtractSpec) -> str:
    return _make_item_key(item, spec)