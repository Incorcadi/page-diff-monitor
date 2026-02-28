from __future__ import annotations

"""keying.py — единая логика извлечения item_id и построения ключа дедупликации.

Зачем:
- runtime/infer хотят стабильный способ достать id
- storage (JSONL/SQLite) хочет стабильный ключ item_key
- чтобы не было расхождений между модулями, держим это в одном месте
"""

import hashlib
import json
from typing import Any, Optional

from .site_profile import ExtractSpec
from .json_path import get_by_path


def extract_item_id(item: dict[str, Any], spec: ExtractSpec) -> Optional[str]:
    """Достаёт id из item по ExtractSpec.

    Приоритет:
    1) spec.id_path (dot-path)
    2) spec.id_keys (fallback, поддерживает dot-path и обычный ключ)
    """
    val: Any = None

    if spec.id_path:
        val = get_by_path(item, spec.id_path)

    if val is None or val == "":
        for k in spec.id_keys:
            if "." in k:
                val = get_by_path(item, k)
            else:
                val = item.get(k)
            if val is not None and val != "":
                break

    if val is None or val == "":
        return None
    return str(val)


def make_item_key(item: dict[str, Any], spec: ExtractSpec) -> str:
    """Ключ дедупликации.

    1) если есть id => "id:<id>"
    2) иначе => "sha1:<sha1(json_sorted)>"
    """
    _id = extract_item_id(item, spec)
    if _id:
        return f"id:{_id}"

    blob = json.dumps(item, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode("utf-8")
    h = hashlib.sha1(blob).hexdigest()
    return f"sha1:{h}"