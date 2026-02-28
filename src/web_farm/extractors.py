"""
extractors.py — извлечение items и ids из JSON “под ферму”.

Отличие от старой версии:
- dot-path вынесен в json_path.get_by_path
- извлечение id вынесено в keying.extract_item_id
- это убирает дублирование и рассинхрон между storage/runtime/infer
"""

from __future__ import annotations

from typing import Any

from .site_profile import JSONType, ExtractSpec
from .json_path import get_by_path
from .keying import extract_item_id
from .html_extract import extract_items_from_html


def extract_items(data: JSONType, spec: ExtractSpec) -> list[Any]:
    """Извлечь список items из JSON-ответа."""
    if spec.items_path:
        v = get_by_path(data, spec.items_path)
        if isinstance(v, list):
            return v
        if isinstance(v, dict):
            for k in spec.items_keys:
                vv = v.get(k)
                if isinstance(vv, list):
                    return vv

    if isinstance(data, list):
        return data

    if isinstance(data, dict):
        for k in spec.items_keys:
            v = data.get(k)
            if isinstance(v, list):
                return v

        level: list[Any] = [data]
        for _ in range(spec.max_depth):
            next_level: list[Any] = []
            for node in level:
                if not isinstance(node, dict):
                    continue

                for k in spec.items_keys:
                    v = node.get(k)
                    if isinstance(v, list):
                        return v

                for ck in spec.container_keys:
                    inner = node.get(ck)
                    if isinstance(inner, dict):
                        next_level.append(inner)
                    elif isinstance(inner, list):
                        return inner

            level = next_level

    return []


def extract_items_any(data: Any, spec: ExtractSpec, *, payload_kind: str = "json") -> list[Any]:
    """Extract items from JSON or HTML payload according to kind."""
    kind = str(payload_kind or "json").lower()
    if kind == "html":
        if isinstance(data, bytes):
            text = data.decode("utf-8", errors="replace")
        else:
            text = str(data) if data is not None else ""
        return extract_items_from_html(text, spec)

    if isinstance(data, (dict, list)):
        return extract_items(data, spec)
    return []


def ids_of(items: list[Any], spec: ExtractSpec) -> set[str]:
    """Получить множество ID из items по единому правилу keying.extract_item_id()."""
    out: set[str] = set()
    for it in items:
        if not isinstance(it, dict):
            continue
        v = extract_item_id(it, spec)
        if v is not None and v != "":
            out.add(str(v))
    return out
