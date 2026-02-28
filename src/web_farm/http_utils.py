from __future__ import annotations

"""
http_utils.py — извлечение next/cursor/meta из уже полученного ответа.

ВАЖНО:
- Этот модуль НЕ делает HTTP.
- Он работает только с headers и JSON (data), чтобы runtime/infer могли понимать пагинацию.

Идея:
Response = накладная + груз.
Мы читаем накладную (headers) и признаки на грузе (JSON).
"""

from typing import Any, Optional,Mapping
import re
from .resp_read import JSONType


from .json_path import get_by_path
def looks_like_url(s: str) -> bool:
    return s.startswith("http://") or s.startswith("https://")


def parse_link_next(headers: Mapping[str, str] | None) -> str | None:
    """
    Парсим HTTP header: Link: <url>; rel="next", <url2>; rel="prev"

    Возвращаем next_url или None.
    """
    if not headers:
        return None
    link = headers.get("Link") or headers.get("link")
    if not link:
        return None

    # Разбиваем по запятым между ссылками
    parts = [p.strip() for p in link.split(",") if p.strip()]
    for part in parts:
        m_url = re.search(r"<([^>]+)>", part)
        m_rel = re.search(r'rel="?([^";]+)"?', part)
        if not m_url or not m_rel:
            continue
        url = m_url.group(1).strip()
        rel = m_rel.group(1).strip().lower()
        if rel == "next":
            return url
    return None



_NEXT_URL_PATHS = (
    "next",
    "next_url",
    "nextUrl",
    "links.next",
    "paging.next",
    "pagination.next",
    "page.next",
)


def extract_next_url_from_json(data: JSONType) -> Optional[str]:
    """
    Достаём next_url из JSON по типовым путям.
    """
    if not isinstance(data, dict):
        return None

    for p in _NEXT_URL_PATHS:
        v = get_by_path(data, p)
        if isinstance(v, str) and looks_like_url(v):
            return v

    # Иногда next лежит как {"href": "..."} или {"url": "..."}
    for p in _NEXT_URL_PATHS:
        v = get_by_path(data, p)
        if isinstance(v, dict):
            for k in ("href", "url"):
                s = v.get(k)
                if isinstance(s, str) and looks_like_url(s):
                    return s
    return None


_CURSOR_PATHS = (
    "next_cursor",
    "nextCursor",
    "cursor",
    "cursor.next",
    "page_info.end_cursor",
    "pageInfo.endCursor",
    "meta.cursor",
)


def extract_cursor_token(data: JSONType) -> Optional[str]:
    if not isinstance(data, dict):
        return None

    for p in _CURSOR_PATHS:
        v = get_by_path(data, p)
        if isinstance(v, str) and v.strip():
            return v.strip()

    # Иногда cursor лежит в meta как число
    for p in _CURSOR_PATHS:
        v = get_by_path(data, p)
        if isinstance(v, (int, float)):
            return str(v)

    return None


def has_page_meta(data: JSONType) -> bool:
    """
    Признак, что API говорит “страницами” (page/totalPages/...)
    """
    if not isinstance(data, dict):
        return False
    for key in ("page", "pageIndex", "totalPages", "pages", "currentPage"):
        if key in data:
            return True
    meta = data.get("meta")
    if isinstance(meta, dict):
        for key in ("page", "totalPages", "pages"):
            if key in meta:
                return True
    return False


def has_offset_meta(data: JSONType) -> bool:
    """
    Признак, что API говорит “offset/limit” (offset, start, count, total, ...)
    """
    if not isinstance(data, dict):
        return False
    for key in ("offset", "start", "_start", "limit", "_limit", "count", "total", "totalCount"):
        if key in data:
            return True
    meta = data.get("meta")
    if isinstance(meta, dict):
        for key in ("offset", "start", "limit", "count", "total"):
            if key in meta:
                return True
    return False
