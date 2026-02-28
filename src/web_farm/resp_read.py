from __future__ import annotations

"""
resp_read.py — “ОТК ответа”: безопасно читать текст/JSON, не падая на реальных API.

Зачем это нужно
---------------
В парсинге очень часто бывает так:
- статус 200, но тело — HTML (капча/логин/бан/страница ошибки),
- статус 200, но JSON начинается с XSSI-префикса (например ")]}',\n"),
- в JSON формально "ok", но внутри есть "error"/"errors"/success=false (soft error),
- кодировка пляшет (кракозябры) — особенно на локальных/старых сайтах.

Этот модуль НЕ делает HTTP. Он только “вскрывает контейнер” (Response) как ОТК.
"""

from dataclasses import dataclass
from typing import Any, Optional, Union, Iterable
import json
import re

import requests


JSONType = Union[dict[str, Any], list[Any]]


@dataclass
class TextPayload:
    text: str
    encoding_used: str
    source: str
    content_type: str
    size_bytes: int


@dataclass
class JsonReadResult:
    ok: bool
    data: Optional[JSONType] = None
    error: Optional[str] = None          # not_json | json_decode_error | binary | bad_shape | soft_error
    details: Optional[str] = None        # подробности
    preview: Optional[str] = None        # первые N символов
    encoding_used: Optional[str] = None  # какая кодировка реально применена
    content_type: Optional[str] = None   # Content-Type


def _extract_charset(content_type: str) -> Optional[str]:
    m = re.search(r"charset=([^\s;]+)", content_type or "", flags=re.IGNORECASE)
    return m.group(1).strip("\"'") if m else None


def is_binary_content_type(content_type: str) -> bool:
    ct = (content_type or "").lower()
    return (
        ct.startswith("image/")
        or "pdf" in ct
        or "zip" in ct
        or "octet-stream" in ct
        or "application/gzip" in ct
    )


def read_text_safely(
    resp: requests.Response,
    *,
    fallback_encodings: tuple[str, ...] = ("utf-8", "cp1251"),
    errors: str = "replace",
) -> Optional[TextPayload]:
    """
    Пытаемся получить текст из resp.content корректно.

    Алгоритм = “гипотезы как на заводе”:
    1) если в Content-Type есть charset=... — пробуем его
    2) иначе пробуем resp.encoding (что requests выставил)
    3) иначе пробуем resp.apparent_encoding (если есть)
    4) иначе перебираем fallback_encodings
    5) в самом конце — utf-8 replace
    """
    content_type = resp.headers.get("Content-Type", "")
    if is_binary_content_type(content_type):
        return None

    raw = resp.content or b""
    size = len(raw)

    charset = _extract_charset(content_type)
    if charset:
        try:
            return TextPayload(
                text=raw.decode(charset, errors=errors),
                encoding_used=charset,
                source="header_charset",
                content_type=content_type,
                size_bytes=size,
            )
        except LookupError:
            pass

    if resp.encoding:
        try:
            return TextPayload(
                text=raw.decode(resp.encoding, errors=errors),
                encoding_used=resp.encoding,
                source="requests_encoding",
                content_type=content_type,
                size_bytes=size,
            )
        except LookupError:
            pass

    apparent = getattr(resp, "apparent_encoding", None)
    if apparent:
        try:
            return TextPayload(
                text=raw.decode(apparent, errors=errors),
                encoding_used=apparent,
                source="apparent_encoding",
                content_type=content_type,
                size_bytes=size,
            )
        except LookupError:
            pass

    for enc in fallback_encodings:
        try:
            return TextPayload(
                text=raw.decode(enc, errors=errors),
                encoding_used=enc,
                source="fallback_list",
                content_type=content_type,
                size_bytes=size,
            )
        except LookupError:
            continue

    return TextPayload(
        text=raw.decode("utf-8", errors="replace"),
        encoding_used="utf-8",
        source="fallback_utf8",
        content_type=content_type,
        size_bytes=size,
    )


_XSSI_PREFIXES: tuple[str, ...] = (
    ")]}'",         # angular/google style; часто первая строка
    "while(1);",    # защитные JS-префиксы
    "for(;;);",
    "throw 1;",     # иногда встречается
)


def strip_xssi_prefix(text: str) -> str:
    """
    Убираем XSSI-префиксы.

    Принцип: “снимаем пломбу с контейнера перед распаковкой”.
    """
    t = text.lstrip()
    for pref in _XSSI_PREFIXES:
        if t.startswith(pref):
            # Вариант 1: pref стоит одной строкой (обычно так)
            lines = t.splitlines(True)
            if len(lines) > 1:
                return "".join(lines[1:])
            # Вариант 2: вообще нет перевода строки
            return t[len(pref):]
    return text


def _strip_bom(text: str) -> str:
    # BOM (Byte Order Mark) иногда прилетает в начале UTF-8 текста.
    if text.startswith("\ufeff"):
        return text.lstrip("\ufeff")
    return text


def looks_like_json(content_type: str, text: str) -> bool:
    ct = (content_type or "").lower()
    if "json" in ct:
        return True
    s = _strip_bom(strip_xssi_prefix(text)).lstrip()
    return s[:1] in ("{", "[")


def _check_shape(must_have_keys: Optional[set[str]], data: JSONType) -> Optional[str]:
    if must_have_keys is None:
        return None
    if not isinstance(data, dict):
        return "not_dict"
    missing = [k for k in must_have_keys if k not in data]
    if missing:
        return "missing_keys: " + ", ".join(missing)
    return None


def detect_soft_error(data: JSONType) -> Optional[str]:
    """
    “Soft error” = ошибка уровня приложения, когда HTTP=200, но в JSON явно написано “ошибка”.

    Важно: делаем проверку аккуратно, чтобы не ловить ложные срабатывания.
    """
    if not isinstance(data, dict):
        return None

    # 1) error / errors
    if "error" in data:
        val = data.get("error")
        if isinstance(val, str) and val.strip():
            return f"error: {val.strip()}"
        if isinstance(val, (dict, list)) and val:
            return "error: non-empty"
    if "errors" in data:
        val = data.get("errors")
        if isinstance(val, list) and len(val) > 0:
            return "errors: non-empty list"
        if isinstance(val, dict) and len(val) > 0:
            return "errors: non-empty dict"

    # 2) success=false
    succ = data.get("success", None)
    if succ is False:
        return "success=false"
    if isinstance(succ, str) and succ.strip().lower() in ("false", "0", "no", "fail", "failed"):
        return f"success={succ}"

    # 3) status=error/fail
    status = data.get("status", None)
    if isinstance(status, str) and status.strip().lower() in ("error", "fail", "failed"):
        return f"status={status.strip()}"

    # 4) message (если явно похоже на ошибку)
    msg = data.get("message", None)
    if isinstance(msg, str):
        m = msg.strip().lower()
        if m.startswith("error") or "permission denied" in m or "unauthorized" in m:
            return f"message={msg.strip()}"

    return None


def safe_read_json(
    resp: requests.Response,
    *,
    force: bool = False,
    must_have_keys: Optional[set[str]] = None,
    detect_soft: bool = True,
    preview_len: int = 220,
) -> JsonReadResult:
    """
    Безопасная попытка извлечь JSON.

    force=True — пробовать json.loads даже если не похоже на JSON (используй, когда ТОЧНО ждёшь JSON).
    """
    content_type = resp.headers.get("Content-Type", "")
    tp = read_text_safely(resp)
    if tp is None:
        return JsonReadResult(
            ok=False,
            error="binary",
            details=content_type,
            preview=f"<binary {len(resp.content or b'')} bytes>",
            content_type=content_type,
        )

    raw_text = tp.text
    preview = raw_text[:preview_len].replace("\n", " ")

    if not (looks_like_json(content_type, raw_text) or force):
        # Частый кейс: HTML под видом успеха.
        # Не пытаемся парсить, чтобы не получать “json_decode_error” на каждом втором бане.
        return JsonReadResult(
            ok=False,
            error="not_json",
            details=content_type,
            preview=preview,
            encoding_used=tp.encoding_used,
            content_type=content_type,
        )

    cleaned = _strip_bom(strip_xssi_prefix(raw_text)).lstrip()

    try:
        data: JSONType = json.loads(cleaned)
    except Exception as e:
        return JsonReadResult(
            ok=False,
            error="json_decode_error",
            details=str(e),
            preview=preview,
            encoding_used=tp.encoding_used,
            content_type=content_type,
        )

    shape_problem = _check_shape(must_have_keys, data)
    if shape_problem:
        return JsonReadResult(
            ok=False,
            error="bad_shape",
            details=shape_problem,
            preview=preview,
            encoding_used=tp.encoding_used,
            content_type=content_type,
            data=data,
        )

    if detect_soft:
        soft = detect_soft_error(data)
        if soft:
            return JsonReadResult(
                ok=False,
                error="soft_error",
                details=soft,
                preview=preview,
                encoding_used=tp.encoding_used,
                content_type=content_type,
                data=data,
            )

    return JsonReadResult(
        ok=True,
        data=data,
        preview=preview,
        encoding_used=tp.encoding_used,
        content_type=content_type,
    )
