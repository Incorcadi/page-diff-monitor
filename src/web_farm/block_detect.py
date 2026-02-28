from __future__ import annotations

"""
block_detect.py — эвристика для распознавания блокировок/антиботов.

Важно:
- Это НЕ "обход". Это детектор для фермы: понять, что нужен человек/авторизация,
  записать событие в SQLite и остановить прогон с возможностью resume.
"""

from typing import Any, Optional
import re


def _low_text(resp: Any, limit: int = 6000) -> str:
    try:
        t = resp.text or ""
    except Exception:
        try:
            t = (resp.content or b"").decode("utf-8", errors="ignore")
        except Exception:
            t = ""
    t = t[:max(0, int(limit))]
    return t.lower()


def _headers_lower(resp: Any) -> dict[str, str]:
    out: dict[str, str] = {}
    try:
        for k, v in (resp.headers or {}).items():
            out[str(k).lower()] = str(v)
    except Exception:
        pass
    return out


def classify_block(resp: Any) -> Optional[dict[str, Any]]:
    """
    Возвращает None, если не похоже на блокировку.
    Иначе dict:
      - hint: cloudflare|js_challenge|captcha|rate_limited|auth_required|access_denied|blocked
      - resp_url_final
      - resp_headers (subset)
      - resp_snippet (first N chars)
    """
    if resp is None:
        return None

    try:
        sc = int(getattr(resp, "status_code", 0) or 0)
    except Exception:
        sc = 0

    # чаще всего блоки на этих статусах
    if sc and sc not in (401, 403, 429, 503):
        # иногда CF отдаёт 200 с challenge HTML, но это редкий кейс — проверим по тексту ниже
        pass

    h = _headers_lower(resp)
    txt = _low_text(resp)

    # CF / challenge markers
    is_cf = ("cf-ray" in h) or ("cloudflare" in h.get("server", "").lower()) or ("__cf_bm" in txt) or ("cf-chl" in txt)
    is_js = ("checking your browser" in txt) or ("just a moment" in txt) or ("verify you are human" in txt)
    is_captcha = ("g-recaptcha" in txt) or ("hcaptcha" in txt) or re.search(r"\bcaptcha\b", txt) is not None
    is_rate = (sc == 429) or ("too many requests" in txt)
    is_auth = (sc == 401) or ("sign in" in txt) or ("log in" in txt) or ("authorization" in txt)
    is_denied = (sc == 403) or ("access denied" in txt) or ("forbidden" in txt)

    hint: Optional[str] = None
    if is_cf:
        hint = "cloudflare"
        if is_js:
            hint = "js_challenge"
        if is_captcha:
            hint = "captcha"
    elif is_captcha:
        hint = "captcha"
    elif is_js:
        hint = "js_challenge"
    elif is_rate:
        hint = "rate_limited"
    elif is_auth:
        hint = "auth_required"
    elif is_denied:
        hint = "access_denied"

    # 200 + challenge html
    if hint is None and sc == 200 and (is_cf or is_js or is_captcha):
        hint = "blocked"

    if hint is None:
        return None

    # keep only useful headers
    keep = ["server", "cf-ray", "set-cookie", "location", "content-type", "retry-after"]
    h_keep = {k: v for k, v in h.items() if k in keep}

    # snippet for DB (keep short)
    snippet = ""
    try:
        snippet = (resp.text or "")[:1200]
    except Exception:
        try:
            snippet = (resp.content or b"")[:1200].decode("utf-8", errors="ignore")
        except Exception:
            snippet = ""

    return {
        "hint": hint,
        "resp_url_final": getattr(resp, "url", None),
        "resp_headers": h_keep,
        "resp_snippet": snippet,
        "status_code": sc,
    }
