from __future__ import annotations

"""browser_engine.py — optional Playwright stage for JS-rendered pages.

Goals:
- Optional dependency (Playwright may be absent).
- Integrate into HttpEngine by returning requests.Response objects.
- Strategies:
  - render_html: return final DOM HTML as Response
  - prime_cookies: navigate, copy cookies into requests.Session, then HttpEngine retries with requests

Safety:
- No CAPTCHA bypass. If captcha markers are detected, returns error "captcha_detected".
"""

from dataclasses import dataclass
from typing import Any, Optional, Tuple, List
from urllib.parse import urlencode, urlsplit, urlunsplit, parse_qsl

import time
import requests

CAPTCHA_MARKERS = ("g-recaptcha", "hcaptcha", "cf-captcha", "captcha")


def _build_url(url: str, params: Optional[dict[str, Any]]) -> str:
    if not params:
        return url
    parts = urlsplit(url)
    q = dict(parse_qsl(parts.query, keep_blank_values=True))
    for k, v in params.items():
        if v is None:
            continue
        q[str(k)] = str(v)
    return urlunsplit((parts.scheme, parts.netloc, parts.path, urlencode(q, doseq=True), parts.fragment))


def _safe_extra_headers(headers: dict[str, str]) -> dict[str, str]:
    drop = {"host", "content-length", "connection", "transfer-encoding"}
    out: dict[str, str] = {}
    for k, v in (headers or {}).items():
        if str(k).lower() in drop:
            continue
        out[str(k)] = str(v)
    return out


def _detect_captcha(html: str) -> bool:
    low = (html or "").lower()
    return any(m in low for m in CAPTCHA_MARKERS)


@dataclass
class BrowserResult:
    ok: bool
    status_code: Optional[int]
    elapsed_ms: int
    url_final: str
    html: str = ""
    cookies: List[dict[str, Any]] | None = None
    error: Optional[str] = None


def _pw_import():
    try:
        from playwright.sync_api import sync_playwright, TimeoutError as PwTimeoutError  # type: ignore
        return sync_playwright, PwTimeoutError, None
    except Exception as e:
        return None, None, e


def render_html(*, url: str, params: Optional[dict[str, Any]], headers: dict[str, str], cfg: dict[str, Any]) -> BrowserResult:
    sync_playwright, PwTimeoutError, imp_err = _pw_import()
    if imp_err is not None or sync_playwright is None:
        return BrowserResult(False, None, 0, url_final=url, error=f"playwright_not_installed:{imp_err}")

    headless = bool(cfg.get("headless", True))
    browser_name = str(cfg.get("browser") or "chromium").lower()
    timeout_ms = int(cfg.get("timeout_ms") or 30000)
    wait_until = str(cfg.get("wait_until") or "networkidle")
    wait_selector = str(cfg.get("wait_selector") or "").strip()
    viewport = cfg.get("viewport") if isinstance(cfg.get("viewport"), dict) else {}
    vp_w = int(viewport.get("width") or 1280)
    vp_h = int(viewport.get("height") or 720)

    ua = str(cfg.get("user_agent") or headers.get("User-Agent") or "")
    extra = _safe_extra_headers(headers)
    full_url = _build_url(url, params)

    t0 = time.monotonic()
    with sync_playwright() as p:
        btype = getattr(p, browser_name, None) or p.chromium
        browser = btype.launch(headless=headless)
        context = browser.new_context(
            user_agent=(ua or None),
            extra_http_headers=(extra or None),
            viewport={"width": vp_w, "height": vp_h},
        )
        page = context.new_page()
        try:
            resp = page.goto(full_url, wait_until=wait_until, timeout=timeout_ms)
            if wait_selector:
                page.wait_for_selector(wait_selector, timeout=timeout_ms)

            actions = cfg.get("actions") if isinstance(cfg.get("actions"), list) else []
            for a in actions:
                if not isinstance(a, dict):
                    continue
                t = str(a.get("type") or "").lower()
                if t == "scroll":
                    times = int(a.get("times") or 1)
                    delay = int(a.get("delay_ms") or 300)
                    for _ in range(max(1, times)):
                        page.mouse.wheel(0, 20000)
                        page.wait_for_timeout(delay)
                elif t == "click":
                    sel = str(a.get("selector") or "")
                    if sel:
                        page.click(sel, timeout=timeout_ms)
                        page.wait_for_timeout(int(a.get("delay_ms") or 300))
                elif t == "wait":
                    page.wait_for_timeout(int(a.get("ms") or 300))

            html = page.content() or ""
            final_url = page.url
            cookies = context.cookies()
            status = resp.status if resp is not None else None
            ms = int((time.monotonic() - t0) * 1000)

            if _detect_captcha(html):
                return BrowserResult(False, status, ms, final_url, html="", cookies=cookies, error="captcha_detected")
            return BrowserResult(True, status, ms, final_url, html=html, cookies=cookies, error=None)
        except PwTimeoutError:
            return BrowserResult(False, None, int((time.monotonic() - t0) * 1000), full_url, error="playwright_timeout")
        except Exception as e:
            return BrowserResult(False, None, int((time.monotonic() - t0) * 1000), full_url, error=f"playwright_error:{type(e).__name__}")
        finally:
            try:
                context.close()
            except Exception:
                pass
            try:
                browser.close()
            except Exception:
                pass


def prime_cookies_into_session(
    *,
    session: requests.Session,
    url: str,
    params: Optional[dict[str, Any]],
    headers: dict[str, str],
    cfg: dict[str, Any],
) -> Tuple[bool, Optional[str], int]:
    r = render_html(url=url, params=params, headers=headers, cfg=cfg)
    if not r.ok:
        return False, r.error or "prime_failed", r.elapsed_ms

    try:
        for c in (r.cookies or []):
            name = c.get("name")
            value = c.get("value")
            domain = c.get("domain") or ""
            if not name or value is None:
                continue
            dom = str(domain)
            if dom.startswith("."):
                dom = dom[1:]
            session.cookies.set(str(name), str(value), domain=(dom or None), path=str(c.get("path") or "/"))
    except Exception:
        pass
    return True, None, r.elapsed_ms


def make_response_from_html(url: str, status_code: int | None, html: str, *, headers: Optional[dict[str, str]] = None) -> requests.Response:
    resp = requests.Response()
    resp.status_code = int(status_code or 200)
    resp.url = url
    resp._content = (html or "").encode("utf-8", errors="replace")
    resp.encoding = "utf-8"
    resp.headers.update({"Content-Type": "text/html; charset=utf-8"})
    if headers:
        try:
            for k, v in headers.items():
                if k and v:
                    resp.headers[k] = v
        except Exception:
            pass
    return resp
