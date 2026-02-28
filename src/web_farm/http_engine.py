from __future__ import annotations

"""
http_engine.py — единый “двигатель” HTTP для парсинга:
requests.Session + rate limit (per-domain) + retry/backoff.

Ключевые фичи (универсально):
- TokenBucket со стартом "полным ведром" (start_full=True)
- SlidingWindow (если нужно N запросов за окно)
- MinDelayWrapper (минимальная пауза между запросами + jitter)
- Retry-After: поддержка секунд и HTTP-date
- Фабрики из dict-конфига (под profile._meta.http):
  - make_limiter_factory_from_cfg
  - make_retry_policy_from_cfg
  - make_http_engine_from_meta
"""

import random
import re
import sys
import threading
import time
import json
import hashlib
from dataclasses import dataclass, field
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from typing import Any, Callable, Iterator, Optional, Sequence
from urllib.parse import urlparse

import requests
from requests.structures import CaseInsensitiveDict

from .resp_read import JSONType, safe_read_json




# =========================
# Headers presets (HTML vs JSON) + block hints
# =========================

DEFAULT_HTML_HEADERS: dict[str, str] = {
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
}

DEFAULT_JSON_HEADERS: dict[str, str] = {
    "Accept": "application/json, text/plain, */*",
}

def _looks_like_api_url(url: str) -> bool:
    u = url.lower()
    return (
        "/api/" in u
        or "/graphql" in u
        or "graphql" in u
        or u.endswith(".json")
        or "format=json" in u
    )

def _block_hint(resp: requests.Response) -> Optional[str]:
    """Очень грубая эвристика: помогает в отладке, но не является 'детектором'."""
    try:
        sc = resp.status_code
        if sc not in (401, 403, 429):
            return None
        h = {k.lower(): v for k, v in (resp.headers or {}).items()}
        txt = ""
        try:
            txt = (resp.text or "").lower()
        except Exception:
            txt = ""
        if "cf-ray" in h or "cloudflare" in (h.get("server", "").lower()):
            return "cloudflare"
        if "captcha" in txt or "g-recaptcha" in txt or "hcaptcha" in txt:
            return "captcha"
        if "checking your browser" in txt or "just a moment" in txt:
            return "js_challenge"
        if "access denied" in txt or "forbidden" in txt:
            return "access_denied"
        if sc == 429:
            return "rate_limited"
        if sc in (401, 403):
            # может быть просто auth
            if "login" in txt or "sign in" in txt or "authorization" in txt:
                return "auth_required"
        return None
    except Exception:
        return None

def _sec_headers(mode: str) -> dict[str, str]:
    """Минимальный набор sec-*; используем только как optional fallback."""
    # Эти значения не обязаны быть идеальными — это мягкая попытка приблизить запрос к браузеру.
    base = {
        "sec-ch-ua": '"Chromium";v="122", "Google Chrome";v="122", "Not(A:Brand";v="99"',
        "sec-ch-ua-mobile": "?0",
        "sec-ch-ua-platform": '"Windows"',
    }
    if mode == "json":
        base.update({
            "sec-fetch-site": "same-origin",
            "sec-fetch-mode": "cors",
            "sec-fetch-dest": "empty",
        })
    else:
        base.update({
            "sec-fetch-site": "none",
            "sec-fetch-mode": "navigate",
            "sec-fetch-dest": "document",
            "sec-fetch-user": "?1",
        })
    return base

def _match_domain(rule: str, domain: str) -> bool:
    if not rule:
        return False
    rule = rule.lower().strip()
    domain = domain.lower().strip()
    if rule.startswith("."):
        return domain.endswith(rule[1:])
    return domain == rule


# =========================
# Rate limit (стратегии)
# =========================

class RateLimiter:
    """Интерфейс ограничителя: вернуть сколько секунд ждать перед запросом."""
    def acquire(self) -> float:
        raise NotImplementedError


@dataclass
class TokenBucket(RateLimiter):
    """
    TokenBucket: можно сделать “несколько быстрых запросов”, затем ждать.

    Параметры:
    - rate_per_sec: сколько токенов добавляется в секунду (средняя скорость)
    - capacity: максимум токенов в ведре (разовый “рывок”)
    - start_full: если True, ведро стартует полным (первый запрос без ожидания)
    """
    rate_per_sec: float
    capacity: float
    start_full: bool = True

    tokens: float = field(default=0.0)
    last_ts: float = field(default_factory=time.monotonic)
    _lock: threading.Lock = field(default_factory=threading.Lock, repr=False)

    def __post_init__(self) -> None:
        if self.start_full and self.tokens <= 0.0:
            self.tokens = float(self.capacity)

    def acquire(self) -> float:
        with self._lock:
            now = time.monotonic()
            elapsed = max(0.0, now - self.last_ts)
            self.last_ts = now

            self.tokens = min(self.capacity, self.tokens + elapsed * self.rate_per_sec)
            if self.tokens >= 1.0:
                self.tokens -= 1.0
                return 0.0

            need = 1.0 - self.tokens
            wait = need / max(self.rate_per_sec, 1e-9)
            self.tokens = 0.0  # резервируем
            return float(max(0.0, wait))


@dataclass
class SlidingWindow(RateLimiter):
    """N запросов за window_sec (равномернее для некоторых сайтов)."""
    max_requests: int
    window_sec: float
    stamps: list[float] = field(default_factory=list)
    _lock: threading.Lock = field(default_factory=threading.Lock, repr=False)

    def acquire(self) -> float:
        with self._lock:
            now = time.monotonic()
            cutoff = now - self.window_sec
            self.stamps = [t for t in self.stamps if t >= cutoff]

            if len(self.stamps) < self.max_requests:
                self.stamps.append(now)
                return 0.0

            earliest = min(self.stamps) if self.stamps else now
            wait = (earliest + self.window_sec) - now
            return float(max(0.0, wait))


@dataclass
class MinDelayWrapper(RateLimiter):
    """
    Обёртка: добавляет min_delay между запросами + jitter (случайный разброс).
    wait = max(inner_wait, respect_min_delay) + random(0..jitter)
    """
    inner: RateLimiter
    min_delay: float = 0.0
    jitter: float = 0.0
    _next_allowed_ts: float = field(default=0.0)
    _lock: threading.Lock = field(default_factory=threading.Lock, repr=False)

    def acquire(self) -> float:
        inner_wait = float(self.inner.acquire())
        now = time.monotonic()
        with self._lock:
            wait_min = 0.0
            if self._next_allowed_ts > now:
                wait_min = self._next_allowed_ts - now

            wait = max(inner_wait, wait_min)
            if self.jitter > 0:
                wait += random.uniform(0.0, self.jitter)

            self._next_allowed_ts = now + wait + max(0.0, self.min_delay)
        return float(max(0.0, wait))


def make_limiter_factory_from_cfg(cfg: dict[str, Any]) -> Callable[[str], RateLimiter]:
    """
    Собрать limiter_factory(domain)->RateLimiter из dict-конфига.

    cfg пример:
    {
      "kind": "token_bucket" | "sliding_window",
      "rate_per_sec": 1.0, "capacity": 2, "start_full": true,
      "max_requests": 2, "window_sec": 1.0,
      "min_delay_ms": 300, "jitter_ms": 200
    }
    """
    kind = str(cfg.get("kind") or "token_bucket").lower()
    min_delay = float(cfg.get("min_delay_ms", 0) or 0) / 1000.0
    jitter = float(cfg.get("jitter_ms", 0) or 0) / 1000.0

    def factory(_domain: str) -> RateLimiter:
        if kind in ("sliding_window", "window"):
            rl: RateLimiter = SlidingWindow(
                max_requests=int(cfg.get("max_requests", 2)),
                window_sec=float(cfg.get("window_sec", 1.0)),
            )
        else:
            rl = TokenBucket(
                rate_per_sec=float(cfg.get("rate_per_sec", 1.0)),
                capacity=float(cfg.get("capacity", 2.0)),
                start_full=bool(cfg.get("start_full", True)),
            )
        if min_delay > 0 or jitter > 0:
            rl = MinDelayWrapper(inner=rl, min_delay=min_delay, jitter=jitter)
        return rl

    return factory


# =========================
# Конфиг -> limiter_factory
# =========================

def limiter_from_cfg(cfg: dict[str, Any]) -> RateLimiter:
    """Собрать RateLimiter из dict-конфига.

    Поддержка:
    - kind: token_bucket | sliding_window | none
    - token_bucket: rate_per_sec, capacity, start_full
    - sliding_window: max_requests, window_sec
    - min_delay / jitter (секунды) или min_delay_ms / jitter_ms (миллисекунды)
    """
    cfg = dict(cfg or {})
    kind = str(cfg.get("kind") or "token_bucket").lower()

    if kind in ("none", "off", "disabled"):
        inner: RateLimiter = TokenBucket(rate_per_sec=1e9, capacity=1e9, start_full=True)
    elif kind in ("sliding_window", "window"):
        inner = SlidingWindow(
            max_requests=int(cfg.get("max_requests", 10)),
            window_sec=float(cfg.get("window_sec", 1.0)),
        )
    else:
        inner = TokenBucket(
            rate_per_sec=float(cfg.get("rate_per_sec", 1.0)),
            capacity=float(cfg.get("capacity", 2.0)),
            start_full=bool(cfg.get("start_full", True)),
        )

    # задержки
    min_delay = cfg.get("min_delay")
    jitter = cfg.get("jitter")

    if min_delay is None and cfg.get("min_delay_ms") is not None:
        min_delay = float(cfg.get("min_delay_ms") or 0.0) / 1000.0
    if jitter is None and cfg.get("jitter_ms") is not None:
        jitter = float(cfg.get("jitter_ms") or 0.0) / 1000.0

    min_delay_f = float(min_delay or 0.0)
    jitter_f = float(jitter or 0.0)

    if min_delay_f or jitter_f:
        return MinDelayWrapper(inner=inner, min_delay=min_delay_f, jitter=jitter_f)
    return inner


def build_limiter_factory(http_cfg: dict[str, Any] | None) -> Callable[[str], RateLimiter]:
    """Собрать limiter_factory(domain)->RateLimiter из meta.http.

    Поддержка 2 форматов:
    1) meta.http.limiters = { "*": {...}, "api.site.com": {...}, ".site.com": {...} }
    2) meta.http.rate_limit = {..., scope: "domain"|"global"}

    scope=domain: отдельный limiter на домен.
    scope=global: один общий limiter на все домены.
    """
    http_cfg = dict(http_cfg or {})

    # формат 1: limiters
    limiters = http_cfg.get("limiters")
    if isinstance(limiters, dict) and limiters:
        limiters_map = dict(limiters)

        def factory(domain: str) -> RateLimiter:
            cfg = None
            if domain in limiters_map:
                cfg = limiters_map.get(domain)
            else:
                for k, v in limiters_map.items():
                    if k and k != "*" and isinstance(k, str) and domain.endswith(k):
                        cfg = v
                        break
            if cfg is None:
                cfg = limiters_map.get("*") or {}
            if not isinstance(cfg, dict):
                cfg = {}
            return limiter_from_cfg(cfg)

        return factory

    # формат 2: rate_limit
    rl_cfg = http_cfg.get("rate_limit") or {}
    if not isinstance(rl_cfg, dict):
        rl_cfg = {}

    scope = str(rl_cfg.get("scope") or "domain").lower()
    if scope == "global":
        shared = limiter_from_cfg(rl_cfg)
        return lambda _domain: shared

    # domain (по умолчанию)
    return lambda _domain: limiter_from_cfg(rl_cfg)

# =========================
# Retry / backoff
# =========================

@dataclass
class RetryPolicy:
    max_attempts: int = 4
    base_delay: float = 0.5
    cap_delay: float = 8.0
    jitter: str = "full"  # none | full
    retry_statuses: tuple[int, ...] = (429, 500, 502, 503, 504)
    respect_retry_after: bool = True


def make_retry_policy_from_cfg(cfg: dict[str, Any]) -> RetryPolicy:
    if not isinstance(cfg, dict):
        return RetryPolicy()
    return RetryPolicy(
        max_attempts=int(cfg.get("max_attempts", 4)),
        base_delay=float(cfg.get("base_delay", 0.5)),
        cap_delay=float(cfg.get("cap_delay", 8.0)),
        jitter=str(cfg.get("jitter", "full")),
        retry_statuses=tuple(int(x) for x in (cfg.get("retry_statuses") or (429, 500, 502, 503, 504))),
        respect_retry_after=bool(cfg.get("respect_retry_after", True)),
    )


def _backoff_delay(attempt: int, pol: RetryPolicy) -> float:
    exp = pol.base_delay * (2 ** max(0, attempt - 1))
    delay = min(pol.cap_delay, exp)
    if pol.jitter == "full":
        return float(random.uniform(0.0, delay))
    return float(delay)


def _domain_of(url: str) -> str:
    try:
        return urlparse(url).netloc.lower()
    except Exception:
        return ""


def _retry_after_seconds(resp: requests.Response) -> Optional[float]:
    ra = (resp.headers.get("Retry-After") or "").strip()
    if not ra:
        return None
    try:
        sec = float(ra)
        return sec if sec > 0 else None
    except Exception:
        pass
    try:
        dt = parsedate_to_datetime(ra)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        now = datetime.now(timezone.utc)
        sec = (dt - now).total_seconds()
        return sec if sec > 0 else None
    except Exception:
        return None


@dataclass
class FetchResult:
    url: str
    ok: bool
    status_code: Optional[int]
    elapsed_ms: int
    error: Optional[str] = None
    response: Optional[requests.Response] = None


# =========================
# HttpEngine
# =========================

class HttpEngine:
    """Единая точка выполнения HTTP-запросов (rate-limit + retry)."""

    def __init__(
        self,
        *,
        default_timeout: float = 10.0,
        default_headers: Optional[dict[str, str]] = None,
        retry_policy: Optional[RetryPolicy] = None,
        limiter_factory: Optional[Callable[[str], RateLimiter]] = None,
        auth_hook: Optional[Callable[[requests.Session, str, dict[str, Any], dict[str, str]], None]] = None,
        headers_cfg: Optional[dict[str, Any]] = None,
        diag_http: bool = False,
        session: Optional[requests.Session] = None,
        cache_dir: Optional[str] = None,
        replay: bool = False,
        cache_store_statuses: Optional[Sequence[int]] = None,
    ) -> None:
        self.default_timeout = float(default_timeout)
        self.default_headers = dict(default_headers or {})
        self.retry_policy = retry_policy or RetryPolicy()
        self.headers_cfg: dict[str, Any] = dict(headers_cfg or {})
        self.diag_http = bool(diag_http)
        self.last_diag: Optional[dict[str, Any]] = None
        self._limiters: dict[str, RateLimiter] = {}
        self._limiter_factory = limiter_factory or (lambda _d: TokenBucket(rate_per_sec=1.0, capacity=2.0, start_full=True))
        self.session = session or requests.Session()

        # auth hook (секреты/cookies/etc)
        self._auth_hook = auth_hook

        # response cache / replay
        self.cache_dir = cache_dir
        self.replay = bool(replay)
        self.cache_store_statuses = set(int(x) for x in (cache_store_statuses or [200, 201, 202, 203, 204, 206, 301, 302, 304]))
        if self.cache_dir:
            from pathlib import Path
            Path(self.cache_dir).mkdir(parents=True, exist_ok=True)

    def _resolve_headers_cfg(self, url: str) -> dict[str, Any]:
        cfg = dict(self.headers_cfg or {})
        by_domain = cfg.get("by_domain")
        if isinstance(by_domain, dict):
            domain = _domain_of(url)
            # merge matching domain rules
            for k, v in by_domain.items():
                if isinstance(k, str) and isinstance(v, dict) and _match_domain(k, domain):
                    # shallow merge is enough for our keys
                    for kk, vv in v.items():
                        cfg[kk] = vv
        return cfg

    def _choose_mode(self, url: str, *, expect: str, json_body: Any, headers: dict[str, str]) -> str:
        exp = (expect or "auto").lower()
        if exp in ("html", "json"):
            return exp
        # auto
        if json_body is not None:
            return "json"
        acc = (headers.get("Accept") or "").lower()
        if "application/json" in acc:
            return "json"
        if _looks_like_api_url(url):
            return "json"
        return "html"

    def _mode_headers(self, mode: str, cfg: dict[str, Any]) -> dict[str, str]:
        base = DEFAULT_JSON_HEADERS if mode == "json" else DEFAULT_HTML_HEADERS
        out = dict(base)
        extra = cfg.get(mode)
        if isinstance(extra, dict):
            out.update({str(k): str(v) for k, v in extra.items()})
        return out

    def _fallback_cfg(self, cfg: dict[str, Any]) -> dict[str, Any]:
        fb = cfg.get("browser_fallback")
        return fb if isinstance(fb, dict) else {}

    def _emit_diag(self, d: dict[str, Any]) -> None:
        self.last_diag = d
        if not self.diag_http:
            return
        # короткая строка в stderr
        parts = [
            f"[HTTP] {d.get('method')} {d.get('domain')} sc={d.get('status')} err={d.get('err')}",
            f"mode={d.get('mode')} sec={d.get('sec_used')} try={d.get('attempt')}/{d.get('max_attempts')}",
            f"elapsed={d.get('elapsed_ms')}ms",
        ]
        if d.get("hint"):
            parts.append(f"hint={d['hint']}")
        # URL может быть длинным — но для отладки полезно
        parts.append(f"url={d.get('url')}")
        sys.stderr.write(' '.join(parts) + "\n")

    # --------- cache helpers ---------

    def _cache_key(
        self,
        *,
        method: str,
        url: str,
        params: dict[str, Any],
        data: Any,
        json_body: Any,
        headers: dict[str, str],
        mode: str,
    ) -> str:
        """Стабильный ключ для кэша.

        Включаем только то, что реально влияет на ответ.
        Заголовки берём минимально (Accept/Referer/Origin), чтобы кэш не ломался от мелкого шума.
        """
        key_obj = {
            "m": str(method).upper(),
            "u": str(url),
            "p": params or {},
            "d": data or None,
            "j": json_body if json_body is not None else None,
            "h": {
                "Accept": headers.get("Accept"),
                "Referer": headers.get("Referer"),
                "Origin": headers.get("Origin"),
            },
            "mode": mode,
        }
        blob = json.dumps(key_obj, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode("utf-8")
        return hashlib.sha1(blob).hexdigest()

    def _cache_paths(self, key: str) -> tuple[str, str]:
        assert self.cache_dir
        return (
            f"{self.cache_dir}/{key}.meta.json",
            f"{self.cache_dir}/{key}.body",
        )

    def _cache_load(self, key: str, *, url: str) -> Optional[requests.Response]:
        if not self.cache_dir:
            return None
        meta_path, body_path = self._cache_paths(key)
        try:
            with open(meta_path, "r", encoding="utf-8") as f:
                meta = json.load(f)
            with open(body_path, "rb") as f:
                body = f.read()
        except FileNotFoundError:
            return None
        except Exception:
            return None

        resp = requests.Response()
        resp.status_code = int(meta.get("status_code") or 200)
        resp._content = body
        hdrs = meta.get("headers")
        if isinstance(hdrs, dict):
            resp.headers = CaseInsensitiveDict({str(k): str(v) for k, v in hdrs.items()})
        else:
            resp.headers = CaseInsensitiveDict()
        resp.url = url
        enc = meta.get("encoding")
        if isinstance(enc, str) and enc:
            resp.encoding = enc
        return resp

    def _cache_save(self, key: str, resp: requests.Response) -> None:
        if not self.cache_dir:
            return
        try:
            if int(resp.status_code) not in self.cache_store_statuses:
                return
        except Exception:
            return

        meta_path, body_path = self._cache_paths(key)
        try:
            meta = {
                "status_code": int(resp.status_code),
                "headers": dict(resp.headers or {}),
                "encoding": resp.encoding,
            }
            with open(meta_path, "w", encoding="utf-8") as f:
                json.dump(meta, f, ensure_ascii=False, indent=2)
            with open(body_path, "wb") as f:
                f.write(resp.content or b"")
        except Exception:
            return

    def _get_limiter(self, domain: str) -> RateLimiter:
        if domain not in self._limiters:
            self._limiters[domain] = self._limiter_factory(domain)
        return self._limiters[domain]

    def _sleep(self, sec: float) -> None:
        if sec and sec > 0:
            time.sleep(float(sec))

    def request(
        self,
        url: str,
        *,
        method: str = "GET",
        params: Optional[dict[str, Any]] = None,
        headers: Optional[dict[str, str]] = None,
        data: Optional[dict[str, Any]] = None,
        json_body: Optional[Any] = None,
        timeout: Optional[float] = None,
        allow_redirects: bool = True,
        expect: str = "auto",
    ) -> tuple[Optional[requests.Response], Optional[str], int]:
        domain = _domain_of(url)
        limiter = self._get_limiter(domain)
        pol = self.retry_policy

        merged_params: dict[str, Any] = dict(params or {})

        cfg = self._resolve_headers_cfg(url)
        cfg_mode = str(cfg.get("mode") or "auto").lower() if isinstance(cfg, dict) else "auto"
        if cfg_mode in ("html", "json"):
            expect = cfg_mode
        # порядок важен: default_headers -> mode_headers -> request_headers
        base_headers = dict(self.default_headers)
        mode0 = self._choose_mode(url, expect=expect, json_body=json_body, headers=base_headers)
        mode_headers = self._mode_headers(mode0, cfg)
        merged_headers = dict(base_headers)
        merged_headers.update(mode_headers)
        if headers:
            merged_headers.update(headers)

        if self._auth_hook is not None:
            # auth_hook может: (1) добавить/переписать headers, (2) добавить query-параметры,
            # (3) подгрузить cookies в session (один раз/по необходимости).
            self._auth_hook(self.session, url, merged_params, merged_headers)

        # cache/replay: пробуем до реального запроса
        cache_key: Optional[str] = None
        if self.cache_dir:
            cache_key = self._cache_key(
                method=method,
                url=url,
                params=merged_params,
                data=data,
                json_body=json_body,
                headers=merged_headers,
                mode=mode0,
            )
            if self.replay:
                cached = self._cache_load(cache_key, url=url)
                if cached is not None:
                    return cached, None, 0
                return None, "cache_miss", 0

        last_err: Optional[str] = None
        start_all = time.monotonic()

        for attempt in range(1, pol.max_attempts + 1):
            self._sleep(limiter.acquire())

            t0 = time.monotonic()
            try:
                resp = self.session.request(
                    method=method,
                    url=url,
                    params=merged_params,
                    headers=merged_headers,
                    data=data,
                    json=json_body,
                    timeout=float(timeout or self.default_timeout),
                    allow_redirects=allow_redirects,
                )
                elapsed_ms = int((time.monotonic() - t0) * 1000)
            except requests.Timeout:
                last_err = "timeout"
                resp = None
                elapsed_ms = int((time.monotonic() - t0) * 1000)
            except requests.RequestException as e:
                last_err = f"network_error:{type(e).__name__}"
                resp = None
                elapsed_ms = int((time.monotonic() - t0) * 1000)

            if resp is None and last_err is not None:
                self._emit_diag({
                    "url": url,
                    "domain": domain,
                    "method": method,
                    "status": None,
                    "err": last_err,
                    "mode": mode0,
                    "sec_used": 0,
                    "attempt": attempt,
                    "max_attempts": pol.max_attempts,
                    "elapsed_ms": elapsed_ms,
                    "hint": None,
                })

            if resp is not None:
                sc = resp.status_code
                if 200 <= sc < 400:
                    if cache_key is not None:
                        self._cache_save(cache_key, resp)
                    return resp, None, elapsed_ms

                # optional browser_fallback: sec-* headers + (optional) Playwright stage
                fb = self._fallback_cfg(cfg)
                fb_enabled = bool(fb.get("enabled"))
                on_status = fb.get("on_status") or [403]
                max_fb = int(fb.get("max_tries") or 1)
                sec_used = 0
                hint0 = _block_hint(resp) if resp is not None else None
                strategy = str(fb.get("strategy") or "sec_headers").lower()
                pw_cfg = fb.get("playwright") if isinstance(fb.get("playwright"), dict) else {}
                pw_enabled = bool(pw_cfg.get("enabled")) or strategy.startswith("playwright")
                on_hint = set(str(x) for x in (fb.get("on_hint") or []) if str(x).strip())

                # --- 1) SEC-headers fallback (default, cheap) ---
                while (fb_enabled and strategy in ("sec_headers", "auto", "mixed")
                       and sc in set(int(x) for x in on_status) and sec_used < max_fb):
                    sec_used += 1
                    fb_headers = dict(merged_headers)
                    fb_headers.update(_sec_headers(mode0))
                    if self._auth_hook is not None:
                        self._auth_hook(self.session, url, merged_params, fb_headers)
                    self._sleep(limiter.acquire())
                    t1 = time.monotonic()
                    try:
                        resp2 = self.session.request(
                            method=method,
                            url=url,
                            params=merged_params,
                            headers=fb_headers,
                            data=data,
                            json=json_body,
                            timeout=float(timeout or self.default_timeout),
                            allow_redirects=allow_redirects,
                        )
                        elapsed_ms = int((time.monotonic() - t1) * 1000)
                        resp = resp2
                        sc = resp.status_code
                        merged_headers = fb_headers
                        if 200 <= sc < 400:
                            if cache_key is not None:
                                self._cache_save(cache_key, resp)
                            return resp, None, elapsed_ms
                        break
                    except requests.Timeout:
                        last_err = "timeout"
                        resp = None
                        elapsed_ms = int((time.monotonic() - t1) * 1000)
                        break
                    except requests.RequestException as e:
                        last_err = f"network_error:{type(e).__name__}"
                        resp = None
                        elapsed_ms = int((time.monotonic() - t1) * 1000)
                        break

                # --- 2) Playwright stage (optional, heavy) ---
                hint1 = _block_hint(resp) if resp is not None else hint0
                if fb_enabled and pw_enabled and (
                    sc in set(int(x) for x in on_status) or (hint1 is not None and hint1 in on_hint)
                ):
                    try:
                        from .browser_engine import render_html, prime_cookies_into_session, make_response_from_html
                    except Exception:
                        render_html = None
                        prime_cookies_into_session = None
                        make_response_from_html = None

                    pw_mode = str(pw_cfg.get("mode") or "").lower()
                    if not pw_mode:
                        if strategy == "playwright_html":
                            pw_mode = "render_html"
                        elif strategy == "playwright_prime":
                            pw_mode = "prime_cookies"
                        else:
                            pw_mode = "prime_cookies"

                    if pw_mode == "prime_cookies" and prime_cookies_into_session is not None:
                        okp, errp, _ms = prime_cookies_into_session(
                            session=self.session,
                            url=url,
                            params=merged_params,
                            headers=merged_headers,
                            cfg=pw_cfg,
                        )
                        if okp:
                            self._sleep(limiter.acquire())
                            t2 = time.monotonic()
                            try:
                                resp3 = self.session.request(
                                    method=method,
                                    url=url,
                                    params=merged_params,
                                    headers=merged_headers,
                                    data=data,
                                    json=json_body,
                                    timeout=float(timeout or self.default_timeout),
                                    allow_redirects=allow_redirects,
                                )
                                elapsed_ms = int((time.monotonic() - t2) * 1000)
                                resp = resp3
                                sc = resp.status_code
                                if 200 <= sc < 400:
                                    if cache_key is not None:
                                        self._cache_save(cache_key, resp)
                                    return resp, None, elapsed_ms
                            except requests.Timeout:
                                last_err = "timeout"
                                resp = None
                            except requests.RequestException as e:
                                last_err = f"network_error:{type(e).__name__}"
                                resp = None
                        else:
                            last_err = str(errp or "playwright_prime_failed")

                    elif pw_mode == "render_html" and render_html is not None and make_response_from_html is not None:
                        r = render_html(url=url, params=merged_params, headers=merged_headers, cfg=pw_cfg)
                        if r.ok:
                            resp4 = make_response_from_html(r.url_final, r.status_code, r.html, headers=None)
                            elapsed_ms = int(r.elapsed_ms)
                            resp = resp4
                            sc = resp.status_code
                            if 200 <= sc < 400:
                                if cache_key is not None:
                                    self._cache_save(cache_key, resp)
                                return resp, None, elapsed_ms
                        else:
                            last_err = str(r.error or "playwright_render_failed")

                fb = self._fallback_cfg(cfg)
                fb_enabled = bool(fb.get("enabled"))
                on_status = fb.get("on_status") or [403]
                max_fb = int(fb.get("max_tries") or 1)
                sec_used = 0
                # try once (or max_fb) with extra sec-* only for selected statuses
                while fb_enabled and sc in set(int(x) for x in on_status) and sec_used < max_fb:
                    sec_used += 1
                    fb_headers = dict(merged_headers)
                    fb_headers.update(_sec_headers(mode0))
                    # auth может переписать заголовки, поэтому применяем hook снова
                    if self._auth_hook is not None:
                        self._auth_hook(self.session, url, merged_params, fb_headers)
                    self._sleep(limiter.acquire())
                    t1 = time.monotonic()
                    try:
                        resp2 = self.session.request(
                            method=method,
                            url=url,
                            params=merged_params,
                            headers=fb_headers,
                            data=data,
                            json=json_body,
                            timeout=float(timeout or self.default_timeout),
                            allow_redirects=allow_redirects,
                        )
                        elapsed_ms = int((time.monotonic() - t1) * 1000)
                        resp = resp2
                        sc = resp.status_code
                        merged_headers = fb_headers
                        if 200 <= sc < 400:
                            if cache_key is not None:
                                self._cache_save(cache_key, resp)
                            return resp, None, elapsed_ms
                        # если снова блок — выходим и пойдём по retry_policy
                        break
                    except requests.Timeout:
                        last_err = "timeout"
                        resp = None
                        elapsed_ms = int((time.monotonic() - t1) * 1000)
                        break
                    except requests.RequestException as e:
                        last_err = f"network_error:{type(e).__name__}"
                        resp = None
                        elapsed_ms = int((time.monotonic() - t1) * 1000)
                        break

                last_err = f"http_{sc}"
                hint = _block_hint(resp) if resp is not None else None
                self._emit_diag({
                    "url": url,
                    "domain": domain,
                    "method": method,
                    "status": sc,
                    "err": last_err,
                    "mode": mode0,
                    "sec_used": sec_used,
                    "attempt": attempt,
                    "max_attempts": pol.max_attempts,
                    "elapsed_ms": elapsed_ms,
                    "hint": hint,
                })
                if sc not in pol.retry_statuses or attempt >= pol.max_attempts:
                    if cache_key is not None and resp is not None:
                        self._cache_save(cache_key, resp)
                    return resp, last_err, elapsed_ms


                if pol.respect_retry_after:
                    ra = _retry_after_seconds(resp)
                    if ra is not None and ra > 0:
                        self._sleep(ra)
                        continue

            if attempt >= pol.max_attempts:
                break

            self._sleep(_backoff_delay(attempt, pol))

        elapsed_ms = int((time.monotonic() - start_all) * 1000)
        return None, last_err or "request_failed", elapsed_ms

    def safe_get_json(
        self,
        url: str,
        *,
        method: str = "GET",
        params: Optional[dict[str, Any]] = None,
        headers: Optional[dict[str, str]] = None,
        data: Optional[dict[str, Any]] = None,
        json_body: Optional[Any] = None,
        timeout: Optional[float] = None,
        allow_redirects: bool = True,
        force_json: bool = False,
        detect_soft: bool = True,
    ) -> tuple[Optional[requests.Response], Optional[JSONType], Optional[str]]:
        resp, err, _ms = self.request(
            url,
            method=method,
            params=params,
            headers=headers,
            data=data,
            json_body=json_body,
            timeout=timeout,
            allow_redirects=allow_redirects,
            expect="json",
        )
        if resp is None:
            return None, None, err

        if err is not None and err.startswith("http_") and not (200 <= resp.status_code < 400):
            return resp, None, err

        jr = safe_read_json(resp, force=force_json, detect_soft=detect_soft)
        if not jr.ok:
            msg = (jr.error or "json_read_error")
            if jr.details:
                msg += f":{jr.details}"
            return resp, jr.data, msg

        return resp, jr.data, None

    def fetch_many(
        self,
        urls: Sequence[str],
        *,
        method: str = "GET",
        params: Optional[dict[str, Any]] = None,
        headers: Optional[dict[str, str]] = None,
        timeout: Optional[float] = None,
    ) -> Iterator[FetchResult]:
        for url in urls:
            t0 = time.monotonic()
            resp, err, _ = self.request(url, method=method, params=params, headers=headers, timeout=timeout)
            elapsed_ms = int((time.monotonic() - t0) * 1000)
            if resp is None:
                yield FetchResult(url=url, ok=False, status_code=None, elapsed_ms=elapsed_ms, error=err, response=None)
            else:
                ok = err is None and (200 <= resp.status_code < 400)
                yield FetchResult(url=url, ok=ok, status_code=resp.status_code, elapsed_ms=elapsed_ms, error=err, response=resp)


def make_http_engine_from_meta(
    http_meta: dict[str, Any],
    *,
    default_timeout: float = 10.0,
    default_headers: Optional[dict[str, str]] = None,
    session: Optional[requests.Session] = None,
) -> HttpEngine:
    """
    Собрать HttpEngine из http_meta (обычно profile.meta.get("http") или profile.meta["_meta"]["http"]).

    Ожидаемые ключи:
      http_meta["rate_limit"] -> dict для make_limiter_factory_from_cfg
      http_meta["retries"]    -> dict для make_retry_policy_from_cfg
    """
    rl_cfg = http_meta.get("rate_limit") if isinstance(http_meta, dict) else None
    rt_cfg = http_meta.get("retries") if isinstance(http_meta, dict) else None
    hd_cfg = http_meta.get("headers") if isinstance(http_meta, dict) else None
    cache_cfg = http_meta.get("cache") if isinstance(http_meta, dict) else None
    diag = bool(http_meta.get("diag_http")) if isinstance(http_meta, dict) else False

    limiter_factory = make_limiter_factory_from_cfg(rl_cfg or {}) if isinstance(rl_cfg, dict) else None
    retry_policy = make_retry_policy_from_cfg(rt_cfg or {}) if isinstance(rt_cfg, dict) else None

    cache_dir = None
    replay = False
    store_statuses: Optional[list[int]] = None
    if isinstance(cache_cfg, dict):
        cache_dir = cache_cfg.get("dir")
        replay = bool(cache_cfg.get("replay"))
        sts = cache_cfg.get("store_statuses")
        if isinstance(sts, list) and sts:
            try:
                store_statuses = [int(x) for x in sts]
            except Exception:
                store_statuses = None

    return HttpEngine(
        default_timeout=default_timeout,
        default_headers=default_headers,
        retry_policy=retry_policy,
        limiter_factory=limiter_factory,
        headers_cfg=hd_cfg if isinstance(hd_cfg, dict) else None,
        diag_http=diag,
        session=session,
        cache_dir=str(cache_dir) if isinstance(cache_dir, str) and cache_dir else None,
        replay=replay,
        cache_store_statuses=store_statuses,
    )
