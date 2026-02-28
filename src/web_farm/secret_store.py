from __future__ import annotations

"""
secret_store.py — локальное хранилище секретов (vault), НЕ коммитится в репозиторий.

Идея:
- В .env хранится только путь к secrets-файлу (1 переменная).
- В profiles (JSON) хранится только "описание способа авторизации":
    profile._meta.auth = {"ref": "client_api"}  или  {"by_domain": {...}}
- SecretStore возвращает НЕ "строку токена", а план:
    какие headers добавить, нужно ли подгрузить cookies, нужно ли добавить query-param.

ENV:
- PARSER_SECRETS_PATH=/abs/or/relative/secrets.json
  (relative путь считается относительно текущей папки запуска)

Формат secrets.json (пример):
{
  "client_api": {"type":"bearer", "token":"..."},
  "shopify": {"type":"api_key_header", "header":"X-Shopify-Access-Token", "token":"..."},
  "site_cookies": {"type":"cookies_file", "path":"secrets/site.cookies.json"},
  "basic_demo": {"type":"basic", "username":"u", "password":"p"}
}

Поддерживаемые type:
- bearer: Authorization: Bearer <token>
- api_key_header: <header>: <token>
- basic: Authorization: Basic base64(user:pass)
- cookies_file: загрузить cookies из JSON файла (Chrome-export или {"cookies":[...]})
- headers: прямой словарь заголовков {"headers": {...}} (можно комбинировать с bearer/...)
- api_key_query: добавить query параметр ?<param>=<token>

ВАЖНО:
- Никаких попыток "обхода капчи" и т.п. Здесь только легитимная авторизация:
  токены/ключи/куки, предоставленные клиентом или полученные в рамках разрешённого логина.
"""

import base64
import json
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable, Optional
from urllib.parse import urlparse

import requests


def _domain_of(url: str) -> str:
    try:
        return (urlparse(url).netloc or "").lower()
    except Exception:
        return ""


def _load_json(path: str) -> Any:
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


@dataclass(frozen=True)
class AuthSelection:
    """Результат выбора секрета для URL."""
    ref: str
    secret: dict[str, Any]


class SecretStore:
    """
    SecretStore читает secrets.json и даёт доступ по ref.
    """

    ENV_KEY = "PARSER_SECRETS_PATH"
    _cached: Optional["SecretStore"] = None

    def __init__(self, secrets_path: str) -> None:
        self.secrets_path = str(secrets_path)
        self.base_dir = str(Path(self.secrets_path).resolve().parent)
        raw = _load_json(self.secrets_path)
        if not isinstance(raw, dict):
            raise ValueError("secrets.json must be an object: {ref: {...}}")
        self._secrets: dict[str, dict[str, Any]] = {}
        for k, v in raw.items():
            if isinstance(k, str) and isinstance(v, dict):
                self._secrets[k] = v
        if not self._secrets:
            raise ValueError("secrets.json has no valid entries")

        # кэш загруженных cookies по ref (чтобы не читать файл 100 раз)
        self._cookies_loaded: set[str] = set()

    @classmethod
    def from_env(cls) -> Optional["SecretStore"]:
        """Singleton из ENV. Если ENV не задан — вернёт None."""
        if cls._cached is not None:
            return cls._cached
        p = os.getenv(cls.ENV_KEY, "").strip()
        if not p:
            return None
        # относительный путь — относительно CWD
        path = str(Path(p).expanduser().resolve()) if not Path(p).is_absolute() else str(Path(p).expanduser())
        cls._cached = cls(path)
        return cls._cached

    def get(self, ref: str) -> dict[str, Any]:
        if ref not in self._secrets:
            raise KeyError(f"Secret ref not found: {ref}")
        return self._secrets[ref]

    def resolve_ref(self, auth_cfg: dict[str, Any], url: str) -> Optional[str]:
        """
        auth_cfg (из profile._meta.auth):
        - {"ref":"client_api"}
        - {"by_domain":{"api.site.com":"ref1","site.com":"ref2"}}
        """
        if not isinstance(auth_cfg, dict):
            return None
        if isinstance(auth_cfg.get("ref"), str):
            return str(auth_cfg["ref"])
        by_domain = auth_cfg.get("by_domain")
        if isinstance(by_domain, dict):
            dom = _domain_of(url)
            if dom in by_domain and isinstance(by_domain[dom], str):
                return str(by_domain[dom])
            # иногда указывают без поддомена: olx.ua вместо www.olx.ua
            for k, v in by_domain.items():
                if not isinstance(k, str) or not isinstance(v, str):
                    continue
                if dom == k or dom.endswith("." + k):
                    return v
        return None

    def _resolve_path(self, p: str) -> str:
        pp = Path(p).expanduser()
        if pp.is_absolute():
            return str(pp)
        return str((Path(self.base_dir) / pp).resolve())

    def _cookies_from_json(self, raw: Any) -> list[dict[str, Any]]:
        # Chrome export: list[dict] with name/value/domain/path
        if isinstance(raw, list):
            return [x for x in raw if isinstance(x, dict) and "name" in x and "value" in x]
        if isinstance(raw, dict):
            if isinstance(raw.get("cookies"), list):
                return [x for x in raw["cookies"] if isinstance(x, dict) and "name" in x and "value" in x]
        return []

    def _load_cookies_into_session(self, session: requests.Session, ref: str, secret: dict[str, Any]) -> None:
        if ref in self._cookies_loaded:
            return
        p = secret.get("path") or secret.get("cookies_path") or secret.get("cookies_file")
        if not isinstance(p, str) or not p.strip():
            raise ValueError(f"cookies_file secret '{ref}' has no path")
        cookie_path = self._resolve_path(p.strip())
        raw = _load_json(cookie_path)
        cookies = self._cookies_from_json(raw)
        if not cookies:
            raise ValueError(f"cookies file '{cookie_path}' has no cookies list")
        for c in cookies:
            name = str(c.get("name"))
            value = str(c.get("value"))
            domain = c.get("domain")
            path = c.get("path")
            kwargs: dict[str, Any] = {}
            if isinstance(domain, str) and domain:
                kwargs["domain"] = domain.lstrip(".")
            if isinstance(path, str) and path:
                kwargs["path"] = path
            session.cookies.set(name, value, **kwargs)
        self._cookies_loaded.add(ref)

    def _headers_from_secret(self, secret: dict[str, Any]) -> dict[str, str]:
        out: dict[str, str] = {}
        typ = str(secret.get("type") or "").strip().lower()

        # произвольные headers
        if isinstance(secret.get("headers"), dict):
            for k, v in secret["headers"].items():
                if isinstance(k, str) and isinstance(v, (str, int, float)):
                    out[k] = str(v)

        if typ == "bearer":
            token = secret.get("token")
            if not isinstance(token, str) or not token:
                raise ValueError("bearer secret requires 'token'")
            out["Authorization"] = f"Bearer {token}"
        elif typ == "api_key_header":
            token = secret.get("token")
            header = secret.get("header")
            if not isinstance(token, str) or not token:
                raise ValueError("api_key_header secret requires 'token'")
            if not isinstance(header, str) or not header:
                raise ValueError("api_key_header secret requires 'header'")
            out[header] = token
        elif typ == "basic":
            u = secret.get("username")
            p = secret.get("password")
            if not isinstance(u, str) or not isinstance(p, str):
                raise ValueError("basic secret requires username/password")
            b = base64.b64encode(f"{u}:{p}".encode("utf-8")).decode("ascii")
            out["Authorization"] = f"Basic {b}"
        elif typ in ("cookies_file", "headers", "api_key_query"):
            # cookies_file не добавляет headers (только cookies в session)
            pass
        else:
            if typ and typ not in ("cookies_file",):
                # неизвестный тип — явно сообщаем
                raise ValueError(f"Unsupported secret type: {typ}")

        return out

    def _apply_query_param(self, params: dict[str, Any], secret: dict[str, Any]) -> None:
        typ = str(secret.get("type") or "").strip().lower()
        if typ != "api_key_query":
            return
        token = secret.get("token")
        param = secret.get("param")
        if not isinstance(token, str) or not token:
            raise ValueError("api_key_query secret requires 'token'")
        if not isinstance(param, str) or not param:
            raise ValueError("api_key_query secret requires 'param'")
        # не затираем, если уже задано явно
        params.setdefault(param, token)

    def select_for_url(self, auth_cfg: dict[str, Any], url: str) -> Optional[AuthSelection]:
        ref = self.resolve_ref(auth_cfg, url)
        if not ref:
            return None
        sec = self.get(ref)
        return AuthSelection(ref=ref, secret=sec)

    def make_auth_hook(self, auth_cfg: dict[str, Any]) -> Callable[[requests.Session, str, dict[str, Any], dict[str, str]], None]:
        """
        Возвращает hook(session, url, params, headers), который:
        - выбирает секрет по url (ref/by_domain),
        - подгружает cookies (если cookies_file),
        - добавляет заголовки (bearer/api_key/basic/headers),
        - добавляет query param (api_key_query).
        """
        def _hook(session: requests.Session, url: str, params: dict[str, Any], headers: dict[str, str]) -> None:
            sel = self.select_for_url(auth_cfg, url)
            if sel is None:
                return
            ref, sec = sel.ref, sel.secret
            typ = str(sec.get("type") or "").strip().lower()

            if typ == "cookies_file":
                self._load_cookies_into_session(session, ref, sec)

            # query param
            self._apply_query_param(params, sec)

            # headers (может перезаписать Authorization, если нужно)
            extra = self._headers_from_secret(sec)
            if extra:
                headers.update(extra)

        return _hook
