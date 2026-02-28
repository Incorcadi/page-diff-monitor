
#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
gen_playwright_patches.py

Генератор патчей (*.patch.json) для включения Playwright fallback
ТОЛЬКО на выбранных доменах (by_domain).

Зачем:
- Playwright "тяжёлый" и медленный, не нужно включать его глобально.
- Удобно держать базовые профили чистыми, а включение браузера делать патчем.
- Легко включать/выключать на конкретных доменах одной строкой в CLI.

Примеры:

1) Создать ОДИН multi-domain патч:
python gen_playwright_patches.py --mode prime_cookies --multi \
  --out profiles/patches/sites/playwright_prime_multi.patch.json \
  --domains example.com api.example.com

2) Создать пачку патчей "по одному на домен":
python gen_playwright_patches.py --mode render_html --per-domain --name-prefix pw_html \
  --out-dir profiles/patches/sites --domains example.com app.example.com

3) Точное совпадение домена (без . для поддоменов):
python gen_playwright_patches.py --mode prime_cookies --per-domain --exact \
  --out-dir profiles/patches/sites --domains api.example.com

По умолчанию:
- если домен выглядит как "example.com" (без поддомена), будет сохранён как ".example.com"
  (значит применится и к api.example.com, www.example.com и т.д.)
"""

from __future__ import annotations
import argparse
import json
import os
import re
from typing import Any, Dict, List


def make_fallback(mode: str) -> Dict[str, Any]:
    if mode == "prime_cookies":
        return {
            "enabled": True,
            "strategy": "mixed",
            "on_status": [403, 406],
            "on_hint": ["js_challenge", "cloudflare", "access_denied"],
            "max_tries": 1,
            "playwright": {
                "enabled": True,
                "mode": "prime_cookies",
                "headless": True,
                "browser": "chromium",
                "timeout_ms": 45000,
                "wait_until": "networkidle",
                "wait_selector": "",
                "actions": [{"type": "scroll", "times": 2, "delay_ms": 500}],
            },
        }
    if mode == "render_html":
        return {
            "enabled": True,
            "strategy": "playwright_html",
            "on_status": [403, 406],
            "on_hint": ["js_challenge", "cloudflare", "access_denied"],
            "max_tries": 0,
            "playwright": {
                "enabled": True,
                "mode": "render_html",
                "headless": True,
                "browser": "chromium",
                "timeout_ms": 60000,
                "wait_until": "networkidle",
                "wait_selector": "",
                "actions": [{"type": "scroll", "times": 4, "delay_ms": 500}],
            },
        }
    raise ValueError("mode must be prime_cookies or render_html")


def normalize_domain(d: str, *, exact: bool) -> str:
    d = (d or "").strip()
    if not d:
        raise ValueError("empty domain")
    # strip scheme/path if user pasted full URL
    d = re.sub(r"^https?://", "", d, flags=re.I).split("/")[0]
    if exact:
        return d
    # If looks like root domain (one dot), use ".domain" to include subdomains
    # e.g. example.com -> .example.com
    # If user passed api.example.com keep as is (exact host), unless they already started with "."
    if d.startswith("."):
        return d
    parts = d.split(".")
    if len(parts) == 2:
        return "." + d
    return d


def make_patch(name: str, domains: List[str], mode: str) -> Dict[str, Any]:
    fb = make_fallback(mode)
    by_domain = {dom: {"browser_fallback": fb} for dom in domains}
    return {
        "name": name,
        "enabled": True,
        "merge": {
            "_meta": {
                "http": {
                    "headers": {
                        "by_domain": by_domain
                    }
                }
            }
        },
        "set": {},
        "delete": []
    }


def safe_filename(s: str) -> str:
    s = s.strip().lower()
    s = s.replace(".", "_").replace("-", "_")
    s = re.sub(r"[^a-z0-9_]+", "", s)
    return s or "domain"


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--mode", choices=["prime_cookies", "render_html"], required=True)
    ap.add_argument("--domains", nargs="+", required=True, help="Domains or URLs")
    ap.add_argument("--exact", action="store_true", help="Do not prefix root domains with '.' (subdomain wildcard)")

    grp = ap.add_mutually_exclusive_group(required=True)
    grp.add_argument("--multi", action="store_true", help="Write ONE patch containing all domains")
    grp.add_argument("--per-domain", action="store_true", help="Write one patch per domain")

    ap.add_argument("--name", default=None, help="Patch name for --multi")
    ap.add_argument("--name-prefix", default=None, help="Prefix for per-domain patch names")
    ap.add_argument("--out", default=None, help="Output file for --multi")
    ap.add_argument("--out-dir", default=None, help="Output directory for --per-domain")

    args = ap.parse_args()

    doms = [normalize_domain(d, exact=args.exact) for d in args.domains]

    if args.multi:
        name = args.name or f"playwright_{args.mode}_multi"
        out = args.out or f"{name}.patch.json"
        patch = make_patch(name, doms, args.mode)
        os.makedirs(os.path.dirname(out) or ".", exist_ok=True)
        with open(out, "w", encoding="utf-8") as f:
            json.dump(patch, f, ensure_ascii=False, indent=2)
        print(out)
        return 0

    # per-domain
    out_dir = args.out_dir or "."
    os.makedirs(out_dir, exist_ok=True)
    prefix = args.name_prefix or f"playwright_{args.mode}"
    for d in doms:
        nm = f"{prefix}_{safe_filename(d.lstrip('.'))}"
        patch = make_patch(nm, [d], args.mode)
        out_path = os.path.join(out_dir, f"{nm}.patch.json")
        with open(out_path, "w", encoding="utf-8") as f:
            json.dump(patch, f, ensure_ascii=False, indent=2)
        print(out_path)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
