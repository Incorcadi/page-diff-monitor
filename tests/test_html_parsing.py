from __future__ import annotations

from pathlib import Path

import requests

from web_farm.extractors import extract_items_any
from web_farm.offline_tests import run_offline_tests
from web_farm.runtime import paginate_items
from web_farm.site_profile import ExtractSpec, SiteProfile


def _mk_resp(html: str) -> requests.Response:
    r = requests.Response()
    r.status_code = 200
    r._content = html.encode("utf-8")
    r.headers["Content-Type"] = "text/html; charset=utf-8"
    r.encoding = "utf-8"
    r.url = "https://example.com/list"
    return r


def test_extract_items_any_html_with_fields_and_id_attr():
    html = """
    <html><body>
      <article class="card" data-id="A1"><a href="/p/a1"><h2>Alpha</h2></a></article>
      <article class="card" data-id="B2"><a href="/p/b2"><h2>Beta</h2></a></article>
    </body></html>
    """
    spec = ExtractSpec(
        mode="html",
        html_items_selector="article.card",
        html_fields={"title": "h2::text", "url": "a::attr(href)"},
        html_id_attr="data-id",
    )

    items = extract_items_any(html, spec, payload_kind="html")
    assert len(items) == 2
    assert items[0]["id"] == "A1"
    assert items[0]["title"] == "Alpha"
    assert items[0]["url"] == "/p/a1"


def test_runtime_paginate_items_supports_html_mode():
    html = """
    <html><body>
      <article class="card" data-id="A1"><a href="/p/a1"><h2>Alpha</h2></a></article>
      <article class="card" data-id="B2"><a href="/p/b2"><h2>Beta</h2></a></article>
    </body></html>
    """

    profile = SiteProfile.from_dict(
        {
            "name": "html-site",
            "url": "https://example.com/list",
            "pagination": {"kind": "unknown"},
            "extract": {
                "mode": "html",
                "html_items_selector": "article.card",
                "html_fields": {"title": "h2::text", "url": "a::attr(href)"},
                "html_id_attr": "data-id",
            },
        }
    )

    class _Engine:
        def __init__(self) -> None:
            self._used = False

        def request(self, *args, **kwargs):  # type: ignore[no-untyped-def]
            if self._used:
                return None, "done", 0
            self._used = True
            return _mk_resp(html), None, 1

    out = list(paginate_items(profile, engine=_Engine()))
    assert len(out) == 2
    assert out[0]["id"] == "A1"
    assert out[1]["title"] == "Beta"


def test_offline_tests_support_html_cases(tmp_path: Path):
    html = """
    <html><body>
      <article class="card" data-id="A1"><a href="/p/a1"><h2>Alpha</h2></a></article>
      <article class="card" data-id="B2"><a href="/p/b2"><h2>Beta</h2></a></article>
    </body></html>
    """
    (tmp_path / "page_1.html").write_text(html, encoding="utf-8")

    profile = SiteProfile.from_dict(
        {
            "name": "html-offline",
            "url": "https://example.com/list",
            "pagination": {"kind": "unknown"},
            "extract": {
                "mode": "html",
                "html_items_selector": "article.card",
                "html_fields": {"title": "h2::text", "url": "a::attr(href)"},
                "html_id_attr": "data-id",
            },
            "_meta": {
                "tests": {
                    "fixtures_dir": str(tmp_path),
                    "cases": [
                        {
                            "name": "case_html",
                            "file": "page_1.html",
                            "kind": "html",
                            "assert": {"items_min": 2, "unique_ids_min": 2},
                        }
                    ],
                }
            },
        }
    )

    rep = run_offline_tests(profile, fixtures_dir=str(tmp_path))
    assert rep["ok"] is True
    assert rep["cases"][0]["items"] == 2
    assert rep["cases"][0]["unique_ids"] == 2
