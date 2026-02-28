from __future__ import annotations

"""
runtime.py — единый пагинатор “в бою” по готовому профилю.

Принцип:
- Никакой магии: runtime не угадывает, он выполняет техкарту (SiteProfile).
- Все догадки/поиск параметров делаются в infer.py/onboard.py.

Что делает runtime:
- запрашивает страницы партиями согласно profile.pagination.kind
- извлекает items через extractors.extract_items
- отдаёт items как поток (generator), чтобы можно было писать в JSONL и не держать всё в памяти
"""

from typing import Any, Iterator, Optional, Callable
from urllib.parse import urljoin

from .site_profile import SiteProfile
from .http_engine import HttpEngine, make_http_engine_from_meta
from .http_utils import parse_link_next, extract_next_url_from_json, extract_cursor_token
from .resp_read import safe_read_json, read_text_safely
from .block_detect import classify_block
from .extractors import extract_items_any

def _merge_params(base: dict[str, Any], extra: dict[str, Any]) -> dict[str, Any]:
    out = dict(base or {})
    out.update({k: v for k, v in (extra or {}).items() if v is not None})
    return out


def _absolutize_next(base_url: str, next_url: str) -> str:
    # иногда next приходит относительным
    if next_url.startswith("http://") or next_url.startswith("https://"):
        return next_url
    return urljoin(base_url, next_url)


def paginate_items(
    profile: SiteProfile,
    *,
    engine: Optional[HttpEngine] = None,
    state: Optional[dict[str, Any]] = None,
    on_checkpoint: Optional[Callable[[dict[str, Any]], None]] = None,
    on_block: Optional[Callable[[dict[str, Any]], None]] = None,
) -> Iterator[dict[str, Any]]:
    if engine is not None:
        eng = engine
    else:
        http_meta: dict[str, Any] = {}
        try:
            http_meta = (getattr(profile, 'meta', None) or {}).get('http') or {}
        except Exception:
            http_meta = {}
        if make_http_engine_from_meta and isinstance(http_meta, dict) and http_meta:
            eng = make_http_engine_from_meta(http_meta, default_timeout=profile.timeout, default_headers=profile.headers)
        else:
            eng = HttpEngine(default_timeout=profile.timeout, default_headers=profile.headers)

    # state — это сохранённая точка, чтобы можно было "resume".
    st = dict(state or {})
    url = str(st.get("url") or profile.url)
    kind = profile.pagination.kind

    limit = profile.pagination.limit
    limit_param = profile.pagination.limit_param

    page = int(st.get("page") if st.get("page") is not None else profile.pagination.start_from)
    offset = int(st.get("offset") if st.get("offset") is not None else 0)

    cursor: Optional[str] = st.get("cursor") if isinstance(st.get("cursor"), str) else None
    next_url: Optional[str] = st.get("next_url") if isinstance(st.get("next_url"), str) else None

    extract_mode = str(getattr(profile.extract, "mode", "json") or "json").lower()
    if extract_mode not in ("json", "html", "auto"):
        extract_mode = "json"

    for batch_idx in range(profile.pagination.max_batches):
        params = dict(profile.base_params)

        if kind == "page":
            params[profile.pagination.page_param] = page
            if limit_param:
                params[limit_param] = limit

        elif kind == "offset":
            params[profile.pagination.offset_param] = offset
            if limit_param:
                params[limit_param] = limit

        elif kind == "cursor_token":
            if cursor is not None:
                params[profile.pagination.cursor_param or "cursor"] = cursor
            if limit_param:
                params[limit_param] = limit

        elif kind == "next_url":
            # url уже будет next_url, params не нужны (но base_params могут быть)
            if next_url is not None:
                url = next_url

        else:
            # unknown — просто один запрос, чтобы не молоть бесконечно
            pass

        expect = extract_mode if extract_mode in ("json", "html") else "auto"
        resp, err, _elapsed_ms = eng.request(
            url,
            method=profile.method,
            params=params,
            headers=profile.headers,
            timeout=profile.timeout,
            expect=expect,
        )
        if resp is None:
            return

        # Anti-bot / block detection: record event and stop (human-in-the-loop).
        if err is not None and not (200 <= int(resp.status_code) < 400):
            info = classify_block(resp)
            if info is not None and on_block is not None:
                try:
                    on_block({
                        "kind": kind,
                        "batch_idx": batch_idx,
                        "request_url": url,
                        "request_method": profile.method,
                        "request_params": params,
                        "expect": expect,
                        "status_code": int(resp.status_code),
                        "error": str(err),
                        "block_hint": info.get("hint"),
                        "resp_url_final": info.get("resp_url_final"),
                        "resp_headers": info.get("resp_headers"),
                        "resp_snippet": info.get("resp_snippet"),
                        "pagination_state": {
                            "url": url,
                            "page": page,
                            "offset": offset,
                            "cursor": cursor,
                            "next_url": next_url,
                            "batch_idx": batch_idx,
                            "kind": kind,
                        },
                    })
                except Exception:
                    pass
            return

        if not (200 <= int(resp.status_code) < 400):
            info = classify_block(resp)
            if info is not None and on_block is not None:
                try:
                    on_block({
                        "kind": kind,
                        "batch_idx": batch_idx,
                        "request_url": url,
                        "request_method": profile.method,
                        "request_params": params,
                        "expect": expect,
                        "status_code": int(resp.status_code),
                        "error": None,
                        "block_hint": info.get("hint"),
                        "resp_url_final": info.get("resp_url_final"),
                        "resp_headers": info.get("resp_headers"),
                        "resp_snippet": info.get("resp_snippet"),
                        "pagination_state": {
                            "url": url,
                            "page": page,
                            "offset": offset,
                            "cursor": cursor,
                            "next_url": next_url,
                            "batch_idx": batch_idx,
                            "kind": kind,
                        },
                    })
                except Exception:
                    pass
            return

        data_json: Optional[Any] = None
        items: list[Any] = []

        if extract_mode in ("json", "auto"):
            jr = safe_read_json(resp, force=(extract_mode == "json"), detect_soft=True)
            if jr.ok and jr.data is not None:
                data_json = jr.data
                items = extract_items_any(data_json, profile.extract, payload_kind="json")
            elif extract_mode == "json":
                return

        if not items and extract_mode in ("html", "auto"):
            tp = read_text_safely(resp)
            if tp is None:
                return
            items = extract_items_any(tp.text, profile.extract, payload_kind="html")

        if not items:
            return

        for it in items:
            if isinstance(it, dict):
                yield it
            else:
                # иногда items — это строки/числа; оборачиваем чтобы downstream не падал
                yield {"value": it}

        # Обновляем состояние пагинации
        if kind == "page":
            page += 1

        elif kind == "offset":
            step = profile.pagination.step or (limit if limit_param else len(items))
            offset += step

        elif kind == "cursor_token":
            if data_json is None:
                return
            new_cursor = extract_cursor_token(data_json)
            if not new_cursor or new_cursor == cursor:
                return
            cursor = new_cursor

        elif kind == "next_url":
            # 1) из Link header
            if resp is not None:
                nxt = parse_link_next(dict(resp.headers))
            else:
                nxt = None
            # 2) из JSON
            if not nxt and data_json is not None:
                nxt = extract_next_url_from_json(data_json)
            if not nxt:
                return
            next_url = _absolutize_next(profile.url, nxt)
            url = next_url

        else:
            # unknown: делаем только один шаг
            return

        # checkpoint: сохраняем состояние ПОСЛЕ того, как перешли на следующую страницу/курсор.
        if on_checkpoint is not None:
            try:
                on_checkpoint({
                    "kind": kind,
                    "url": url,
                    "page": page,
                    "offset": offset,
                    "cursor": cursor,
                    "next_url": next_url,
                    "batch_idx": batch_idx,
                })
            except Exception:
                # checkpoint не должен ломать прогон
                pass

        # Опциональное правило остановки: если limit применим и вернулось меньше limit — чаще всего конец.
        if limit_param and isinstance(limit, int) and limit > 0 and len(items) < limit:
            return
