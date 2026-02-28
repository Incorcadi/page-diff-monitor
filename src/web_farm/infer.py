"""
infer.py — “онбординг стратегии пагинации”: определить kind и параметры, не изобретая велосипед.

Что делает этот модуль
----------------------
Когда у тебя “сырой профиль”, чаще всего неизвестно:
- какая пагинация (page/offset/cursor/next_url)
- как называется параметр (page/offset/cursor)

infer.py делает несколько осторожных пробных запросов и строит “кандидаты”:
- StrategyCandidate = вариант (kind + param_name) + оценка

Важно:
- Это не “магия”. Это инженерный перебор гипотез с измерением результата.
- На реальных API бывают случаи, когда автоматом НЕ определить (тогда нужен ручной допил профиля).

Как измеряем успех
------------------
Если есть ID:
- good: много новых ID (fresh) и мало пересечений (overlap)

Если ID нет:
- мы НЕ делаем вид, что всё ок. Возвращаем unknown и просим настроить extract.id_path/id_keys.
"""

from __future__ import annotations

from dataclasses import dataclass, asdict, field
from typing import Any, Optional, cast

from .site_profile import SiteProfile, PaginationSpec, PaginationKind
from .http_engine import HttpEngine
from .http_utils import (
    parse_link_next,
    extract_next_url_from_json,
    extract_cursor_token,
    has_page_meta,
    has_offset_meta,
)
from .extractors import extract_items, ids_of

_ALLOWED_KINDS: set[str] = {"page", "offset", "cursor_token", "next_url", "unknown"}

def as_kind(s: str) -> PaginationKind:
    if s in _ALLOWED_KINDS:
        return cast(PaginationKind, s)
    return "unknown"

@dataclass
class CandidateScore:
    fresh: int
    overlap: int
    note: str = ""

    def key(self) -> tuple[int, int]:
        # больше fresh лучше, меньше overlap лучше
        return (self.fresh, -self.overlap)


@dataclass
class StrategyCandidate:
    kind: PaginationKind
    param_name: Optional[str] = None
    score: CandidateScore = field(default_factory=lambda: CandidateScore(0, 0))
    detail: dict[str, Any] = field(default_factory=dict)


def _score_ids(base_ids: set[str], next_ids: set[str]) -> CandidateScore:
    overlap = len(base_ids & next_ids)
    fresh = len(next_ids - base_ids)
    return CandidateScore(fresh=fresh, overlap=overlap)


def infer_pagination(
    profile: SiteProfile,
    *,
    engine: Optional[HttpEngine] = None,
    max_probes: int = 6,
) -> tuple[PaginationSpec, dict[str, Any]]:
    """
    Пытается определить profile.pagination.kind (+ нужные имена параметров).

    Возвращает:
      (pagination_spec, report_dict)

    Если уверенно не получилось — kind остаётся "unknown", а report объясняет почему.
    """
    engine = engine or HttpEngine()

    rep: dict[str, Any] = {
        "base": {},
        "signals": {},
        "candidates": [],
        "picked": None,
        "notes": [],
    }

    limit = int(profile.pagination.limit)
    limit_param = profile.pagination.limit_param

    base_params = dict(profile.base_params)
    if limit_param:
        base_params[limit_param] = limit

    resp0, data0, err0 = engine.safe_get_json(
        profile.url, method=profile.method, params=base_params, headers=profile.headers, timeout=profile.timeout
    )
    rep["base"] = {"err": err0, "status": getattr(resp0, "status_code", None)}
    if data0 is None:
        rep["notes"].append("base_request_failed")
        return profile.pagination, rep

    items0 = extract_items(data0, profile.extract)
    ids0 = ids_of(items0, profile.extract)
    rep["base"].update({"items": len(items0), "ids": len(ids0)})

    if not items0:
        rep["notes"].append("no_items_extracted; fix extract.items_path/items_keys")
        return profile.pagination, rep

    if not ids0:
        rep["notes"].append("no_ids_extracted; fix extract.id_path/id_keys, otherwise pagination inference is weak")
        # мы специально НЕ продолжаем “угадывать” без id, чтобы не подставить тебя
        return profile.pagination, rep

    # 1) next_url по Link header
    headers0: dict[str, str] = dict(resp0.headers) if resp0 is not None else {}
    next_link = parse_link_next(headers0)
    if next_link:
        pag = profile.pagination
        pag.kind = "next_url"
        pag.next_url_field_hint = "Link"
        rep["signals"]["next_url"] = {"where": "Link", "url": next_link}
        rep["picked"] = {"kind": "next_url", "hint": "Link"}
        return pag, rep

    # 2) next_url по JSON
    jnext = extract_next_url_from_json(data0)
    if jnext:
        pag = profile.pagination
        pag.kind = "next_url"
        pag.next_url_field_hint = None
        rep["signals"]["next_url"] = {"where": "json", "url": jnext}
        rep["picked"] = {"kind": "next_url", "hint": "json"}
        return pag, rep

    # 3) cursor_token гипотеза
    cursor = extract_cursor_token(data0)
    if cursor:
        rep["signals"]["cursor_token_found"] = True
        token = cursor

        cursor_param_candidates = [
            profile.pagination.cursor_param,
            "cursor", "after", "pageToken", "page_token", "nextToken", "continuation"
        ]
        cursor_param_candidates = [c for c in cursor_param_candidates if c]

        probes = 0
        best: Optional[StrategyCandidate] = None

        for cp in cursor_param_candidates:
            if probes >= max_probes:
                break
            probes += 1

            p = dict(base_params)
            p[cp] = token
            resp1, data1, err1 = engine.safe_get_json(
                profile.url, method=profile.method, params=p, headers=profile.headers, timeout=profile.timeout
            )
            if data1 is None:
                cand = StrategyCandidate(kind="cursor_token", param_name=cp, score=CandidateScore(0, 0, note=f"err:{err1}"), detail={"err": err1})
                rep["candidates"].append(asdict(cand))
                continue

            items1 = extract_items(data1, profile.extract)
            ids1 = ids_of(items1, profile.extract)
            sc = _score_ids(ids0, ids1)
            cand = StrategyCandidate(kind="cursor_token", param_name=cp, score=sc, detail={"items": len(items1)})
            rep["candidates"].append(asdict(cand))

            if best is None or sc.key() > best.score.key():
                best = cand

        if best and best.score.fresh > 0:
            pag = profile.pagination
            pag.kind = "cursor_token"
            pag.cursor_param = best.param_name
            pag.cursor_field_hint = None
            rep["picked"] = {"kind": "cursor_token", "cursor_param": best.param_name}
            return pag, rep

    # 4) page / offset гипотезы (по ID)
    rep["signals"]["page_meta"] = bool(has_page_meta(data0))
    rep["signals"]["offset_meta"] = bool(has_offset_meta(data0))

    candidates: list[StrategyCandidate] = []

    # page
    page_params = [profile.pagination.page_param, "page", "p", "pageNumber", "page_number"]
    page_params = [x for x in dict.fromkeys(page_params) if x]  # uniq preserve order

    # offset
    offset_params = [profile.pagination.offset_param, "offset", "start", "_start", "skip"]
    offset_params = [x for x in dict.fromkeys(offset_params) if x]

    probes = 0

    def try_probe(kind: str, param_name: str, value: Any) -> None:
        nonlocal probes
        if probes >= max_probes:
            return
        probes += 1

        p = dict(base_params)
        p[param_name] = value
        resp1, data1, err1 = engine.safe_get_json(
            profile.url, method=profile.method, params=p, headers=profile.headers, timeout=profile.timeout
        )
        if data1 is None:
            sc = CandidateScore(0, 0, note=f"err:{err1}")
            candidates.append(StrategyCandidate(kind=as_kind(kind), param_name=param_name, score=sc, detail={"err": err1}))
            return

        items1 = extract_items(data1, profile.extract)
        ids1 = ids_of(items1, profile.extract)
        sc = _score_ids(ids0, ids1)
        candidates.append(StrategyCandidate(kind=as_kind(kind), param_name=param_name, score=sc, detail={"items": len(items1)}))

    # page probes: page=2 (если start_from=1)
    for pp in page_params:
        try_probe("page", pp, profile.pagination.start_from + 1)

    # offset probes: offset=limit (вторая партия)
    for op in offset_params:
        try_probe("offset", op, limit)

    for c in candidates:
        rep["candidates"].append(asdict(c))

    if candidates:
        best = max(candidates, key=lambda c: c.score.key())
        if best.score.fresh > 0:
            pag = profile.pagination
            pag.kind = best.kind  # page/offset
            if best.kind == "page":
                pag.page_param = str(best.param_name)
            else:
                pag.offset_param = str(best.param_name)
                pag.step = pag.step or 0  # оставим 0 => runtime возьмёт limit
            rep["picked"] = {"kind": best.kind, "param": best.param_name, "fresh": best.score.fresh, "overlap": best.score.overlap}
            return pag, rep

    rep["notes"].append("no_strategy_confident; keep kind=unknown and set pagination manually")
    return profile.pagination, rep
