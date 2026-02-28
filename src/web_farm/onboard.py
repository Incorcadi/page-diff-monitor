"""
onboard.py — “доводка профиля”: авто-поиск limit_param (и проверка).

Почему limit_param важен
------------------------
В реальном парсинге limit — это размер партии.
Если limit_param не найден:
- ты либо получаешь слишком мало за запрос (медленно),
- либо сервер игнорирует твой параметр, и ты думаешь, что ускорился, а по факту нет.

Это модуль “наладчика линии”:
- делает 2–4 пробных запроса,
- измеряет размер партии,
- предлагает (или ставит) limit_param.

Важно:
- Он не угадывает бесконечно. Если не нашёл — честно пишет “unknown”.
"""

from __future__ import annotations

from dataclasses import dataclass, asdict
from typing import Any, Optional

from .site_profile import SiteProfile
from .http_engine import HttpEngine
from .extractors import extract_items

@dataclass
@dataclass
class LimitProbe:
    param: str
    small: int
    big: int
    n_small: int
    n_big: int
    ok: bool
    note: str = ""


def find_limit_param(
    profile: SiteProfile,
    *,
    engine: Optional[HttpEngine] = None,
    small: int = 5,
    big: int = 50,
    candidates: Optional[list[str]] = None,
    max_probes: int = 6,
) -> tuple[Optional[str], dict[str, Any]]:
    """
    Подобрать limit_param (как называется лимит у сервера).

    Возвращает:
      (limit_param_or_None, report_dict)
    """
    engine = engine or HttpEngine()

    if profile.pagination.limit_param:
        return profile.pagination.limit_param, {"status": "already_set", "limit_param": profile.pagination.limit_param}

    cands = candidates or [
        "limit", "per_page", "page_size", "pageSize", "_limit",
        "count", "size", "take", "rows"
    ]
    # убрать None, дубли
    uniq: list[str] = []
    for c in cands:
        if c and c not in uniq:
            uniq.append(c)

    base_params = dict(profile.base_params)
    report: dict[str, Any] = {"probes": [], "picked": None, "notes": []}

    probes = 0
    best: Optional[LimitProbe] = None

    for param in uniq:
        if probes >= max_probes:
            break
        probes += 1

        p_small = dict(base_params)
        p_small[param] = small
        resp_s, data_s, err_s = engine.safe_get_json(
            profile.url, method=profile.method, params=p_small, headers=profile.headers, timeout=profile.timeout
        )
        if data_s is None:
            pr = LimitProbe(param, small, big, 0, 0, False, note=f"small_err:{err_s}")
            report["probes"].append(asdict(pr))
            continue
        items_s = extract_items(data_s, profile.extract)
        n_small = len(items_s)

        p_big = dict(base_params)
        p_big[param] = big
        resp_b, data_b, err_b = engine.safe_get_json(
            profile.url, method=profile.method, params=p_big, headers=profile.headers, timeout=profile.timeout
        )
        if data_b is None:
            pr = LimitProbe(param, small, big, n_small, 0, False, note=f"big_err:{err_b}")
            report["probes"].append(asdict(pr))
            continue
        items_b = extract_items(data_b, profile.extract)
        n_big = len(items_b)

        ok = (n_big > n_small) and (n_small > 0)
        note = ""
        if n_small == 0:
            note = "no_items_on_small; extractor likely misconfigured"
        elif n_big == n_small:
            note = "no_growth; server ignores param or has hard max"
        else:
            note = "growth_detected"

        pr = LimitProbe(param, small, big, n_small, n_big, ok, note=note)
        report["probes"].append(asdict(pr))

        if ok:
            if best is None or (n_big - n_small) > (best.n_big - best.n_small):
                best = pr

    if best:
        report["picked"] = {"limit_param": best.param, "n_small": best.n_small, "n_big": best.n_big}
        return best.param, report

    report["notes"].append("limit_param_not_detected; set manually if API supports it")
    return None, report


def apply_limit_param(profile: SiteProfile, limit_param: str) -> SiteProfile:
    """Применить найденный limit_param в профиль."""
    profile.pagination.limit_param = limit_param
    return profile
