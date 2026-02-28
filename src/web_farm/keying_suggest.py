from __future__ import annotations

"""keying_suggest.py — авто-подсказка keying/id_path по фикстуре.

Идея:
- берём JSON ответ (fixture) + профиль (extract spec)
- выдёргиваем items через extractors.extract_items
- анализируем листья item dict (dot-path) и ищем кандидатов:
  - лучший id_path (почти уникален + почти всегда заполнен)
  - набор paths для compound/hash (на случай если явного id нет)

Важно:
- это НЕ “магия”, а эвристика. Лучший кандидат печатается вместе с метриками.
- ничего не пишется в профиль без явного --write (в CLI).
"""

from dataclasses import dataclass
from typing import Any, Iterable, Optional
import itertools

NOISY_KEYWORDS = {
    "price", "cost", "amount", "sum", "total", "discount", "sale",
    "created", "updated", "modified", "timestamp", "time", "date",
    "views", "view", "seen", "count", "rating", "score", "rank",
    "position", "idx", "index", "offset", "page",
    "etag", "checksum", "hash",
}

ID_KEYWORDS = {"id", "uuid", "guid", "pk", "uid", "product_id", "item_id", "listing_id", "ad_id", "sku"}


def _is_primitive(v: Any) -> bool:
    return v is None or isinstance(v, (str, int, float, bool))


def _norm_val(v: Any) -> Optional[str]:
    if v is None:
        return None
    if isinstance(v, str):
        s = v.strip()
        return s if s != "" else None
    if isinstance(v, bool):
        return "true" if v else "false"
    if isinstance(v, (int, float)):
        # 0 — валидное значение; не выкидываем
        return str(v)
    return None


def iter_leaf_paths(obj: Any, *, max_depth: int = 4, _prefix: str = "", _depth: int = 0) -> Iterable[tuple[str, Any]]:
    """Итерирует leaf-значения dict как (dot_path, value).

    Списки пропускаем (слишком много шумных данных), кроме случая:
    - если список длины 1 и внутри dict — можно пройти внутрь (редко).
    """
    if _depth > max_depth:
        return
    if isinstance(obj, dict):
        for k, v in obj.items():
            if not isinstance(k, str):
                continue
            p = f"{_prefix}.{k}" if _prefix else k
            if isinstance(v, dict):
                yield from iter_leaf_paths(v, max_depth=max_depth, _prefix=p, _depth=_depth + 1)
            elif isinstance(v, list):
                # мягкий допуск: [ {..} ] -> пройти внутрь
                if len(v) == 1 and isinstance(v[0], dict):
                    yield from iter_leaf_paths(v[0], max_depth=max_depth, _prefix=f"{p}.0", _depth=_depth + 1)
                else:
                    continue
            else:
                yield (p, v)
    # если obj не dict — ничего


@dataclass
class PathStats:
    path: str
    presence: float
    unique_ratio: float
    nonempty: int
    unique: int
    noisy: bool
    id_like: bool
    score: float


def _score_path(path: str, presence: float, unique_ratio: float) -> float:
    seg = path.split(".")[-1].lower()
    noisy = any(kw in seg for kw in NOISY_KEYWORDS)
    id_like = (seg in ID_KEYWORDS) or seg.endswith("_id") or seg.endswith("id")
    score = 0.55 * presence + 0.45 * unique_ratio
    if id_like:
        score += 0.25
    if noisy:
        score -= 0.35
    # clamp
    if score < 0:
        score = 0.0
    if score > 1.0:
        score = 1.0
    return score


def analyze_items_for_keying(items: list[Any], *, max_paths: int = 500, max_depth: int = 4) -> dict[str, Any]:
    """Вернуть рекомендации по keying на основе списка items."""
    dict_items = [it for it in items if isinstance(it, dict)]
    n = len(dict_items)
    if n == 0:
        return {"error": "no_dict_items"}

    # собрать значения по путям
    values_by_path: dict[str, list[Optional[str]]] = {}
    for it in dict_items:
        seen_paths = set()
        for p, v in iter_leaf_paths(it, max_depth=max_depth):
            if p in seen_paths:
                continue
            seen_paths.add(p)
            nv = _norm_val(v)
            values_by_path.setdefault(p, []).append(nv)

        # заполнить отсутствующие пути None не будем — presence считаем по длине списка,
        # поэтому нужно выравнивание: проще накапливать по каждому item отдельно
        # => сделаем второй проход: теперь у нас список не выровнен.
    # Выравнивание: для каждого path построим список длины n (с None на пропусках)
    # Делаем более корректно: переобход items второй раз
    all_paths = list(values_by_path.keys())
    # ограничим число путей для скорости: сначала оставим те, что выглядят как id/url/slug и т.п.
    def _priority(p: str) -> int:
        seg = p.split(".")[-1].lower()
        if seg in ID_KEYWORDS or seg.endswith("_id") or seg.endswith("id"):
            return 3
        if any(x in seg for x in ("url", "link", "slug", "handle", "code")):
            return 2
        if any(kw in seg for kw in NOISY_KEYWORDS):
            return 0
        return 1

    all_paths.sort(key=_priority, reverse=True)
    if len(all_paths) > max_paths:
        all_paths = all_paths[:max_paths]

    aligned: dict[str, list[Optional[str]]] = {p: [None] * n for p in all_paths}
    for idx, it in enumerate(dict_items):
        item_map = {}
        for p, v in iter_leaf_paths(it, max_depth=max_depth):
            if p in aligned:
                item_map[p] = _norm_val(v)
        for p in aligned:
            if p in item_map:
                aligned[p][idx] = item_map[p]

    stats: list[PathStats] = []
    for p, vals in aligned.items():
        nonempty_vals = [v for v in vals if v is not None]
        nonempty = len(nonempty_vals)
        if nonempty == 0:
            continue
        unique = len(set(nonempty_vals))
        presence = nonempty / n
        unique_ratio = unique / nonempty if nonempty else 0.0
        seg = p.split(".")[-1].lower()
        noisy = any(kw in seg for kw in NOISY_KEYWORDS)
        id_like = (seg in ID_KEYWORDS) or seg.endswith("_id") or seg.endswith("id")
        score = _score_path(p, presence, unique_ratio)
        stats.append(PathStats(p, presence, unique_ratio, nonempty, unique, noisy, id_like, score))

    stats.sort(key=lambda s: (s.score, s.unique_ratio, s.presence), reverse=True)

    # Кандидаты id_path: очень высокий unique_ratio и хороший presence
    id_candidates = [s for s in stats if s.unique_ratio >= 0.95 and s.presence >= 0.7]
    # приоритет тем, что id_like
    id_candidates.sort(key=lambda s: (s.id_like, s.score, s.unique_ratio, s.presence), reverse=True)

    # кандидаты для compound/hash: исключим шумные, возьмём топ
    base_fields = [s for s in stats if not s.noisy and s.presence >= 0.7]
    base_fields.sort(key=lambda s: (s.id_like, s.unique_ratio, s.presence, s.score), reverse=True)
    base_fields = base_fields[:12]

    # перебор комбинаций 1..3
    combos: list[dict[str, Any]] = []
    for r in (1, 2, 3):
        for comb in itertools.combinations(base_fields, r):
            paths = [c.path for c in comb]
            tuples = []
            present = 0
            for i in range(n):
                row = []
                ok = True
                for p in paths:
                    v = aligned[p][i]
                    if v is None:
                        ok = False
                        break
                    row.append(v)
                if not ok:
                    continue
                present += 1
                tuples.append("\u241f".join(row))  # unit separator-like
            if present == 0:
                continue
            uniq = len(set(tuples))
            uniq_ratio = uniq / present
            presence = present / n
            score = 0.7 * uniq_ratio + 0.3 * presence
            combos.append({
                "paths": paths,
                "presence": round(presence, 4),
                "unique_ratio": round(uniq_ratio, 4),
                "score": round(score, 4),
            })

    combos.sort(key=lambda c: (c["score"], c["unique_ratio"], c["presence"], -len(c["paths"])), reverse=True)
    combos = combos[:10]

    def _pack_stats(s: PathStats) -> dict[str, Any]:
        return {
            "path": s.path,
            "presence": round(s.presence, 4),
            "unique_ratio": round(s.unique_ratio, 4),
            "nonempty": s.nonempty,
            "unique": s.unique,
            "id_like": bool(s.id_like),
            "noisy": bool(s.noisy),
            "score": round(s.score, 4),
        }

    out = {
        "items": n,
        "top_paths": [_pack_stats(x) for x in stats[:15]],
        "id_candidates": [_pack_stats(x) for x in id_candidates[:10]],
        "combo_candidates": combos,
        "recommend": {
            "id_path": id_candidates[0].path if id_candidates else None,
            "fallback_paths": combos[0]["paths"] if combos else None,
        },
    }
    return out
