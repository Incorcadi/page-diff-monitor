from __future__ import annotations

"""profile_explain.py — инструменты "explain" и "verify" для профилей.

Цель (практическая):
- быстро понять, почему профиль извлекает/не извлекает items
- увидеть, как именно export-схема формирует колонки (какой path сработал)
- сделать быстрый офлайн-контроль одной фикстуры (без сети)

Модуль НЕ делает HTTP. Он работает по сохранённым фикстурам JSON/HTML.
"""

import json
import hashlib
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Optional


# --- safe imports (пакет или рядом) ---

try:
    from web_farm.site_profile import SiteProfile
except Exception:  # pragma: no cover
    from site_profile import SiteProfile

try:
    from web_farm.extractors import extract_items, ids_of
except Exception:  # pragma: no cover
    from extractors import extract_items, ids_of

try:
    from web_farm.json_path import get_by_path
except Exception:  # pragma: no cover
    from json_path import get_by_path

try:
    import web_farm.export_csv as export_mod
except Exception:  # pragma: no cover
    import export_csv as export_mod

try:
    from web_farm.keying import extract_item_id, make_item_key
except Exception:  # pragma: no cover
    try:
        from keying import extract_item_id, make_item_key
    except Exception:  # pragma: no cover
        extract_item_id = None  # type: ignore
        make_item_key = None  # type: ignore


@dataclass
class _Issue:
    level: str  # error|warn
    path: str
    message: str


def _is_empty(v: Any) -> bool:
    return v is None or v == ""


def _read_json(path: str) -> Any:
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def _read_text(path: str) -> str:
    return Path(path).read_text(encoding="utf-8", errors="ignore")


def _resolve_fixture_from_case(profile: SiteProfile, case: str, fixtures_dir: Optional[str]) -> tuple[Optional[str], str, dict[str, Any]]:
    """Вернуть (fixture_path, kind, assert_cfg) по имени кейса из _meta.tests.cases."""
    meta = profile.meta if isinstance(profile.meta, dict) else {}
    tests = meta.get("tests") if isinstance(meta.get("tests"), dict) else {}

    fd = fixtures_dir or tests.get("fixtures_dir")
    if not isinstance(fd, str) or not fd.strip():
        fd = "tests/fixtures"

    cases = tests.get("cases")
    if not isinstance(cases, list):
        return None, "json", {}

    chosen: Optional[dict[str, Any]] = None
    for c in cases:
        if isinstance(c, dict) and str(c.get("name")) == case:
            chosen = c
            break

    if not chosen:
        return None, "json", {}

    file_rel = chosen.get("file")
    kind = str(chosen.get("kind") or "json").lower()
    assert_cfg = chosen.get("assert") if isinstance(chosen.get("assert"), dict) else {}

    if not isinstance(file_rel, str) or not file_rel.strip():
        return None, kind, assert_cfg

    fp = Path(fd) / file_rel
    return str(fp), kind, assert_cfg


def _resolve_export_schema(profile: SiteProfile, schema: Optional[str]) -> tuple[str, Any]:
    """Вернуть (schema_name, columns_spec). columns_spec может быть list или columns_map dict."""
    meta = profile.meta if isinstance(profile.meta, dict) else {}
    export_cfg = meta.get("export") if isinstance(meta.get("export"), dict) else {}

    default_schema = export_cfg.get("default_schema")
    sch = schema
    if not sch:
        sch = default_schema if isinstance(default_schema, str) and default_schema else "default"

    schemas = export_cfg.get("schemas")
    if not isinstance(schemas, dict):
        return sch, None

    s_cfg = schemas.get(sch)
    if not isinstance(s_cfg, dict):
        return sch, None

    if isinstance(s_cfg.get("columns_map"), dict):
        return sch, s_cfg.get("columns_map")
    if isinstance(s_cfg.get("columns"), list):
        return sch, s_cfg.get("columns")
    return sch, None


def _normalize_columns(columns_spec: Any) -> Optional[list[dict[str, Any]]]:
    norm = getattr(export_mod, "_normalize_columns", None)
    if callable(norm):
        return norm(columns_spec)

    if columns_spec is None:
        return None
    if isinstance(columns_spec, list):
        return columns_spec
    if isinstance(columns_spec, dict):
        out = []
        for name, spec in columns_spec.items():
            if isinstance(spec, dict) and (spec.get("enabled") is False or spec.get("_disabled") is True):
                continue
            col = dict(spec) if isinstance(spec, dict) else {"path": spec}
            col.setdefault("name", name)
            out.append(col)
        return out
    return None


def _cast(v: Any, typ: str) -> str:
    fn = getattr(export_mod, "_cast", None)
    if callable(fn):
        return fn(v, typ)
    return "" if v is None else str(v)


def _fallback_item_key(obj: Any) -> str:
    blob = json.dumps(obj, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return "sha1:" + hashlib.sha1(blob).hexdigest()


def _compute_id_and_key(item: Any, profile: SiteProfile) -> tuple[str, str]:
    if isinstance(item, dict) and extract_item_id is not None and make_item_key is not None:
        _id = extract_item_id(item, profile.extract)
        _key = make_item_key(item, profile.extract)
        return ("" if _id is None else str(_id), str(_key))

    # fallback (достаточно для explain)
    if isinstance(item, dict):
        v = item.get("id") or item.get("uuid") or item.get("guid") or item.get("slug")
        if v is not None and v != "":
            return (str(v), f"id:{v}")

    return ("", _fallback_item_key(item))


def _get_one(obj: Any, path: str) -> Any:
    if path == "":
        return obj
    # поддерживаем ускорение: без '.' — прямой ключ
    if "." not in path and isinstance(obj, dict):
        return obj.get(path)
    return get_by_path(obj, path)


def _explain_value(item: Any, col: dict[str, Any], *, ctx: dict[str, Any], profile: SiteProfile) -> dict[str, Any]:
    """Вернуть {value, rule, path_used, default_used} для колонки."""
    name = str(col.get("name") or "")
    typ = str(col.get("type") or "str")

    # compute
    if "compute" in col:
        kind = str(col.get("compute") or "")
        if kind in ("item_id", "item_key"):
            _id, _key = _compute_id_and_key(item, profile)
            v = _id if kind == "item_id" else _key
        else:
            v = ""
        default_used = False
        if _is_empty(v) and "default" in col:
            v = col.get("default")
            default_used = True
        return {"name": name, "value": _cast(v, typ), "rule": f"compute:{kind}", "path_used": None, "default_used": default_used}

    # const_ref
    if "const_ref" in col:
        key = str(col.get("const_ref") or "")
        v = ctx.get(key)
        default_used = False
        if _is_empty(v) and "default" in col:
            v = col.get("default")
            default_used = True
        return {"name": name, "value": _cast(v, typ), "rule": f"const_ref:{key}", "path_used": None, "default_used": default_used}

    # const
    if "const" in col:
        v = col.get("const")
        default_used = False
        if _is_empty(v) and "default" in col:
            v = col.get("default")
            default_used = True
        return {"name": name, "value": _cast(v, typ), "rule": "const", "path_used": None, "default_used": default_used}

    # paths/path
    used: Optional[str] = None
    v: Any = None

    paths = col.get("paths")
    if isinstance(paths, list) and paths:
        for p in [str(x) for x in paths if isinstance(x, (str, int, float))]:
            vv = _get_one(item, str(p))
            if not _is_empty(vv):
                v = vv
                used = str(p)
                break
        rule = "paths"
    else:
        p = str(col.get("path") or "")
        v = _get_one(item, p)
        used = p
        rule = "path"

    default_used = False
    if _is_empty(v) and "default" in col:
        v = col.get("default")
        default_used = True
        rule = rule + "+default"

    return {"name": name, "value": _cast(v, typ), "rule": rule, "path_used": used, "default_used": default_used}


def explain_profile(
    profile: SiteProfile,
    *,
    fixture_path: Optional[str] = None,
    case: Optional[str] = None,
    fixtures_dir: Optional[str] = None,
    schema: Optional[str] = None,
    ctx: Optional[dict[str, Any]] = None,
    max_items: int = 50,
    show_items: int = 3,
) -> dict[str, Any]:
    ctx = dict(ctx or {})

    assert_cfg: dict[str, Any] = {}
    kind = "json"

    if not fixture_path and case:
        fp, k, a = _resolve_fixture_from_case(profile, case, fixtures_dir)
        fixture_path = fp
        kind = k
        assert_cfg = a

    if not fixture_path:
        return {
            "ok": False,
            "error": "fixture is required: provide --fixture or --case (from _meta.tests.cases)",
        }

    p = Path(fixture_path)
    if not p.exists():
        return {"ok": False, "error": f"fixture not found: {fixture_path}"}

    # kind autodetect if unknown
    if kind not in ("json", "html"):
        low = p.name.lower()
        kind = "json" if low.endswith(".json") else "html"

    # load
    data: Any
    if kind == "json":
        try:
            data = _read_json(str(p))
        except Exception as e:
            return {"ok": False, "error": f"cannot read JSON fixture: {e}"}
    else:
        data = _read_text(str(p))

    rep: dict[str, Any] = {
        "ok": True,
        "profile": profile.name,
        "fixture": str(p),
        "kind": kind,
        "extract": {
            "items_path": profile.extract.items_path,
            "items_keys": list(profile.extract.items_keys),
            "container_keys": list(profile.extract.container_keys),
            "max_depth": profile.extract.max_depth,
            "id_path": profile.extract.id_path,
            "id_keys": list(profile.extract.id_keys),
        },
        "export": {},
        "items": {},
        "sample_items": [],
        "case_assert": assert_cfg or None,
    }

    if kind != "json":
        rep["ok"] = False
        rep["error"] = "HTML fixture is supported only minimally: extractor/export are JSON-first in this project"
        return rep

    # items
    try:
        items = extract_items(data, profile.extract)
    except Exception as e:
        rep["ok"] = False
        rep["error"] = f"extract_items failed: {e}"
        return rep

    rep["items"]["count"] = len(items)

    # ids
    try:
        ids = ids_of(items, profile.extract)
    except Exception:
        ids = []

    rep["items"]["ids_count"] = len(ids)
    rep["items"]["unique_ids_count"] = len(set(ids))
    rep["items"]["id_coverage"] = (len(ids) / len(items)) if items else 0.0

    # export schema
    schema_name, columns_spec = _resolve_export_schema(profile, schema)
    cols = _normalize_columns(columns_spec)

    rep["export"]["schema"] = schema_name
    rep["export"]["columns_count"] = 0 if not cols else len(cols)

    if not cols:
        rep["export"]["warning"] = "no export schema columns found in _meta.export.schemas"
        # still show sample id/key
        for it in items[: max(0, show_items)]:
            _id, _key = _compute_id_and_key(it, profile)
            rep["sample_items"].append({"item_id": _id, "item_key": _key})
        return rep

    scan = items[: max_items]

    col_reports: list[dict[str, Any]] = []
    for col in cols:
        name = str(col.get("name") or "")
        nonempty = 0
        used_paths: dict[str, int] = {}
        example = ""
        rule_example = ""

        for i, it in enumerate(scan):
            ex = _explain_value(it, col, ctx=ctx, profile=profile)
            val = ex.get("value")
            if isinstance(val, str) and val != "":
                nonempty += 1
            pu = ex.get("path_used")
            if isinstance(pu, str):
                used_paths[pu] = used_paths.get(pu, 0) + 1
            if i == 0:
                example = str(val)
                rule_example = str(ex.get("rule"))

        ratio = (nonempty / len(scan)) if scan else 0.0
        top_path = None
        if used_paths:
            top_path = sorted(used_paths.items(), key=lambda kv: (-kv[1], kv[0]))[0][0]

        col_reports.append({
            "name": name,
            "type": str(col.get("type") or "str"),
            "rule_example": rule_example,
            "top_path": top_path,
            "nonempty": nonempty,
            "checked": len(scan),
            "nonempty_ratio": ratio,
            "example": example[:240],
        })

    rep["export"]["columns"] = col_reports

    # sample items (first N)
    for it in items[: max(0, show_items)]:
        _id, _key = _compute_id_and_key(it, profile)
        row = {"item_id": _id, "item_key": _key, "cols": {}}
        # показываем первые 8 колонок, чтобы не раздувать
        for col in cols[:8]:
            ex = _explain_value(it, col, ctx=ctx, profile=profile)
            row["cols"][str(col.get("name") or "")] = ex.get("value")
        rep["sample_items"].append(row)

    # ok heuristic
    if len(items) == 0:
        rep["ok"] = False
        rep["error"] = "0 items extracted (check extract.items_path/items_keys)"

    return rep


def format_explain_text(rep: dict[str, Any]) -> str:
    if not rep:
        return "(empty)"
    if rep.get("ok") is False and rep.get("error"):
        # still print what we have
        head = f"EXPLAIN: FAIL — {rep.get('error')}"
    else:
        head = "EXPLAIN: OK"

    lines: list[str] = [head]
    lines.append(f"Profile: {rep.get('profile')}")
    lines.append(f"Fixture: {rep.get('fixture')} ({rep.get('kind')})")

    items = rep.get("items") or {}
    lines.append(f"Items: {items.get('count', 0)} | ids: {items.get('ids_count', 0)} | unique_ids: {items.get('unique_ids_count', 0)} | id_coverage: {items.get('id_coverage', 0):.2f}")

    ext = rep.get("extract") or {}
    lines.append("Extractor:")
    lines.append(f"  items_path: {ext.get('items_path')}")
    lines.append(f"  id_path:    {ext.get('id_path')}")

    exp = rep.get("export") or {}
    lines.append(f"Export schema: {exp.get('schema')} | columns: {exp.get('columns_count', 0)}")

    cols = exp.get("columns")
    if isinstance(cols, list) and cols:
        lines.append("Columns coverage (top 15):")
        for c in cols[:15]:
            lines.append(
                f"  - {c.get('name')}: nonempty_ratio={float(c.get('nonempty_ratio') or 0):.2f} top_path={c.get('top_path')} example_rule={c.get('rule_example')}"
            )

    samples = rep.get("sample_items")
    if isinstance(samples, list) and samples:
        lines.append("Sample items:")
        for i, s in enumerate(samples, 1):
            lines.append(f"  #{i}: item_id={s.get('item_id')} item_key={s.get('item_key')}")
            cols2 = s.get("cols")
            if isinstance(cols2, dict):
                for k, v in list(cols2.items())[:8]:
                    vv = str(v)
                    if len(vv) > 160:
                        vv = vv[:160] + "…"
                    lines.append(f"      {k}: {vv}")

    return "\n".join(lines)


def verify_profile(
    profile: SiteProfile,
    *,
    fixture_path: Optional[str] = None,
    case: Optional[str] = None,
    fixtures_dir: Optional[str] = None,
    schema: Optional[str] = None,
    items_min: Optional[int] = None,
    unique_ids_min: Optional[int] = None,
    columns_nonempty: Optional[str] = None,
    min_nonempty_ratio: Optional[float] = None,
    max_items: int = 200,
) -> dict[str, Any]:
    # if case provided, load its fixture and asserts
    assert_cfg: dict[str, Any] = {}
    kind = "json"

    if not fixture_path and case:
        fp, k, a = _resolve_fixture_from_case(profile, case, fixtures_dir)
        fixture_path = fp
        kind = k
        assert_cfg = a

    if not fixture_path:
        return {"ok": False, "error": "fixture is required"}

    # overrides / defaults
    def _as_int(v: Any, default: int) -> int:
        try:
            return int(v)
        except Exception:
            return int(default)

    def _as_float(v: Any, default: float) -> float:
        try:
            return float(v)
        except Exception:
            return float(default)

    a_items_min = items_min if items_min is not None else _as_int(assert_cfg.get("items_min"), 1)
    a_unique_ids_min = unique_ids_min if unique_ids_min is not None else _as_int(assert_cfg.get("unique_ids_min"), 0)
    a_ratio = min_nonempty_ratio if min_nonempty_ratio is not None else _as_float(assert_cfg.get("min_nonempty_ratio"), 0.3)

    # columns list
    col_list: list[str]
    if columns_nonempty is not None:
        col_list = [x.strip() for x in str(columns_nonempty).split(",") if x.strip()]
    else:
        raw_cols = assert_cfg.get("columns_nonempty")
        if isinstance(raw_cols, list) and raw_cols:
            col_list = [str(x) for x in raw_cols if str(x).strip()]
        else:
            col_list = ["item_key"]  # безопасный минимум

    # schema
    if not schema:
        if isinstance(assert_cfg.get("schema"), str):
            schema = str(assert_cfg.get("schema"))

    # run explain (it computes columns coverage)
    rep_ex = explain_profile(
        profile,
        fixture_path=fixture_path,
        case=None,
        fixtures_dir=fixtures_dir,
        schema=schema,
        ctx=None,
        max_items=max_items,
        show_items=0,
    )

    issues: list[_Issue] = []
    if not rep_ex.get("ok"):
        return {"ok": False, "error": rep_ex.get("error"), "issues": [], "explain": rep_ex}

    items = rep_ex.get("items") or {}
    items_count = int(items.get("count") or 0)
    unique_ids_count = int(items.get("unique_ids_count") or 0)

    if items_count < a_items_min:
        issues.append(_Issue("error", "assert.items_min", f"items_count={items_count} < items_min={a_items_min}"))

    if a_unique_ids_min > 0 and unique_ids_count < a_unique_ids_min:
        issues.append(_Issue("error", "assert.unique_ids_min", f"unique_ids_count={unique_ids_count} < unique_ids_min={a_unique_ids_min}"))

    # map column -> ratio
    col_cov: dict[str, float] = {}
    exp = rep_ex.get("export") or {}
    cols = exp.get("columns")
    if isinstance(cols, list):
        for c in cols:
            if isinstance(c, dict):
                name = str(c.get("name") or "")
                try:
                    col_cov[name] = float(c.get("nonempty_ratio") or 0.0)
                except Exception:
                    col_cov[name] = 0.0

    for cn in col_list:
        if cn not in col_cov:
            issues.append(_Issue("error", f"assert.columns_nonempty.{cn}", "column not found in export schema"))
            continue
        r = col_cov.get(cn, 0.0)
        if r < a_ratio:
            issues.append(_Issue("error", f"assert.columns_nonempty.{cn}", f"nonempty_ratio={r:.2f} < min_nonempty_ratio={a_ratio:.2f}"))

    ok = not any(x.level == "error" for x in issues)

    return {
        "ok": ok,
        "profile": profile.name,
        "fixture": str(fixture_path),
        "schema": rep_ex.get("export", {}).get("schema"),
        "assert": {
            "items_min": a_items_min,
            "unique_ids_min": a_unique_ids_min,
            "columns_nonempty": col_list,
            "min_nonempty_ratio": a_ratio,
        },
        "issues": [{"level": x.level, "path": x.path, "message": x.message} for x in issues],
        "stats": rep_ex.get("items"),
    }


def format_verify_text(rep: dict[str, Any]) -> str:
    if not rep:
        return "(empty)"
    if rep.get("ok"):
        head = "VERIFY: OK"
    else:
        head = "VERIFY: FAIL"

    lines: list[str] = [head]
    if rep.get("error"):
        lines.append(str(rep.get("error")))
        return "\n".join(lines)

    lines.append(f"Profile: {rep.get('profile')}")
    lines.append(f"Fixture: {rep.get('fixture')}")
    lines.append(f"Schema: {rep.get('schema')}")

    a = rep.get("assert") or {}
    lines.append(f"Asserts: items_min={a.get('items_min')} unique_ids_min={a.get('unique_ids_min')} min_nonempty_ratio={a.get('min_nonempty_ratio')} columns_nonempty={a.get('columns_nonempty')}")

    st = rep.get("stats") or {}
    lines.append(f"Stats: items={st.get('count', 0)} unique_ids={st.get('unique_ids_count', 0)} id_coverage={st.get('id_coverage', 0):.2f}")

    issues = rep.get("issues")
    if isinstance(issues, list) and issues:
        lines.append("Issues:")
        for it in issues:
            lines.append(f"  - {it.get('path')}: {it.get('message')}")

    return "\n".join(lines)
