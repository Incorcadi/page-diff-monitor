from __future__ import annotations

"""Offline checks for profile fixtures (JSON/HTML) without network."""

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Optional


from .extractors import extract_items_any, ids_of

from . import export_csv as export_mod


@dataclass
class _Issue:
    level: str  # error|warn
    case: str
    message: str


def _read_json(path: str) -> Any:
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def _as_float(v: Any, default: float) -> float:
    try:
        return float(v)
    except Exception:
        return float(default)


def _as_int(v: Any, default: int) -> int:
    try:
        return int(v)
    except Exception:
        return int(default)


def _resolve_tests_cfg(profile: SiteProfile, fixtures_dir: Optional[str]) -> tuple[str, list[dict[str, Any]]]:
    meta = profile.meta if isinstance(profile.meta, dict) else {}
    tests = meta.get("tests") if isinstance(meta, dict) else None
    if not isinstance(tests, dict):
        tests = {}

    fd = fixtures_dir or tests.get("fixtures_dir")
    if not isinstance(fd, str) or not fd.strip():
        fd = "tests/fixtures"

    cases = tests.get("cases")
    if isinstance(cases, list) and cases:
        return str(fd), [c for c in cases if isinstance(c, dict)]

    p = Path(fd)
    out: list[dict[str, Any]] = []
    if p.exists() and p.is_dir():
        for fp in sorted(p.glob("*.json")):
            out.append({"name": fp.stem, "file": fp.name, "kind": "json", "assert": {}})
        for fp in sorted(p.glob("*.html")):
            out.append({"name": fp.stem, "file": fp.name, "kind": "html", "assert": {}})
        for fp in sorted(p.glob("*.htm")):
            out.append({"name": fp.stem, "file": fp.name, "kind": "html", "assert": {}})
    return str(fd), out


def _resolve_export_schema(profile: SiteProfile, schema: Optional[str], case: dict[str, Any]) -> tuple[str, Optional[Any]]:
    meta = profile.meta if isinstance(profile.meta, dict) else {}
    export_cfg = meta.get("export") if isinstance(meta, dict) else None
    if not isinstance(export_cfg, dict):
        export_cfg = {}

    default_schema = export_cfg.get("default_schema")
    sch = schema
    if not sch:
        a = case.get("assert")
        if isinstance(a, dict) and isinstance(a.get("schema"), str):
            sch = a.get("schema")
    if not sch:
        sch = default_schema if isinstance(default_schema, str) and default_schema else "default"

    schemas = export_cfg.get("schemas")
    if not isinstance(schemas, dict):
        return sch, None

    s_cfg = schemas.get(sch)
    if not isinstance(s_cfg, dict):
        return sch, None

    if isinstance(s_cfg.get("columns"), list):
        return sch, s_cfg.get("columns")
    if isinstance(s_cfg.get("columns_map"), dict):
        return sch, s_cfg.get("columns_map")
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
            if isinstance(spec, dict):
                col = dict(spec)
            else:
                col = {"path": spec}
            col.setdefault("name", name)
            out.append(col)
        return out
    return None


def _value_by_column(item: Any, col: dict[str, Any], *, ctx: dict[str, Any], extract_spec: Any) -> str:
    fn = getattr(export_mod, "_value_by_column", None)
    if callable(fn):
        return fn(item, col, ctx=ctx, extract_spec=extract_spec)

    if isinstance(item, dict):
        v = item.get(col.get("path"))
        return "" if v is None else str(v)
    return ""


def run_offline_tests(
    profile: SiteProfile,
    *,
    fixtures_dir: Optional[str] = None,
    only_case: Optional[str] = None,
    schema: Optional[str] = None,
    max_items: int = 50,
) -> dict[str, Any]:
    fd, cases = _resolve_tests_cfg(profile, fixtures_dir)
    if isinstance(only_case, str) and only_case.strip():
        cases = [c for c in cases if str(c.get("name")) == only_case]

    issues: list[_Issue] = []
    case_reports: list[dict[str, Any]] = []
    base_dir = Path(fd)

    for c in cases:
        name = str(c.get("name") or "case")
        kind = str(c.get("kind") or "json").lower()
        file_rel = str(c.get("file") or "")
        if not file_rel:
            issues.append(_Issue("error", name, "case.file is required"))
            continue

        fp = (base_dir / file_rel).resolve()
        if not fp.exists():
            issues.append(_Issue("error", name, f"fixture file not found: {fp}"))
            continue

        data: Any = None
        payload_kind = kind
        if kind == "json":
            try:
                data = _read_json(str(fp))
            except Exception as e:
                issues.append(_Issue("error", name, f"cannot read JSON fixture: {e}"))
                continue
        elif kind in ("html", "htm"):
            payload_kind = "html"
            try:
                data = fp.read_text(encoding="utf-8", errors="ignore")
            except Exception as e:
                issues.append(_Issue("error", name, f"cannot read HTML fixture: {e}"))
                continue
        else:
            issues.append(_Issue("error", name, f"unsupported case.kind={kind!r}, expected json/html"))
            continue

        rep_case: dict[str, Any] = {"name": name, "file": str(fp), "kind": payload_kind}

        try:
            items = extract_items_any(data, profile.extract, payload_kind=payload_kind)
        except Exception as e:
            issues.append(_Issue("error", name, f"extract_items failed: {e}"))
            case_reports.append(rep_case)
            continue

        rep_case["items"] = len(items)
        a = c.get("assert") if isinstance(c.get("assert"), dict) else {}

        items_min = _as_int((a or {}).get("items_min"), 1)
        if len(items) < items_min:
            issues.append(_Issue("error", name, f"items count {len(items)} < items_min {items_min}"))

        try:
            ids = ids_of(items, profile.extract)
        except Exception:
            ids = set()
        rep_case["unique_ids"] = len(ids)
        unique_ids_min = _as_int((a or {}).get("unique_ids_min"), 0)
        if unique_ids_min > 0 and len(ids) < unique_ids_min:
            issues.append(_Issue("warn", name, f"unique_ids {len(ids)} < unique_ids_min {unique_ids_min} (maybe id_path wrong?)"))

        sch_name, cols_spec = _resolve_export_schema(profile, schema, c)
        rep_case["schema"] = sch_name
        cols = _normalize_columns(cols_spec)
        if cols is None:
            issues.append(_Issue("warn", name, f"export schema '{sch_name}' not found or has no columns/columns_map"))
            case_reports.append(rep_case)
            continue

        rep_case["columns"] = [str(x.get("name")) for x in cols]
        cols_nonempty = (a or {}).get("columns_nonempty")
        if not isinstance(cols_nonempty, list):
            cols_nonempty = []

        min_ratio = _as_float((a or {}).get("min_nonempty_ratio"), 0.5)
        min_ratio = max(0.0, min(1.0, min_ratio))

        ctx: dict[str, Any] = {}
        export_cfg = (profile.meta or {}).get("export") if isinstance(profile.meta, dict) else None
        if isinstance(export_cfg, dict) and isinstance(export_cfg.get("ctx_defaults"), dict):
            ctx.update(export_cfg.get("ctx_defaults") or {})

        sample = items[: max(1, int(max_items or 50))]

        for col_name in [str(x) for x in cols_nonempty]:
            col = next((cc for cc in cols if str(cc.get("name")) == col_name), None)
            if col is None:
                issues.append(_Issue("error", name, f"columns_nonempty refers to missing column: {col_name}"))
                continue

            nonempty = 0
            total = 0
            for it in sample:
                if not isinstance(it, dict):
                    continue
                total += 1
                v = _value_by_column(it, col, ctx=ctx, extract_spec=profile.extract)
                if v is not None and str(v) != "":
                    nonempty += 1

            ratio = (nonempty / total) if total else 0.0
            rep_case.setdefault("columns_nonempty_stats", {})[col_name] = {
                "nonempty": nonempty,
                "total": total,
                "ratio": ratio,
                "min_ratio": min_ratio,
            }

            if total == 0:
                issues.append(_Issue("warn", name, f"no dict-items in sample to validate column '{col_name}'"))
            elif ratio < min_ratio:
                issues.append(_Issue("warn", name, f"column '{col_name}' nonempty ratio {ratio:.2f} < {min_ratio:.2f} (check paths/defaults/compute)"))

        case_reports.append(rep_case)

    ok = not any(i.level == "error" for i in issues)
    return {
        "ok": ok,
        "profile": profile.name,
        "fixtures_dir": str(base_dir),
        "cases": case_reports,
        "issues": [{"level": i.level, "case": i.case, "message": i.message} for i in issues],
    }


def format_report_text(rep: dict[str, Any]) -> str:
    ok = bool(rep.get("ok"))
    lines: list[str] = []
    lines.append(f"offline-test: {'OK' if ok else 'FAIL'}  profile={rep.get('profile')}  fixtures={rep.get('fixtures_dir')}")

    issues = rep.get("issues")
    if isinstance(issues, list) and issues:
        lines.append("Issues:")
        for it in issues:
            if not isinstance(it, dict):
                continue
            lines.append(f"- {it.get('level')}: case={it.get('case')} - {it.get('message')}")

    cases = rep.get("cases")
    if isinstance(cases, list) and cases:
        lines.append("Cases:")
        for c in cases:
            if not isinstance(c, dict):
                continue
            nm = c.get("name")
            lines.append(f"- {nm}: kind={c.get('kind')} items={c.get('items')} unique_ids={c.get('unique_ids')} schema={c.get('schema')}")

    return "\n".join(lines)
