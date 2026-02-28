from __future__ import annotations

"""profile_lint.py — валидатор профиля (lint).

Это не "тесты ради тестов". Это технологический контроль (ОТК) перед запуском:
проверяем, что профиль хотя бы структурно корректный, и что контракт экспорта описан без ошибок.

Что lint УМЕЕТ:
- проверка обязательных полей (url, method, pagination.kind)
- проверка структуры `_meta.export.schemas.*.columns_map`
- базовая проверка dot-path синтаксиса

Что lint ОСОЗНАННО НЕ ДЕЛАЕТ:
- не ходит в сеть
- не проверяет, что items_path реально существует в ответе (для этого нужен snapshot-тест)
"""

from dataclasses import dataclass
from typing import Any, Iterable, Optional


_ALLOWED_METHODS = {"GET", "POST"}
_ALLOWED_PAGINATION = {"page", "offset", "cursor_token", "next_url", "unknown"}
_ALLOWED_EXTRACT_MODES = {"json", "html", "auto"}
_ALLOWED_TYPES = {
    "str",
    "int",
    "float",
    "bool",
    "date",
    "datetime",
    "url",
    "json",
}


@dataclass
class LintIssue:
    level: str   # error|warn
    path: str
    message: str


def _is_dot_path(s: str) -> bool:
    if not isinstance(s, str) or not s.strip():
        return False
    # Разрешаем "a.b.0.c" (ключи/индексы). Никаких "..", пустых сегментов.
    parts = s.split(".")
    if any(p == "" for p in parts):
        return False
    return True


def _iter_columns_map(columns_map: Any) -> Iterable[tuple[str, dict[str, Any]]]:
    if not isinstance(columns_map, dict):
        return []
    out = []
    for col, spec in columns_map.items():
        if isinstance(col, str) and isinstance(spec, dict):
            out.append((col, spec))
    return out


def lint_profile_dict(profile: dict[str, Any]) -> list[LintIssue]:
    issues: list[LintIssue] = []

    # --- basic ---
    url = profile.get("url")
    if not isinstance(url, str) or not url.strip():
        issues.append(LintIssue("error", "url", "url обязателен и должен быть строкой"))

    method = str(profile.get("method", "GET")).upper()
    if method not in _ALLOWED_METHODS:
        issues.append(LintIssue("error", "method", f"method должен быть одним из: {sorted(_ALLOWED_METHODS)}"))

    pag = profile.get("pagination") or {}
    kind = (pag.get("kind") or "unknown")
    if kind not in _ALLOWED_PAGINATION:
        issues.append(LintIssue("error", "pagination.kind", f"kind должен быть одним из: {sorted(_ALLOWED_PAGINATION)}"))

    if kind == "page":
        if not isinstance(pag.get("page_param") or "page", str):
            issues.append(LintIssue("error", "pagination.page_param", "page_param должен быть строкой"))
    if kind == "offset":
        if not isinstance(pag.get("offset_param") or "offset", str):
            issues.append(LintIssue("error", "pagination.offset_param", "offset_param должен быть строкой"))
    if kind == "cursor_token":
        if not isinstance(pag.get("cursor_param"), str) or not str(pag.get("cursor_param")).strip():
            issues.append(LintIssue("warn", "pagination.cursor_param", "для cursor_token желательно задать cursor_param"))

    ext = profile.get("extract") or {}
    items_path = ext.get("items_path")
    if items_path is not None:
        if not isinstance(items_path, str) or not items_path.strip():
            issues.append(LintIssue("error", "extract.items_path", "items_path должен быть строкой"))
        elif not _is_dot_path(items_path):
            issues.append(LintIssue("warn", "extract.items_path", "items_path выглядит не как dot-path (a.b.0.c)"))

    id_path = ext.get("id_path")
    if id_path is not None:
        if not isinstance(id_path, str) or not id_path.strip():
            issues.append(LintIssue("error", "extract.id_path", "id_path должен быть строкой"))
        elif not _is_dot_path(id_path):
            issues.append(LintIssue("warn", "extract.id_path", "id_path выглядит не как dot-path"))

    mode = str(ext.get("mode") or "json").lower()
    if mode not in _ALLOWED_EXTRACT_MODES:
        issues.append(LintIssue("error", "extract.mode", f"mode must be one of: {sorted(_ALLOWED_EXTRACT_MODES)}"))
    if mode in ("html", "auto"):
        sel = ext.get("html_items_selector")
        if not isinstance(sel, str) or not sel.strip():
            issues.append(LintIssue("warn", "extract.html_items_selector", "for HTML mode set html_items_selector"))

    html_id_attr = ext.get("html_id_attr")
    if html_id_attr is not None and (not isinstance(html_id_attr, str) or not html_id_attr.strip()):
        issues.append(LintIssue("error", "extract.html_id_attr", "html_id_attr must be non-empty string"))

    html_fields = ext.get("html_fields")
    if html_fields is not None:
        if not isinstance(html_fields, dict):
            issues.append(LintIssue("error", "extract.html_fields", "html_fields must be object: {field: rule}"))
        else:
            for k, v in html_fields.items():
                if not isinstance(k, str) or not k.strip():
                    issues.append(LintIssue("error", "extract.html_fields", "html_fields keys must be non-empty strings"))
                    continue
                if isinstance(v, str):
                    if not v.strip():
                        issues.append(LintIssue("error", f"extract.html_fields.{k}", "rule must not be empty"))
                elif isinstance(v, list):
                    if not v:
                        issues.append(LintIssue("error", f"extract.html_fields.{k}", "rule list must not be empty"))
                    else:
                        bad = [x for x in v if not isinstance(x, str) or not x.strip()]
                        if bad:
                            issues.append(LintIssue("error", f"extract.html_fields.{k}", "rule list must contain non-empty strings"))
                else:
                    issues.append(LintIssue("error", f"extract.html_fields.{k}", "rule must be string or list[str]"))

    # --- export contract ---
    meta = profile.get("_meta") or profile.get("meta") or {}
    if not isinstance(meta, dict):
        meta = {}

    export = meta.get("export")
    if export is None:
        # это не ошибка: профиль может быть для диагностики
        return issues

    if not isinstance(export, dict):
        issues.append(LintIssue("error", "_meta.export", "_meta.export должен быть объектом"))
        return issues

    schemas = export.get("schemas")
    if not isinstance(schemas, dict) or not schemas:
        issues.append(LintIssue("warn", "_meta.export.schemas", "нет export.schemas — CSV экспорт будет примитивным"))
        return issues

    for schema_name, schema in schemas.items():
        if not isinstance(schema, dict):
            issues.append(LintIssue("error", f"_meta.export.schemas.{schema_name}", "schema должен быть объектом"))
            continue

        columns_map = schema.get("columns_map")
        if columns_map is None:
            issues.append(LintIssue("warn", f"_meta.export.schemas.{schema_name}.columns_map", "нет columns_map"))
            continue

        if not isinstance(columns_map, dict) or not columns_map:
            issues.append(LintIssue("error", f"_meta.export.schemas.{schema_name}.columns_map", "columns_map должен быть непустым объектом"))
            continue

        for col, spec in _iter_columns_map(columns_map):
            base_path = f"_meta.export.schemas.{schema_name}.columns_map.{col}"

            tp = spec.get("type")
            if tp is not None and str(tp) not in _ALLOWED_TYPES:
                issues.append(LintIssue("warn", f"{base_path}.type", f"неизвестный type={tp!r}. Разрешено: {sorted(_ALLOWED_TYPES)}"))

            has_path = isinstance(spec.get("path"), str)
            has_paths = isinstance(spec.get("paths"), list)
            has_const_ref = ("const_ref" in spec)
            has_const_value = ("const" in spec)
            has_compute = ("compute" in spec)

            if not (has_path or has_paths or has_const_ref or has_const_value or has_compute):
                issues.append(LintIssue("error", base_path, "нужно одно из: path | paths | const_ref | const | compute"))
                continue

            if has_compute:
                kind = spec.get("compute")
                if kind not in ("item_id", "item_key"):
                    issues.append(LintIssue("warn", f"{base_path}.compute", "compute должен быть 'item_id' или 'item_key'"))

            if has_path:
                p = spec.get("path")
                # path == "" разрешаем как "весь объект" (удобно для raw_json)
                if isinstance(p, str) and p == "":
                    pass
                elif not _is_dot_path(p):
                    issues.append(LintIssue("warn", f"{base_path}.path", "path выглядит не как dot-path"))

            if has_paths:
                paths = spec.get("paths")
                if not paths:
                    issues.append(LintIssue("error", f"{base_path}.paths", "paths не должен быть пустым"))
                else:
                    for i, p in enumerate(paths):
                        if not isinstance(p, str) or not p.strip():
                            issues.append(LintIssue("error", f"{base_path}.paths[{i}]", "каждый paths[i] должен быть строкой"))
                        elif not _is_dot_path(p):
                            issues.append(LintIssue("warn", f"{base_path}.paths[{i}]", "paths[i] выглядит не как dot-path"))


    return issues


def format_issues_text(issues: list[LintIssue]) -> str:
    if not issues:
        return "OK: профиль прошёл lint (структура выглядит корректно)."

    # grouped
    lines: list[str] = []
    errs = [x for x in issues if x.level == "error"]
    warns = [x for x in issues if x.level != "error"]

    if errs:
        lines.append(f"ERRORS: {len(errs)}")
        for it in errs:
            lines.append(f"  - {it.path}: {it.message}")

    if warns:
        lines.append(f"WARNINGS: {len(warns)}")
        for it in warns:
            lines.append(f"  - {it.path}: {it.message}")

    return "\n".join(lines)
