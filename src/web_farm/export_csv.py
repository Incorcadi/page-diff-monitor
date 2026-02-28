from __future__ import annotations

"""
export_csv.py — универсальный экспорт результатов в CSV по "контракту" колонок.

Поддержка режимов:
1) fields=["a","b.c","arr.0.x"] — старый режим (колонки = список полей).
2) columns=[{ColumnSpec}, ...]  — режим "контракта" из профиля (_meta.export.schemas).
3) columns_map={name: ColumnSpecWithoutName, ...} — инженерный режим (удобно для deep_merge).

ColumnSpec (dict):
- name: имя колонки (обязательно)
- path: dot-path (один) ; path == "" означает "весь объект" (удобно для raw_json).
- paths: список dot-path (coalesce: берём первое непустое)
- const: literal-константа (если ключ "const" присутствует в ColumnSpec)
- const_ref: имя переменной, которую надо взять из ctx (передаётся из CLI)
- compute: "item_id" | "item_key"  (опционально; требует extract_spec)
- default: значение по умолчанию, если поле не найдено/пусто
- type: "str" | "int" | "float" | "bool" | "json"

Зачем ctx/const_ref:
- run_id, batch_id, source_profile, fetched_at и любые "константы профиля" (country/currency/category)
  задаются один раз на запуск и подставляются в каждую строку при экспорте.

Важно:
- Этот модуль НЕ привязан к конкретному сайту.
- Для compute=item_id/item_key можно (опционально) передать extract_spec=profile.extract.
"""

import csv
import json
import re
import sqlite3
import hashlib
from pathlib import Path
from typing import Any, Optional, Sequence



# --- schema helpers: columns_map -> columns(list) ---

def _columns_from_map(columns_map: dict[str, Any]) -> list[dict[str, Any]]:
    """Преобразовать columns_map в список ColumnSpec.

    Формат:
      columns_map = {
        "id": {"paths": [...], "type": "str", "default": "", "pos": 10},
        "title": {"path": "title", "pos": 20},
        ...
      }

    Правило порядка:
      - сначала сортируем по pos (если есть, иначе 10**9),
      - затем по имени колонки.
    """
    cols: list[dict[str, Any]] = []
    for name, spec in (columns_map or {}).items():
        if isinstance(spec, dict):
            if spec.get("enabled") is False or spec.get("_disabled") is True:
                continue
        if not isinstance(spec, dict):
            # допускаем короткую форму: "title": "title.path"
            spec = {"path": spec}
        col = dict(spec)
        col.setdefault("name", name)
        cols.append(col)

    def _pos(c: dict[str, Any]) -> int:
        v = c.get("pos")
        try:
            return int(v)
        except Exception:
            return 10**9

    cols.sort(key=lambda c: (_pos(c), str(c.get("name", ""))))
    return cols


def _normalize_columns(columns: Any) -> Optional[list[dict[str, Any]]]:
    """Принять columns как list[dict] или columns_map (dict[name] -> dictSpec)."""
    if columns is None:
        return None
    if isinstance(columns, list):
        return columns
    if isinstance(columns, dict):
        return _columns_from_map(columns)
    raise TypeError(f"columns must be list or dict (columns_map), got {type(columns)!r}")

# dot-path helper
from .json_path import get_by_path

# keying helpers (optional)
from .site_profile import ExtractSpec
from .keying import extract_item_id as _extract_item_id
from .keying import make_item_key as _make_item_key


def _stringify_json(v: Any) -> str:
    try:
        return json.dumps(v, ensure_ascii=False)
    except Exception:
        return str(v)


_RE_FLOAT = re.compile(r"-?\d+(?:[\.,]\d+)?")
_RE_INT = re.compile(r"-?\d+")


def _cast(v: Any, typ: str) -> str:
    if v is None:
        return ""
    if typ == "json":
        return _stringify_json(v)
    if typ == "str":
        return str(v)
    if typ == "bool":
        if isinstance(v, bool):
            return "true" if v else "false"
        s = str(v).strip().lower()
        if s in ("1", "true", "yes", "y", "да", "ok"):
            return "true"
        if s in ("0", "false", "no", "n", "нет"):
            return "false"
        return ""
    if typ == "int":
        if isinstance(v, int) and not isinstance(v, bool):
            return str(v)
        if isinstance(v, float) and not isinstance(v, bool):
            return str(int(v))
        s = str(v).replace("\u00A0", " ").replace(" ", "")
        m = _RE_INT.search(s)
        if not m:
            return ""
        try:
            return str(int(m.group(0)))
        except Exception:
            return ""
    if typ == "float":
        if isinstance(v, (int, float)) and not isinstance(v, bool):
            return str(float(v))
        s = str(v).replace("\u00A0", " ").replace(" ", "")
        m = _RE_FLOAT.search(s)
        if not m:
            return ""
        num = m.group(0).replace(",", ".")
        try:
            return str(float(num))
        except Exception:
            return ""
    # fallback
    if isinstance(v, (dict, list)):
        return _stringify_json(v)
    return str(v)


def _is_empty(v: Any) -> bool:
    return v is None or v == ""


def _value_by_paths(obj: Any, paths: list[str]) -> Any:
    for p in paths:
        if p == "":
            v = obj
        else:
            v = get_by_path(obj, p) if "." in p else (obj.get(p) if isinstance(obj, dict) else None)
        if not _is_empty(v):
            return v
    return None


def _fallback_item_id(obj: Any) -> Optional[str]:
    if not isinstance(obj, dict):
        return None
    for k in ("id", "uuid", "guid", "slug", "item_id", "product_id"):
        v = obj.get(k)
        if not _is_empty(v):
            return str(v)
    # meta.id
    v2 = get_by_path(obj, "meta.id")
    if not _is_empty(v2):
        return str(v2)
    return None


def _fallback_item_key(obj: Any) -> str:
    _id = _fallback_item_id(obj)
    if _id:
        return f"id:{_id}"
    blob = json.dumps(obj, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return "sha1:" + hashlib.sha1(blob).hexdigest()


def _compute_value(obj: Any, kind: str, extract_spec: Any) -> Any:
    if kind == "item_id":
        if _extract_item_id is not None and extract_spec is not None:
            try:
                v = _extract_item_id(obj, extract_spec)
                if not _is_empty(v):
                    return v
            except Exception:
                pass
        return _fallback_item_id(obj) or ""
    if kind == "item_key":
        if _make_item_key is not None and extract_spec is not None:
            try:
                return _make_item_key(obj, extract_spec)
            except Exception:
                pass
        return _fallback_item_key(obj)
    return None


def _value_by_column(obj: Any, col: dict[str, Any], *, ctx: Optional[dict[str, Any]] = None, extract_spec: Any = None) -> str:
    typ = str(col.get("type") or "str")

    # 0) compute (на основе item + extract_spec)
    if "compute" in col:
        kind = str(col.get("compute") or "")
        v = _compute_value(obj, kind, extract_spec)
        if _is_empty(v) and "default" in col:
            v = col.get("default")
        return _cast(v, typ)

    # 1) const_ref (из ctx)
    if "const_ref" in col:
        key = str(col.get("const_ref") or "")
        v = (ctx or {}).get(key)
        if _is_empty(v) and "default" in col:
            v = col.get("default")
        return _cast(v, typ)

    # 2) literal const (ключ "const" присутствует)
    if "const" in col:
        v = col.get("const")
        if _is_empty(v) and "default" in col:
            v = col.get("default")
        return _cast(v, typ)

    # 3) paths / path
    if "paths" in col and isinstance(col["paths"], list):
        v = _value_by_paths(obj, [str(x) for x in col["paths"]])
    else:
        p = str(col.get("path") or "")
        if p == "":
            v = obj
        else:
            v = get_by_path(obj, p) if "." in p else (obj.get(p) if isinstance(obj, dict) else None)

    if _is_empty(v) and "default" in col:
        v = col.get("default")

    return _cast(v, typ)


def _infer_fields_from_jsonl(jsonl_path: str, *, probe_lines: int = 200) -> list[str]:
    keys: set[str] = set()
    n = 0
    with open(jsonl_path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                obj = json.loads(line)
            except Exception:
                continue
            if isinstance(obj, dict):
                keys.update(obj.keys())
            n += 1
            if n >= probe_lines:
                break
    return sorted(keys)


def jsonl_to_csv(
    jsonl_path: str,
    csv_path: str,
    *,
    fields: Optional[Sequence[str]] = None,
    columns: Optional[Sequence[dict[str, Any]]] = None,
    ctx: Optional[dict[str, Any]] = None,
    extract_spec: Any = None,
    probe_lines: int = 200,
    limit: Optional[int] = None,
    dialect: str = "excel",
) -> dict[str, Any]:
    """Экспорт JSONL → CSV."""
    jsonl_path = str(jsonl_path)
    csv_path = str(csv_path)
    Path(csv_path).parent.mkdir(parents=True, exist_ok=True)

    headers: list[str]
    if columns is not None:
        headers = [str(c.get("name") or "") for c in columns]
        if any(not h for h in headers):
            raise ValueError("all columns must have non-empty name")
    else:
        if fields is None:
            fields = _infer_fields_from_jsonl(jsonl_path, probe_lines=probe_lines)
        headers = list(fields)

    rows = 0
    with open(jsonl_path, "r", encoding="utf-8") as fin, open(csv_path, "w", encoding="utf-8", newline="") as fout:
        w = csv.DictWriter(fout, fieldnames=headers, dialect=dialect)
        w.writeheader()

        for line in fin:
            if limit is not None and isinstance(limit, int) and limit > 0 and rows >= limit:
                break

            line = line.strip()
            if not line:
                continue

            try:
                obj = json.loads(line)
            except Exception:
                continue

            if columns is not None:
                row = {str(c["name"]): _value_by_column(obj, c, ctx=ctx, extract_spec=extract_spec) for c in columns}
            else:
                row = {}
                for k in headers:
                    if "." in k:
                        val = get_by_path(obj, k)
                    else:
                        val = obj.get(k) if isinstance(obj, dict) else None
                    row[k] = "" if val is None else (_stringify_json(val) if isinstance(val, (dict, list)) else str(val))

            w.writerow(row)
            rows += 1

    rep = {"kind": "jsonl", "in": jsonl_path, "out": csv_path, "rows": rows, "fields": headers}
    if ctx:
        rep["ctx"] = ctx
    return rep


def sqlite_to_csv(
    db_path: str,
    csv_path: str,
    *,
    table: str = "items_unique",
    fields: Optional[Sequence[str]] = None,
    columns: Optional[Sequence[dict[str, Any]]] = None,
    ctx: Optional[dict[str, Any]] = None,
    extract_spec: Any = None,
    probe_rows: int = 200,
    limit: Optional[int] = None,
    dialect: str = "excel",
) -> dict[str, Any]:
    """Экспорт SQLite → CSV.

    По умолчанию читает payload из столбца payload или payload_last.
    """
    db_path = str(db_path)
    csv_path = str(csv_path)
    Path(csv_path).parent.mkdir(parents=True, exist_ok=True)

    conn = sqlite3.connect(db_path)

    payload_col = "payload"
    cols = [r[1] for r in conn.execute(f"PRAGMA table_info({table})")]
    if "payload" in cols:
        payload_col = "payload"
    elif "payload_last" in cols:
        payload_col = "payload_last"
    else:
        conn.close()
        raise RuntimeError(f"Table '{table}' has no payload column (payload/payload_last). Columns: {cols}")

    def iter_payloads(n: int) -> Sequence[str]:
        out: list[str] = []
        q = f"SELECT {payload_col} FROM {table} LIMIT {int(n)}"
        for (payload,) in conn.execute(q):
            out.append(payload)
        return out

    headers: list[str]
    if columns is not None:
        headers = [str(c.get("name") or "") for c in columns]
        if any(not h for h in headers):
            conn.close()
            raise ValueError("all columns must have non-empty name")
    else:
        if fields is None:
            keys: set[str] = set()
            for payload in iter_payloads(probe_rows):
                try:
                    obj = json.loads(payload)
                except Exception:
                    continue
                if isinstance(obj, dict):
                    keys.update(obj.keys())
            fields = sorted(keys)
        headers = list(fields)

    rows = 0
    with open(csv_path, "w", encoding="utf-8", newline="") as fout:
        w = csv.DictWriter(fout, fieldnames=headers, dialect=dialect)
        w.writeheader()

        q = f"SELECT {payload_col} FROM {table}"
        if limit is not None and isinstance(limit, int) and limit > 0:
            q += f" LIMIT {int(limit)}"

        for (payload,) in conn.execute(q):
            try:
                obj = json.loads(payload)
            except Exception:
                continue

            if columns is not None:
                row = {str(c["name"]): _value_by_column(obj, c, ctx=ctx, extract_spec=extract_spec) for c in columns}
            else:
                row = {}
                for k in headers:
                    if "." in k:
                        val = get_by_path(obj, k)
                    else:
                        val = obj.get(k) if isinstance(obj, dict) else None
                    row[k] = "" if val is None else (_stringify_json(val) if isinstance(val, (dict, list)) else str(val))

            w.writerow(row)
            rows += 1

    conn.close()
    rep = {"kind": "sqlite", "in": db_path, "out": csv_path, "rows": rows, "fields": headers, "table": table, "payload_col": payload_col}
    if ctx:
        rep["ctx"] = ctx
    return rep
