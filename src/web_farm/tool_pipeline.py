from __future__ import annotations

"""
tool.py — единая точка входа (CLI) для фермы профилей.

Философия:
- “Один пульт” вместо пачки tool_with_*.
- HTTP — только через HttpEngine.
- Профиль — через site_profile.load_profile (defaults/extends).

Команды:
- triage   : быстрый диагноз (метка + краткая строка), можно папкой
- diagnose : расширенная диагностика + подсказки + patch (опционально apply)
- onboard  : infer пагинации + поиск limit_param + сохранение готового профиля
- run      : прогон одного профиля в JSONL
- farm     : прогон папки профилей (run по каждому)
- pipeline : draft -> fixed -> active -> errors (двухпроходный конвейер)
"""

import argparse
import json
import re
import os
import shutil
import sys
from dataclasses import asdict
from pathlib import Path
from typing import Any, Optional

from .profile_lint import lint_profile_dict, format_issues_text
from . import offline_tests as offline_tests_mod

from .site_profile import SiteProfile, load_profile, save_profile
from .http_engine import HttpEngine, make_retry_policy_from_cfg, build_limiter_factory
from .secret_store import SecretStore

from .extractors import extract_items, ids_of, extract_items_any
from .http_utils import parse_link_next, extract_next_url_from_json, extract_cursor_token
from .resp_read import safe_read_json, read_text_safely

from . import infer as infer_mod
from . import onboard as onboard_mod
from . import runtime as runtime_mod

from .storage_sqlite import DualSqliteStore
from . import export_csv as export_mod

# ----------------------------
# ----------------------------
# Утилиты
# ----------------------------

def _pretty(obj: Any, pretty: bool) -> str:
    return json.dumps(obj, ensure_ascii=False, indent=(2 if pretty else None))


def _read_json(path: str) -> Any:
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def _write_json(path: str, obj: Any, pretty: bool) -> None:
    with open(path, "w", encoding="utf-8") as f:
        json.dump(obj, f, ensure_ascii=False, indent=(2 if pretty else None))


def _iter_profiles(dir_path: str, recursive: bool) -> list[str]:
    p = Path(dir_path)
    if not p.exists():
        return []
    out: list[str] = []
    if recursive:
        for fp in p.rglob("*.json"):
            if fp.name.startswith("_"):
                continue
            if "templates" in fp.parts:
                continue
            out.append(str(fp))
    else:
        for fp in p.glob("*.json"):
            if fp.name.startswith("_"):
                continue
            out.append(str(fp))
    return sorted(out)


def _merge_params(profile: SiteProfile) -> dict[str, Any]:
    # base_params + ничего больше. (pagination добавляется в runtime/infer)
    return dict(profile.base_params or {})


def _build_engine(profile: SiteProfile, args: argparse.Namespace) -> HttpEngine:
    """Создать HttpEngine с учётом meta.http (rate_limit + retries)."""
    http_cfg = profile.meta.get("http") if isinstance(profile.meta, dict) else {}
    if not isinstance(http_cfg, dict):
        http_cfg = {}
    limiter_factory = build_limiter_factory(http_cfg)
    retry_policy = make_retry_policy_from_cfg(http_cfg.get("retries") or {})
    # --- auth: secrets vault (optional)
    auth_hook = None
    store = _get_secret_store(args)
    auth_cfg = profile.meta.get("auth") if isinstance(profile.meta, dict) else None

    # Валидация: если профиль явно требует авторизацию, но secrets vault не подключён — это ошибка конфигурации.
    if _auth_cfg_active(auth_cfg) and store is None:
        raise CliError(
            "Профиль требует секреты (_meta.auth задан), но переменная окружения PARSER_SECRETS_PATH не установлена.\n"
            "Создай локальный secrets.json (не коммить) и задай путь в ENV.\n"
            "PowerShell:  $env:PARSER_SECRETS_PATH='C:\\path\\to\\secrets.json'\n"
            "Постоянно:   setx PARSER_SECRETS_PATH \"C:\\path\\to\\secrets.json\""
        )

    if store is not None and isinstance(auth_cfg, dict):
        auth_hook = store.make_auth_hook(auth_cfg)

    # cache / replay: CLI имеет приоритет над профилем
    cache_dir = getattr(args, "cache_dir", None)
    replay = bool(getattr(args, "replay", False))
    meta_cache = http_cfg.get("cache") if isinstance(http_cfg, dict) else None
    if not cache_dir and isinstance(meta_cache, dict):
        cache_dir = meta_cache.get("dir")
    if not replay and isinstance(meta_cache, dict):
        replay = bool(meta_cache.get("replay"))

    cache_store_statuses = None
    if isinstance(meta_cache, dict):
        sts = meta_cache.get("store_statuses")
        if isinstance(sts, list) and sts:
            try:
                cache_store_statuses = [int(x) for x in sts]
            except Exception:
                cache_store_statuses = None

    return HttpEngine(
        default_timeout=profile.timeout,
        default_headers=profile.headers,
        limiter_factory=limiter_factory,
        retry_policy=retry_policy,
        auth_hook=auth_hook,
        headers_cfg=http_cfg.get("headers") or {},
        diag_http=bool(getattr(args, "diag_http", False)),
        cache_dir=str(cache_dir) if isinstance(cache_dir, str) and cache_dir else None,
        replay=replay,
        cache_store_statuses=cache_store_statuses,
    )



def _guess_items_path(data: Any) -> Optional[str]:
    """
    Супер-эвристика: найти первый путь, где лежит list[dict] (или list вообще).
    Возвращаем dot-path.
    """
    # BFS по dict'ам
    from collections import deque

    def is_good_list(x: Any) -> bool:
        if not isinstance(x, list) or not x:
            return False
        # предпочтение list[dict], но допускаем list любых
        return True

    q = deque([(data, "")])
    visited = 0
    while q and visited < 300:
        cur, path = q.popleft()
        visited += 1
        if isinstance(cur, dict):
            for k, v in cur.items():
                p2 = f"{path}.{k}" if path else str(k)
                if is_good_list(v):
                    return p2
                if isinstance(v, dict):
                    q.append((v, p2))
                # иногда список контейнеров
                if isinstance(v, list) and v and isinstance(v[0], dict):
                    q.append((v[0], p2 + ".0"))
    return None


def _guess_id_path(items: list[Any]) -> Optional[str]:
    """
    Ищем поле, которое похоже на id: id/uuid/guid/... или что-то с большим разнообразием значений.
    """
    if not items:
        return None
    # смотрим первые 50 dict-элементов
    sample = [it for it in items[:50] if isinstance(it, dict)]
    if not sample:
        return None

    common = ["id", "uuid", "guid", "pk", "item_id", "product_id", "slug"]
    for key in common:
        if all((key in it) for it in sample):
            return key

    # эвристика по “разнообразию”
    keys = set().union(*[set(it.keys()) for it in sample])
    best = None
    best_score = 0
    for k in keys:
        vals = []
        for it in sample:
            v = it.get(k)
            if isinstance(v, (str, int)):
                vals.append(str(v))
        uniq = len(set(vals))
        if uniq >= 10 and uniq > best_score:
            best = k
            best_score = uniq
    return best


def _triage(profile: SiteProfile, *, engine: HttpEngine, smoke: int, stagnation_window: int) -> dict[str, Any]:
    """
    Вернуть компактный отчёт triage.
    """
    base = {"url": profile.url, "kind": profile.pagination.kind, "limit_param": profile.pagination.limit_param}

    resp, data, err = engine.safe_get_json(
        profile.url,
        method=profile.method,
        params=_merge_params(profile),
        headers=profile.headers,
        timeout=profile.timeout,
        force_json=False,
        detect_soft=True,
    )
    status = getattr(resp, "status_code", None) if resp is not None else None
    base.update({"status": status, "err": err})

    if err:
        label = "ACCESS" if err.startswith(("timeout", "network_error", "http_")) else err.split(":")[0].upper()
        return {"label": label, **base, "items": 0, "ids": 0}

    if data is None:
        return {"label": "NO_DATA", **base, "items": 0, "ids": 0}

    items = extract_items(data, profile.extract)
    ids = ids_of(items, profile.extract)
    base.update({"items": len(items), "ids": len(ids)})

    if not items:
        return {"label": "NO_ITEMS", **base}
    if len(ids) == 0:
        return {"label": "NO_IDS", **base}

    if smoke <= 0:
        return {"label": "OK", **base}

    # smoke: прогон первых N items и ловим “стагнацию”
    seen: set[str] = set()
    no_new = 0
    got = 0
    for item in runtime_mod.paginate_items(profile, engine=engine):
        got += 1
        item_ids = ids_of([item], profile.extract)
        if item_ids:
            new = len(item_ids - seen)
            if new == 0:
                no_new += 1
            else:
                no_new = 0
                seen |= item_ids
        if no_new >= stagnation_window:
            return {"label": "LOOP", **base, "smoke_items": got, "unique": len(seen)}
        if got >= smoke:
            break

    return {"label": "OK", **base, "smoke_items": got, "unique": len(seen)}


# ----------------------------
# Команды
# ----------------------------

def cmd_triage(args: argparse.Namespace) -> int:
    engine = None  # built per-profile
    only = set([x.strip().upper() for x in (args.only.split(",") if args.only else []) if x.strip()])

    def handle(path: str) -> dict[str, Any]:
        prof = load_profile(path, defaults_path=args.defaults)
        eng = _build_engine(prof, args)
        return {"profile": path, **_triage(prof, engine=eng, smoke=args.smoke, stagnation_window=args.stagnation_window)}

    if args.profile:
        out = handle(args.profile)
        if only and out["label"] not in only:
            return 0
        print(_pretty(out if args.json else out, args.pretty))
        return 0

    results = []
    for p in _iter_profiles(args.profiles_dir, args.recursive):
        try:
            res = handle(p)
        except Exception as e:
            res = {"profile": p, "label": "PROFILE_ERR", "err": str(e)}
        if only and res.get("label") not in only:
            continue
        results.append(res)
        if not args.json:
            print(f'{res["label"]} items={res.get("items","-")} ids={res.get("ids","-")} status={res.get("status","-")}  {p}')

    if args.json:
        print(_pretty(results, args.pretty))
    if args.summary:
        from collections import Counter
        c = Counter([r.get("label") for r in results])
        print("SUMMARY:", dict(c))
    return 0


def cmd_diagnose(args: argparse.Namespace) -> int:
    prof = load_profile(args.profile, defaults_path=args.defaults)
    engine = _build_engine(prof, args)

    resp, data, err = engine.safe_get_json(
        prof.url,
        method=prof.method,
        params=_merge_params(prof),
        headers=prof.headers,
        timeout=prof.timeout,
        force_json=False,
        detect_soft=True,
    )

    report: dict[str, Any] = {
        "profile": args.profile,
        "base": {
            "url": prof.url,
            "method": prof.method,
            "status": getattr(resp, "status_code", None) if resp is not None else None,
            "content_type": (resp.headers.get("Content-Type") if resp is not None else None),
            "err": err,
        },
        "extract": {},
        "hints": {},
        "patch": None,
    }

    if err or data is None:
        print(_pretty(report, args.pretty))
        return 0

    items = extract_items(data, prof.extract)
    ids = ids_of(items, prof.extract)
    report["extract"] = {"items_count": len(items), "unique_ids_count": len(ids)}

    # hints
    patch: dict[str, Any] = {}
    if len(items) == 0:
        guess = _guess_items_path(data)
        if guess:
            report["hints"]["items_path"] = guess
            patch.setdefault("extract", {})["items_path"] = guess

    if len(items) > 0 and len(ids) == 0:
        guess_id = _guess_id_path(items)
        if guess_id:
            report["hints"]["id_path"] = guess_id
            patch.setdefault("extract", {})["id_path"] = guess_id

    # infer + limit probe (опционально)
    if args.infer:
        pag, inf_rep = infer_mod.infer_pagination(prof, engine=engine)
        report["infer"] = {"pagination": asdict(pag), "report": inf_rep}
        # apply into profile copy for later saving if needed
        prof.pagination = pag

    if args.limit_probe:
        limit_param, lim_rep = onboard_mod.find_limit_param(prof, engine=engine)
        report["limit_probe"] = {"limit_param": limit_param, "report": lim_rep}
        if limit_param:
            patch.setdefault("pagination", {})["limit_param"] = limit_param

    report["patch"] = patch or None

    # apply patch if asked
    if args.apply and patch:
        # apply to dict form then from_dict
        d = prof.to_dict()
        # merge patch into d
        def deep_merge(a, b):
            out = dict(a)
            for k, v in b.items():
                if k in out and isinstance(out[k], dict) and isinstance(v, dict):
                    out[k] = deep_merge(out[k], v)
                else:
                    out[k] = v
            return out
        new_d = deep_merge(d, patch)
        new_prof = SiteProfile.from_dict(new_d)
        out_path = args.apply_out or args.profile
        save_profile(new_prof, out_path, pretty=args.pretty)
        report["apply_out"] = out_path

    print(_pretty(report, args.pretty))
    return 0


def cmd_onboard(args: argparse.Namespace) -> int:
    prof = load_profile(args.in_path, defaults_path=args.defaults)
    engine = _build_engine(prof, args)

    # 1) infer пагинации
    pag, inf_rep = infer_mod.infer_pagination(prof, engine=engine)
    prof.pagination = pag

    # 2) probe limit
    limit_param, lim_rep = onboard_mod.find_limit_param(prof, engine=engine)
    if limit_param:
        prof.pagination.limit_param = limit_param

    save_profile(prof, args.out_path, pretty=args.pretty)

    out = {
        "in": args.in_path,
        "out": args.out_path,
        "pagination": asdict(prof.pagination),
        "limit_param": limit_param,
        "infer_report": inf_rep,
        "limit_report": lim_rep,
    }
    if args.print_report:
        print(_pretty(out, args.pretty))
    return 0


def cmd_lint(args: argparse.Namespace) -> int:
    """Статический контроль профиля (без сети)."""
    prof = load_profile(args.profile, defaults_path=args.defaults)
    d = prof.to_dict()
    issues = lint_profile_dict(d)
    has_errors = any(x.level == "error" for x in issues)

    if getattr(args, "json", False):
        out = {
            "ok": not has_errors,
            "issues": [
                {"level": x.level, "path": x.path, "message": x.message}
                for x in issues
            ],
        }
        print(_pretty(out, getattr(args, "pretty", False)))
    else:
        print(format_issues_text(issues))

    return 2 if has_errors else 0


def cmd_offline_test(args: argparse.Namespace) -> int:
    """Офлайн-тесты по профилю на сохранённых фикстурах (JSON/HTML), без сети.

    Идея: профили — это "станки". Офлайн-тесты — это ОТК, который проверяет:
    - что extractor действительно извлекает items (items_min)
    - что id/key находятся
    - что экспортный контракт (schema) даёт непустые колонки
    """
    if offline_tests_mod is None:
        raise CliError("Не найден offline_tests.py. Положи модуль рядом со скриптом или в пакет web_farm.")

    prof = load_profile(args.profile, defaults_path=args.defaults)
    rep = offline_tests_mod.run_offline_tests(
        prof,
        fixtures_dir=getattr(args, "fixtures_dir", None),
        only_case=getattr(args, "case", None),
        schema=getattr(args, "schema", None),
        max_items=int(getattr(args, "max_items", 50) or 50),
    )

    if bool(getattr(args, "json", False)):
        print(_pretty(rep, pretty=bool(getattr(args, "pretty", False))))
    else:
        print(offline_tests_mod.format_report_text(rep))

    return 0 if rep.get("ok") else 1


def _project_root_hint() -> str:
    return (
        "Похоже, ты запускаешь CLI не из корня репозитория.\n"
        "Для команды demo нужны файлы в папках examples/ и tests/fixtures/.\n\n"
        "Решение: открой терминал в папке проекта (где лежит pyproject.toml) и повтори команду."
    )


def _resolve_demo_profile(name: str) -> str:
    """Вернуть путь к демо-профилю по краткому имени."""
    nm = str(name).strip().lower()
    mapping = {
        "jsonplaceholder": "examples/profiles/jsonplaceholder_posts_page.json",
        "pokeapi": "examples/profiles/pokeapi_pokemon_next.json",
    }
    if nm not in mapping:
        raise CliError(f"Unknown demo name: {name!r}. Expected one of: {', '.join(sorted(mapping))}.")
    path = mapping[nm]
    if not Path(path).exists():
        raise CliError(_project_root_hint())
    return path


def _iter_demo_names(name: str) -> list[str]:
    nm = str(name).strip().lower()
    if nm in ("all", "*"):
        return ["jsonplaceholder", "pokeapi"]
    return [nm]


def _resolve_export_columns(profile: SiteProfile, schema: str) -> Optional[list[dict[str, Any]]]:
    meta = profile.meta if isinstance(profile.meta, dict) else {}
    export_cfg = meta.get("export") if isinstance(meta, dict) else None
    if not isinstance(export_cfg, dict):
        return None

    schemas = export_cfg.get("schemas")
    if not isinstance(schemas, dict):
        return None
    s = schemas.get(schema)
    if not isinstance(s, dict):
        return None

    cols_spec: Any = None
    if isinstance(s.get("columns"), list):
        cols_spec = s.get("columns")
    elif isinstance(s.get("columns_map"), dict):
        cols_spec = s.get("columns_map")

    norm = getattr(export_mod, "_normalize_columns", None)
    if callable(norm):
        return norm(cols_spec)

    # fallback
    if isinstance(cols_spec, list):
        return cols_spec
    if isinstance(cols_spec, dict):
        out: list[dict[str, Any]] = []
        for k, v in cols_spec.items():
            if isinstance(v, dict):
                col = dict(v)
            else:
                col = {"path": v}
            col.setdefault("name", k)
            out.append(col)
        return out
    return None


def cmd_demo(args: argparse.Namespace) -> int:
    """Portfolio-friendly 1-command demo.

    Per demo profile:
      1) runs offline-test on bundled fixtures (no network)
      2) extracts items from fixtures into out/<profile>.example.jsonl
      3) exports CSV using the profile export schema(s)
    """
    out_dir = Path(getattr(args, "out_dir", "out") or "out").resolve()
    out_dir.mkdir(parents=True, exist_ok=True)

    demo_names = _iter_demo_names(getattr(args, "name", "all"))

    exit_code = 0
    for demo in demo_names:
        prof_path = _resolve_demo_profile(demo)
        prof = load_profile(prof_path, defaults_path=args.defaults)

        rep = offline_tests_mod.run_offline_tests(
            prof,
            fixtures_dir=None,
            only_case=None,
            schema=getattr(args, "schema", None),
            max_items=int(getattr(args, "max_items", 50) or 50),
        )
        print("\n" + "=" * 60)
        print(f"DEMO: {demo}  |  profile: {prof.name}")
        print("=" * 60)
        print(offline_tests_mod.format_report_text(rep))

        if not rep.get("ok"):
            exit_code = 1
            if getattr(args, "fail_fast", False):
                return exit_code

        # --- build aggregated JSONL from fixtures
        meta = prof.meta if isinstance(prof.meta, dict) else {}
        tests = meta.get("tests") if isinstance(meta, dict) else None
        if not isinstance(tests, dict):
            raise CliError(f"Profile {prof.name} has no _meta.tests config")
        fixtures_dir = str(tests.get("fixtures_dir") or "tests/fixtures")
        base_dir = Path(fixtures_dir)
        if not base_dir.exists():
            raise CliError(_project_root_hint())
        cases = tests.get("cases") if isinstance(tests.get("cases"), list) else []

        jsonl_path = out_dir / f"{prof.name}.example.jsonl"
        written = 0
        with open(jsonl_path, "w", encoding="utf-8") as fout:
            for c in cases:
                if not isinstance(c, dict):
                    continue
                kind = str(c.get("kind") or "json").lower()
                file_rel = str(c.get("file") or "")
                if not file_rel:
                    continue
                fp = (base_dir / file_rel).resolve()
                if not fp.exists():
                    continue

                payload_kind = "json" if kind == "json" else "html"
                if payload_kind == "json":
                    try:
                        payload = json.loads(fp.read_text(encoding="utf-8"))
                    except Exception:
                        continue
                else:
                    payload = fp.read_text(encoding="utf-8", errors="ignore")

                try:
                    items = extract_items_any(payload, prof.extract, payload_kind=payload_kind)
                except Exception:
                    continue

                max_items = int(getattr(args, "items", 0) or 0)
                if max_items > 0:
                    items = items[:max_items]

                for it in items:
                    obj = it if isinstance(it, dict) else {"value": it}
                    fout.write(json.dumps(obj, ensure_ascii=False) + "\n")
                    written += 1

        print(f"Artifacts: {jsonl_path}  (rows={written})")

        # --- export CSV
        export_cfg = meta.get("export") if isinstance(meta, dict) else None
        if not isinstance(export_cfg, dict):
            continue

        ctx: dict[str, Any] = {}
        if isinstance(export_cfg.get("ctx_defaults"), dict):
            ctx.update(export_cfg.get("ctx_defaults") or {})

        if getattr(args, "all_schemas", False):
            sch_list = sorted(list((export_cfg.get("schemas") or {}).keys()))
        else:
            sch = getattr(args, "schema", None)
            if not sch:
                ds = export_cfg.get("default_schema")
                sch = str(ds) if isinstance(ds, str) and ds else "default"
            sch_list = [str(sch)]

        for sch in sch_list:
            cols = _resolve_export_columns(prof, sch)
            if not cols:
                print(f"Skip CSV export: schema '{sch}' not found")
                continue
            csv_path = out_dir / f"{prof.name}.{sch}.csv"
            rep_csv = export_mod.jsonl_to_csv(
                str(jsonl_path),
                str(csv_path),
                columns=cols,
                ctx=ctx,
                extract_spec=prof.extract,
            )
            print(f"Artifacts: {csv_path}  (rows={rep_csv.get('rows')})")

    return exit_code


def _resolve_fixtures_dir(profile: SiteProfile, override: Optional[str]) -> str:
    tests = (profile.meta or {}).get("tests") if isinstance(profile.meta, dict) else None
    if not isinstance(tests, dict):
        tests = {}
    fd = override or tests.get("fixtures_dir")
    if not isinstance(fd, str) or not fd.strip():
        fd = "tests/fixtures"
    p = Path(fd).expanduser()
    if not p.is_absolute():
        p = (Path.cwd() / p).resolve()
    p.mkdir(parents=True, exist_ok=True)
    return str(p)


def _load_state_arg(s: Optional[str]) -> dict[str, Any]:
    """Стартовое состояние для snapshot: JSON-строка или путь к JSON-файлу."""
    if not s:
        return {}
    ss = str(s).strip()
    if not ss:
        return {}
    p = Path(ss)
    if p.exists() and p.is_file():
        try:
            v = json.loads(p.read_text(encoding="utf-8"))
            return v if isinstance(v, dict) else {}
        except Exception:
            return {}
    try:
        v = json.loads(ss)
        return v if isinstance(v, dict) else {}
    except Exception:
        return {}


def _update_profile_tests_cases(profile_path: str, fixtures_dir: str, new_cases: list[dict[str, Any]]) -> dict[str, Any]:
    """Добавляет/обновляет _meta.tests.cases прямо в JSON профиля (defaults не трогаем)."""
    p = Path(profile_path)
    d = json.loads(p.read_text(encoding="utf-8"))
    if not isinstance(d, dict):
        raise CliError("Profile JSON must be an object")

    meta = d.get("_meta")
    if not isinstance(meta, dict):
        meta = {}
        d["_meta"] = meta

    tests = meta.get("tests")
    if not isinstance(tests, dict):
        tests = {}
        meta["tests"] = tests

    if not isinstance(tests.get("fixtures_dir"), str) or not str(tests.get("fixtures_dir")).strip():
        tests["fixtures_dir"] = fixtures_dir

    cases = tests.get("cases")
    if not isinstance(cases, list):
        cases = []

    # заменить кейсы с теми же name
    by_name: dict[str, dict[str, Any]] = {}
    for c in cases:
        if isinstance(c, dict) and isinstance(c.get("name"), str):
            by_name[str(c["name"])] = c
    for nc in new_cases:
        nm = str(nc.get("name"))
        by_name[nm] = nc

    replaced = {str(nc.get("name")) for nc in new_cases}
    keep: list[dict[str, Any]] = []
    for c in cases:
        if isinstance(c, dict) and str(c.get("name")) not in replaced:
            keep.append(c)

    out_cases = keep + [by_name[str(nc.get("name"))] for nc in new_cases]
    tests["cases"] = out_cases
    meta["tests"] = tests
    d["_meta"] = meta

    p.write_text(json.dumps(d, ensure_ascii=False, indent=2), encoding="utf-8")
    return {"written_profile": str(p), "cases_total": len(out_cases)}


def cmd_snapshot(args: argparse.Namespace) -> int:
    """Снять офлайн-фикстуры (HTTP ответы) “человеческими” именами.

    Зачем:
    - быстро собрать реальные ответы как файлы fixtures
    - потом гонять offline-test без сети и банов

    Режимы:
    - обычный: ходит в сеть (и может параллельно писать hash-cache, если задан --cache-dir)
    - --from-cache: НЕ ходит в сеть, а читает из кэша (replay)
    """
    if safe_read_json is None:
        raise CliError("Не найден resp_read.safe_read_json — snapshot не может определить JSON/HTML.")

    prof = load_profile(args.profile, defaults_path=args.defaults)

    # локально принудим replay, если попросили --from-cache
    local_args = argparse.Namespace(**vars(args))
    if bool(getattr(args, "from_cache", False)):
        local_args.replay = True
        # cache_dir должен быть задан либо в CLI, либо в _meta.http.cache.dir
        if not getattr(local_args, "cache_dir", None):
            http_cfg = prof.meta.get("http") if isinstance(prof.meta, dict) else {}
            meta_cache = http_cfg.get("cache") if isinstance(http_cfg, dict) else None
            if not (isinstance(meta_cache, dict) and isinstance(meta_cache.get("dir"), str) and meta_cache.get("dir")):
                raise CliError("snapshot --from-cache требует --cache-dir (или _meta.http.cache.dir в профиле)")

    eng = _build_engine(prof, local_args)

    fixtures_dir = _resolve_fixtures_dir(prof, getattr(args, "fixtures_dir", None))
    base_name = str(args.name).strip()
    if not base_name:
        raise CliError("--name is required")

    kind = str(getattr(args, "kind", "auto") or "auto").lower()
    batches = int(getattr(args, "batches", 1) or 1)
    if batches < 1:
        batches = 1

    state = _load_state_arg(getattr(args, "state", None))

    # Стартовые значения как в runtime
    url = str(state.get("url") or prof.url)
    page = int(state.get("page") if state.get("page") is not None else prof.pagination.start_from)
    offset = int(state.get("offset") if state.get("offset") is not None else 0)
    cursor = state.get("cursor") if isinstance(state.get("cursor"), str) else None
    next_url = state.get("next_url") if isinstance(state.get("next_url"), str) else None

    limit = prof.pagination.limit
    limit_param = prof.pagination.limit_param
    pag_kind = prof.pagination.kind

    saved: list[dict[str, Any]] = []
    case_snippets: list[dict[str, Any]] = []

    import datetime as _dt
    from urllib.parse import urljoin as _urljoin

    for i in range(batches):
        params = dict(prof.base_params or {})

        # pagination params — как в runtime
        if pag_kind == "page":
            params[prof.pagination.page_param] = page
            if limit_param:
                params[limit_param] = limit
        elif pag_kind == "offset":
            params[prof.pagination.offset_param] = offset
            if limit_param:
                params[limit_param] = limit
        elif pag_kind == "cursor_token":
            if cursor is not None:
                params[prof.pagination.cursor_param or "cursor"] = cursor
            if limit_param:
                params[limit_param] = limit
        elif pag_kind == "next_url":
            if next_url is not None:
                url = next_url

        expect = kind if kind in ("json", "html") else "auto"
        resp, err, elapsed_ms = eng.request(
            url,
            method=prof.method,
            params=params,
            headers=prof.headers,
            timeout=prof.timeout,
            expect=expect,
        )
        if resp is None:
            if bool(getattr(args, "from_cache", False)):
                raise CliError(
                    "snapshot --from-cache: не найден ответ в кэше для этого запроса.\n"
                    "Подсказка: сначала сделай обычный snapshot/run с --cache-dir, потом повтори --from-cache.\n"
                    f"Причина: {err or 'no_response'}"
                )
            raise CliError(f"snapshot request failed: {err or 'no_response'}")

        jr = safe_read_json(resp, force=False, detect_soft=True)

        # determine save kind
        if kind == "json":
            out_kind = "json"
        elif kind == "html":
            out_kind = "html"
        else:
            out_kind = "json" if jr.ok else "html"

        suffix = f"_{i+1}" if batches > 1 else ""
        fn = f"{base_name}{suffix}.{'json' if out_kind == 'json' else 'html'}"
        out_path = Path(fixtures_dir) / fn

        if out_kind == "json":
            out_path.write_text(json.dumps(jr.data, ensure_ascii=False, indent=2), encoding="utf-8")
        else:
            try:
                out_path.write_text(resp.text, encoding=resp.encoding or "utf-8", errors="replace")
            except Exception:
                out_path.write_bytes(resp.content)

        meta = {
            "saved_at": _dt.datetime.now(_dt.timezone.utc).isoformat(),
            "profile": prof.name,
            "file": fn,
            "kind": out_kind,
            "request": {"method": prof.method, "url": url, "params": params},
            "response": {
                "status": int(resp.status_code),
                "elapsed_ms": int(elapsed_ms),
                "content_type": str(resp.headers.get("Content-Type", "")),
            },
            "mode": "from_cache" if bool(getattr(args, "from_cache", False)) else "live",
        }
        meta_path = Path(fixtures_dir) / f"{base_name}{suffix}.meta.json"
        meta_path.write_text(json.dumps(meta, ensure_ascii=False, indent=2), encoding="utf-8")
        saved.append(meta)

        # case snippet
        schema = getattr(args, "schema", None) or "default"
        items_min = int(getattr(args, "items_min", 1) or 1)
        unique_ids_min = int(getattr(args, "unique_ids_min", 0) or 0)
        min_ratio = float(getattr(args, "min_nonempty_ratio", 0.5) or 0.5)
        cols_nonempty = getattr(args, "col_nonempty", None)
        if not isinstance(cols_nonempty, list):
            cols_nonempty = []
        case_snippets.append(
            {
                "name": Path(fn).stem,
                "file": fn,
                "kind": out_kind,
                "assert": {
                    "items_min": items_min,
                    "unique_ids_min": unique_ids_min,
                    "schema": schema,
                    "columns_nonempty": cols_nonempty,
                    "min_nonempty_ratio": min_ratio,
                },
            }
        )

        data_json: Optional[Any] = None
        if out_kind == "json" and jr.ok and jr.data is not None:
            data_json = jr.data
            items = extract_items_any(data_json, prof.extract, payload_kind="json") or []
        elif out_kind == "html":
            tp = read_text_safely(resp)
            if tp is None:
                break
            items = extract_items_any(tp.text, prof.extract, payload_kind="html") or []
        else:
            break
        if not items:
            break

        # update state — копия runtime логики
        if pag_kind == "page":
            page += 1
        elif pag_kind == "offset":
            step = prof.pagination.step or (limit if limit_param else len(items))
            offset += int(step)
        elif pag_kind == "cursor_token":
            if extract_cursor_token is None:
                break
            if data_json is None:
                break
            new_cursor = extract_cursor_token(data_json)
            if not new_cursor or new_cursor == cursor:
                break
            cursor = new_cursor
        elif pag_kind == "next_url":
            nxt = None
            if parse_link_next is not None:
                nxt = parse_link_next(dict(resp.headers))
            if not nxt and data_json is not None and extract_next_url_from_json is not None:
                nxt = extract_next_url_from_json(data_json)
            if not nxt:
                break
            next_url = _urljoin(prof.url, nxt)
            url = next_url
        else:
            break

        if limit_param and isinstance(limit, int) and limit > 0 and len(items) < limit:
            break

    report: dict[str, Any] = {
        "fixtures_dir": fixtures_dir,
        "saved": saved,
        "case_snippets": case_snippets,
    }
    if bool(getattr(args, "write_case", False)):
        report["write_case"] = _update_profile_tests_cases(args.profile, fixtures_dir, case_snippets)

    print(_pretty(report, getattr(args, "pretty", False)))
    return 0


def cmd_run(args: argparse.Namespace) -> int:
    prof = load_profile(args.profile, defaults_path=args.defaults)
    engine = _build_engine(prof, args)

    out_path = args.out
    Path(out_path).parent.mkdir(parents=True, exist_ok=True)

    n = 0
    with open(out_path, "w", encoding="utf-8") as f:
        for item in runtime_mod.paginate_items(prof, engine=engine):
            f.write(json.dumps(item, ensure_ascii=False) + "\n")
            n += 1
            if args.max_items and n >= args.max_items:
                break

    print(_pretty({"profile": args.profile, "out": out_path, "items_written": n}, args.pretty))
    return 0




def cmd_run_sqlite(args: argparse.Namespace) -> int:
    """
    run-sqlite — прогон одного профиля, но запись в SQLite (.db) сразу в две таблицы:

    1) items_raw    — сырьё: сохраняем КАЖДУЮ встречу карточки (повторы НЕ теряем)
    2) items_unique — витрина: уникальные карточки + seen_count + last_seen_at

    Дополнительно:
    - run_state: сохранение state для resume
    - blocked_events: очередь "нужен человек" при антиботе/капче
    """
    prof = load_profile(args.profile, defaults_path=args.defaults)
    engine = _build_engine(prof, args)

    db_path = args.db
    Path(db_path).parent.mkdir(parents=True, exist_ok=True)

    resume = bool(getattr(args, "resume", False))
    run_id = (args.run_id or "").strip()

    items_seen = 0
    raw_inserted = 0
    unique_inserted = 0
    unique_updated = 0

    with DualSqliteStore(
        db_path,
        extract_spec=prof.extract,
        raw_table=args.raw_table,
        unique_table=args.unique_table,
    ) as db:
        if resume and not run_id:
            run_id = db.latest_run_id(profile=prof.name) or ""
        if not run_id:
            run_id = DualSqliteStore.new_run_id()

        start_state = db.load_state(profile=prof.name, run_id=run_id) if resume else None

        seq_counter = 0
        last_blocked_bid: int | None = None

        def checkpoint_cb(st: dict[str, Any]) -> None:
            """runtime вызывает это после завершения batch"""
            try:
                bi = int(st.get("batch_idx") or 0)
            except Exception:
                bi = 0
            db.save_state(
                profile=prof.name,
                run_id=run_id,
                state=st,
                batch_idx=bi,
                last_seq=int(seq_counter),
                items_seen=int(items_seen),
            )

        def on_block_cb(ev: dict[str, Any]) -> None:
            """runtime вызывает это при блокировке (anti-bot), чтобы сохранить state и завести blocked_event."""
            nonlocal last_blocked_bid
            st = ev.get("pagination_state") if isinstance(ev.get("pagination_state"), dict) else None
            try:
                bi = int(ev.get("batch_idx") or 0)
            except Exception:
                bi = 0

            # Save the blocked state so resume retries the same request
            if st is not None:
                try:
                    db.save_state(
                        profile=prof.name,
                        run_id=run_id,
                        state=st,
                        batch_idx=bi,
                        last_seq=int(seq_counter),
                        items_seen=int(items_seen),
                    )
                except Exception:
                    pass

            try:
                last_blocked_bid = db.add_blocked_event(
                    profile=prof.name,
                    profile_path=str(args.profile),
                    run_id=run_id,
                    batch_idx=bi,
                    url=str(ev.get("request_url") or prof.url),
                    method=str(ev.get("request_method") or prof.method),
                    params=ev.get("request_params") if isinstance(ev.get("request_params"), dict) else None,
                    pagination_state=st,
                    status_code=int(ev.get("status_code") or 0) if ev.get("status_code") is not None else None,
                    block_hint=str(ev.get("block_hint") or "") or None,
                    error=str(ev.get("error") or "") or None,
                    resp_url_final=str(ev.get("resp_url_final") or "") or None,
                    resp_headers=ev.get("resp_headers") if isinstance(ev.get("resp_headers"), dict) else None,
                    resp_snippet=str(ev.get("resp_snippet") or "") or None,
                )
            except Exception:
                last_blocked_bid = None

        it = runtime_mod.paginate_items(
            prof,
            engine=engine,
            state=start_state,
            on_checkpoint=checkpoint_cb,
            on_block=on_block_cb,
        )
        for item in it:
            seq_counter += 1
            inserted, _key = db.put_both(item, run_id=run_id, seq=seq_counter)
            items_seen += 1
            raw_inserted += 1
            if inserted:
                unique_inserted += 1
            else:
                unique_updated += 1

            if args.max_items and items_seen >= args.max_items:
                break

        raw_total = db.count_raw()
        unique_total = db.count_unique()

    print(_pretty({
        "profile": args.profile,
        "db": db_path,
        "run_id": run_id,
        "resumed": bool(start_state is not None),
        "raw_table": args.raw_table,
        "unique_table": args.unique_table,
        "items_seen": items_seen,
        "raw_inserted": raw_inserted,
        "unique_inserted": unique_inserted,
        "unique_updated": unique_updated,
        "raw_total_in_db": raw_total,
        "unique_total_in_db": unique_total,
        "blocked_bid": last_blocked_bid,
        "blocked": bool(last_blocked_bid is not None),
    }, args.pretty))
    return 0

def cmd_export(args: argparse.Namespace) -> int:
    """
    export — экспорт JSONL или SQLite в CSV.

    Режимы:
    1) Старый: --fields a,b.c  (колонки = список полей)
    2) Контрактный: --profile профиля + (опц.) --schema default|analytics
       берём columns_map/columns из profile.meta.export.schemas[schema]
    3) Авто: без fields и без profile — пробуем вывести поля из первых N строк.

    Примеры:
      JSONL -> CSV (по схеме default из defaults):
        python tool.py export --in out/items.jsonl --out out/items.csv --profile profiles/site.json --defaults _defaults.merged.clean.json

      JSONL -> CSV (analytics схема):
        python tool.py export --in out/items.jsonl --out out/items_analytics.csv --profile profiles/site.json --schema analytics --defaults _defaults.merged.clean.json

      SQLite -> CSV:
        python tool.py export --in out/site.db --out out/site.csv --kind sqlite --table items_unique --profile profiles/site.json --schema default
    """
    in_path = args.in_path
    out_path = args.out_path

    # legacy fields
    fields = None
    if args.fields:
        fields = [x.strip() for x in args.fields.split(",") if x.strip()]

    limit = args.limit if (args.limit and args.limit > 0) else None

    # kind autodetect
    kind = args.kind
    if kind == "auto":
        low = in_path.lower()
        if low.endswith((".db", ".sqlite", ".sqlite3")):
            kind = "sqlite"
        else:
            kind = "jsonl"

    # contract columns/ctx from profile
    columns = None
    ctx: dict[str, Any] = {}
    extract_spec = None

    if args.profile:
        prof = load_profile(args.profile, defaults_path=args.defaults)
        extract_spec = prof.extract

        meta = prof.meta if isinstance(prof.meta, dict) else {}
        export_meta = meta.get("export") if isinstance(meta.get("export"), dict) else {}
        schemas = export_meta.get("schemas") if isinstance(export_meta.get("schemas"), dict) else {}

        schema_name = args.schema or export_meta.get("default_schema") or "default"
        schema_obj = schemas.get(schema_name) if isinstance(schemas.get(schema_name), dict) else {}

        if fields is None:
            # предпочитаем columns_map (dict) — он лучше для deep_merge; export_csv принимает dict как columns_map
            columns = schema_obj.get("columns_map") or schema_obj.get("columns")

        # ctx_defaults + служебные константы
        cd = meta.get("ctx_defaults") if isinstance(meta.get("ctx_defaults"), dict) else {}
        ctx.update(dict(cd))

        ctx.setdefault("source_profile", prof.name or Path(args.profile).stem)
        if args.run_id:
            ctx["run_id"] = args.run_id
        if args.batch_id:
            ctx["batch_id"] = args.batch_id
        # export time as fallback
        ctx.setdefault("fetched_at", __import__("datetime").datetime.utcnow().isoformat(timespec="seconds") + "Z")

    # дополнительные ctx key=value
    if args.ctx:
        for raw in args.ctx:
            if not raw:
                continue
            if "=" not in raw:
                # если без "=", просто кладём как флаг True
                ctx[raw.strip()] = True
                continue
            k, v = raw.split("=", 1)
            ctx[k.strip()] = v.strip()

    if kind == "jsonl":
        rep = export_mod.jsonl_to_csv(
            in_path,
            out_path,
            fields=fields,
            columns=columns,
            ctx=(ctx or None),
            extract_spec=extract_spec,
            probe_lines=args.probe,
            limit=limit,
        )
    else:
        rep = export_mod.sqlite_to_csv(
            in_path,
            out_path,
            table=args.table,
            fields=fields,
            columns=columns,
            ctx=(ctx or None),
            extract_spec=extract_spec,
            probe_rows=args.probe,
            limit=limit,
        )

    print(_pretty(rep, args.pretty))
    return 0

def cmd_farm(args: argparse.Namespace) -> int:
    profiles = _iter_profiles(args.profiles_dir, args.recursive)
    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    results = []
    for p in profiles:
        name = Path(p).stem
        out_path = str(out_dir / f"{name}.jsonl")
        try:
            ns = argparse.Namespace(profile=p, defaults=args.defaults, out=out_path, max_items=args.max_items, pretty=False)
            cmd_run(ns)
            results.append({"profile": p, "ok": True, "out": out_path})
        except Exception as e:
            results.append({"profile": p, "ok": False, "error": str(e)})

    print(_pretty({"count": len(results), "results": results}, args.pretty))
    return 0


def cmd_pipeline(args: argparse.Namespace) -> int:
    """
    Конвейер в 2 прохода под твою логику папок:

    PASS 1 (draft -> active|fixed):
      - пробуем "как есть" (без auto-fix items/id):
        1) onboard (infer pagination + limit_param)
        2) triage (+ optional smoke)
      - если OK -> в active
      - иначе -> в fixed

    PASS 2 (fixed -> active|errors):
      - пробуем авто-починку (auto-fix items_path/id_path + infer + limit-probe):
        1) diagnose-like patch: items_path/id_path (по данным первой страницы)
        2) применяем patch в памяти
        3) infer pagination + probe limit_param
        4) сохраняем "применённый" профиль
        5) triage (+ smoke)
      - если OK -> в active
      - иначе -> в errors

    Примечание:
      - В режиме --move исходники из draft/fixed удаляются после успешной раскладки.
      - Если --move не указан, файлы копируются, исходники остаются.
    """
    draft = Path(args.draft)
    fixed = Path(args.fixed)
    active = Path(args.active)
    errors = Path(args.errors)
    reports_dir = Path(args.reports_dir) if args.reports_dir else None

    for d in (fixed, active, errors):
        d.mkdir(parents=True, exist_ok=True)
    if reports_dir:
        reports_dir.mkdir(parents=True, exist_ok=True)

    def _write_report(name: str, payload: dict[str, Any]) -> None:
        if not reports_dir:
            return
        out = reports_dir / name
        out.parent.mkdir(parents=True, exist_ok=True)
        out.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")

    def move_or_copy(src: Path, dst: Path) -> None:
        dst.parent.mkdir(parents=True, exist_ok=True)
        if args.move:
            shutil.move(str(src), str(dst))
        else:
            shutil.copy2(str(src), str(dst))

    def _deep_merge(a: dict[str, Any], b: dict[str, Any]) -> dict[str, Any]:
        out = dict(a)
        for k, v in b.items():
            if k in out and isinstance(out[k], dict) and isinstance(v, dict):
                out[k] = _deep_merge(out[k], v)
            else:
                out[k] = v
        return out

    pass1_results: list[dict[str, Any]] = []
    pass2_results: list[dict[str, Any]] = []

    # ----------------------------
    # PASS 1: draft -> active|fixed
    # ----------------------------
    draft_profiles = _iter_profiles(str(draft), args.recursive)
    for src_path in draft_profiles:
        src = Path(src_path)
        report: dict[str, Any] = {"in": src_path, "pass": 1}
        try:
            # 1) onboard -> tmp
            tmp = fixed / f"{src.stem}.pass1.tmp.json"
            ns_on = argparse.Namespace(
                in_path=src_path,
                defaults=args.defaults,
                out_path=str(tmp),
                pretty=True,
                print_report=False,
            )
            cmd_onboard(ns_on)

            # 2) triage onboarded
            prof_tmp = load_profile(str(tmp), defaults_path=None)
            eng = _build_engine(prof_tmp, args)
            tri = _triage(
                prof_tmp,
                engine=eng,
                smoke=(0 if args.smoke0 else args.smoke),
                stagnation_window=args.stagnation_window,
            )
            report["triage"] = tri

            if tri["label"] == "OK":
                out_active = active / src.name
                move_or_copy(tmp, out_active)
                report["stage"] = "ACTIVE"
                report["out"] = str(out_active)
            else:
                out_fixed = fixed / src.name
                move_or_copy(tmp, out_fixed)
                report["stage"] = "FIXED"
                report["out"] = str(out_fixed)

            # cleanup tmp if it still exists (copy mode)
            tmp.unlink(missing_ok=True)

            # в режиме move — удаляем исходник из draft
            if args.move:
                src.unlink(missing_ok=True)

        except Exception as e:
            # PASS1 не удался (например, onboard упал) -> всё равно кладём в fixed (как ты хотел),
            # чтобы PASS2 попробовал авто-починку.
            report["error"] = str(e)
            try:
                out_fixed = fixed / src.name
                move_or_copy(src, out_fixed)
                report["stage"] = "FIXED"
                report["out"] = str(out_fixed)
                if args.move:
                    src.unlink(missing_ok=True)
            except Exception as e2:
                out_err = errors / src.name
                try:
                    move_or_copy(src, out_err)
                except Exception:
                    pass
                report["stage"] = "ERRORS"
                report["out"] = str(out_err)
                report["error2"] = str(e2)

        pass1_results.append(report)
        _write_report(f"{src.stem}.pass1.json", report)

    # ----------------------------
    # PASS 2: fixed -> active|errors
    # ----------------------------
    fixed_profiles = _iter_profiles(str(fixed), args.recursive)
    for src_path in fixed_profiles:
        src = Path(src_path)
        report: dict[str, Any] = {"in": src_path, "pass": 2}
        try:
            prof = load_profile(src_path, defaults_path=args.defaults)
            eng = _build_engine(prof, args)

            # 1) базовый запрос (чтобы подсказать items/id)
            resp, data, err = eng.safe_get_json(
                prof.url,
                method=prof.method,
                params=_merge_params(prof),
                headers=prof.headers,
                timeout=prof.timeout,
                force_json=False,
                detect_soft=True,
            )
            report["base"] = {
                "status": getattr(resp, "status_code", None) if resp is not None else None,
                "content_type": (resp.headers.get("Content-Type") if resp is not None else None),
                "err": err,
            }

            if err or data is None:
                # во втором проходе — это уже errors
                out_err = errors / src.name
                move_or_copy(src, out_err)
                report["stage"] = "ERRORS"
                report["out"] = str(out_err)
                pass2_results.append(report)
                _write_report(f"{src.stem}.pass2.json", report)
                if args.move:
                    src.unlink(missing_ok=True)
                continue

            # 2) auto-fix items_path / id_path по первой странице
            patch: dict[str, Any] = {}
            items = extract_items(data, prof.extract)
            if not items:
                guess = _guess_items_path(data)
                if guess:
                    patch.setdefault("extract", {})["items_path"] = guess
            else:
                ids = ids_of(items, prof.extract)
                if len(ids) == 0:
                    guess_id = _guess_id_path(items)
                    if guess_id:
                        patch.setdefault("extract", {})["id_path"] = guess_id

            # применяем extract-патч в память, чтобы infer работал по правильным путям
            if "extract" in patch:
                d0 = prof.to_dict()
                prof = SiteProfile.from_dict(_deep_merge(d0, patch))

            report["patch_extract"] = patch or None

            # 3) infer pagination
            if not args.no_infer:
                pag, inf_rep = infer_mod.infer_pagination(prof, engine=eng)
                prof.pagination = pag
                report["infer_report"] = inf_rep
                report["pagination"] = asdict(pag)

            # 4) probe limit_param
            if not args.no_limit_probe:
                limit_param, lim_rep = onboard_mod.find_limit_param(prof, engine=eng)
                report["limit_report"] = lim_rep
                report["limit_param_found"] = limit_param
                if limit_param:
                    prof.pagination.limit_param = limit_param

            # 5) сохранить применённый профиль во временный файл
            tmp = fixed / f"{src.stem}.pass2.tmp.json"
            save_profile(prof, str(tmp), pretty=True)

            # 6) финальный triage
            tri = _triage(
                prof,
                engine=eng,
                smoke=args.smoke,
                stagnation_window=args.stagnation_window,
            )
            report["triage"] = tri

            if tri["label"] == "OK":
                out_active = active / src.name
                move_or_copy(tmp, out_active)
                report["stage"] = "ACTIVE"
                report["out"] = str(out_active)
                if args.move:
                    src.unlink(missing_ok=True)
            else:
                out_err = errors / src.name
                move_or_copy(tmp, out_err)
                report["stage"] = "ERRORS"
                report["out"] = str(out_err)
                if args.move:
                    src.unlink(missing_ok=True)

            tmp.unlink(missing_ok=True)

        except Exception as e:
            report["error"] = str(e)
            out_err = errors / src.name
            try:
                move_or_copy(src, out_err)
            except Exception:
                pass
            report["stage"] = "ERRORS"
            report["out"] = str(out_err)
            if args.move:
                src.unlink(missing_ok=True)

        pass2_results.append(report)
        _write_report(f"{src.stem}.pass2.json", report)

    out = {"pass1": pass1_results, "pass2": pass2_results}
    print(_pretty(out, args.pretty))
    return 0


def _safe_table_suffix(name: str, *, max_len: int = 42) -> str:
    s = (name or "").strip().lower()
    s = re.sub(r"[^a-z0-9]+", "_", s)
    s = re.sub(r"_+", "_", s).strip("_")
    return (s or "profile")[:max_len]


def cmd_farm_sqlite(args: argparse.Namespace) -> int:
    profiles = _iter_profiles(args.profiles_dir, args.recursive)
    Path(os.path.dirname(args.db) or ".").mkdir(parents=True, exist_ok=True)

    results: list[dict[str, Any]] = []
    for p in profiles:
        stem = Path(p).stem
        try:
            prof = load_profile(p, defaults_path=args.defaults)
            engine = _build_engine(prof, args)

            key = prof.name or stem
            raw_table = f"{args.raw_prefix}{_safe_table_suffix(key)}"
            unique_table = f"{args.unique_prefix}{_safe_table_suffix(key)}"

            resume = bool(getattr(args, "resume", False))
            run_id = ""

            items_seen = 0
            raw_inserted = 0
            unique_inserted = 0
            unique_updated = 0
            last_blocked_bid: int | None = None

            with DualSqliteStore(
                args.db,
                extract_spec=prof.extract,
                raw_table=raw_table,
                unique_table=unique_table,
            ) as db:
                if resume:
                    run_id = db.latest_run_id(profile=prof.name) or ""
                if not run_id:
                    run_id = DualSqliteStore.new_run_id()
                start_state = db.load_state(profile=prof.name, run_id=run_id) if resume else None

                seq_counter = 0

                def checkpoint_cb(st: dict[str, Any]) -> None:
                    try:
                        bi = int(st.get("batch_idx") or 0)
                    except Exception:
                        bi = 0
                    db.save_state(
                        profile=prof.name,
                        run_id=run_id,
                        state=st,
                        batch_idx=bi,
                        last_seq=int(seq_counter),
                        items_seen=int(items_seen),
                    )

                def on_block_cb(ev: dict[str, Any]) -> None:
                    nonlocal last_blocked_bid, seq_counter, items_seen
                    st = ev.get("pagination_state") if isinstance(ev.get("pagination_state"), dict) else None
                    try:
                        bi = int(ev.get("batch_idx") or 0)
                    except Exception:
                        bi = 0
                    if st is not None:
                        try:
                            db.save_state(
                                profile=prof.name,
                                run_id=run_id,
                                state=st,
                                batch_idx=bi,
                                last_seq=int(seq_counter),
                                items_seen=int(items_seen),
                            )
                        except Exception:
                            pass

                    last_blocked_bid = db.add_blocked_event(
                        profile=prof.name,
                        profile_path=str(p),
                        run_id=run_id,
                        batch_idx=bi,
                        url=str(ev.get("request_url") or prof.url),
                        method=str(ev.get("request_method") or prof.method),
                        params=ev.get("request_params") if isinstance(ev.get("request_params"), dict) else None,
                        pagination_state=st,
                        status_code=int(ev.get("status_code") or 0) if ev.get("status_code") is not None else None,
                        block_hint=str(ev.get("block_hint") or "") or None,
                        error=str(ev.get("error") or "") or None,
                        resp_url_final=str(ev.get("resp_url_final") or "") or None,
                        resp_headers=ev.get("resp_headers") if isinstance(ev.get("resp_headers"), dict) else None,
                        resp_snippet=str(ev.get("resp_snippet") or "") or None,
                    )

                it = runtime_mod.paginate_items(prof, engine=engine, state=start_state, on_checkpoint=checkpoint_cb, on_block=on_block_cb)
                for item in it:
                    seq_counter += 1
                    inserted, _ = db.put_both(item, run_id=run_id, seq=seq_counter)
                    items_seen += 1
                    raw_inserted += 1
                    if inserted:
                        unique_inserted += 1
                    else:
                        unique_updated += 1
                    if args.max_items and items_seen >= args.max_items:
                        break

                raw_total = db.count_raw()
                unique_total = db.count_unique()

            results.append({
                "profile": p,
                "profile_name": prof.name,
                "ok": True,
                "blocked": bool(last_blocked_bid is not None),
                "blocked_bid": last_blocked_bid,
                "db": args.db,
                "run_id": run_id,
                "raw_table": raw_table,
                "unique_table": unique_table,
                "items_seen": items_seen,
                "unique_inserted": unique_inserted,
                "unique_updated": unique_updated,
                "raw_total_in_table": raw_total,
                "unique_total_in_table": unique_total,
            })
        except Exception as e:
            results.append({"profile": p, "ok": False, "error": str(e)})

    summary = {
        "count": len(results),
        "ok": sum(1 for r in results if r.get("ok")),
        "blocked": sum(1 for r in results if r.get("blocked")),
        "failed": sum(1 for r in results if not r.get("ok")),
        "db": args.db,
        "results": results,
    }
    print(_pretty(summary, args.pretty))
    return 0


def _db_stub_extract_spec() -> Any:
    # blocked commands don't parse items, but DualSqliteStore requires extract_spec
    return {"mode": "json", "items_path": "", "id_path": None}


def cmd_blocked_list(args: argparse.Namespace) -> int:
    prof = args.profile_name
    with DualSqliteStore(args.db, extract_spec=_db_stub_extract_spec(), raw_table="items_raw", unique_table="items_unique") as db:
        rows = db.list_blocked_events(profile=prof, run_id=args.run_id, only_open=(not args.all), limit=args.limit, offset=args.offset)
    print(_pretty({"db": args.db, "count": len(rows), "items": rows}, args.pretty))
    return 0


def cmd_blocked_export(args: argparse.Namespace) -> int:
    with DualSqliteStore(args.db, extract_spec=_db_stub_extract_spec(), raw_table="items_raw", unique_table="items_unique") as db:
        rows = db.list_blocked_events(profile=args.profile_name, run_id=args.run_id, only_open=(not args.all), limit=args.limit, offset=args.offset)

    out_path = args.out
    Path(os.path.dirname(out_path) or ".").mkdir(parents=True, exist_ok=True)

    if args.format == "jsonl":
        with open(out_path, "w", encoding="utf-8") as f:
            for r in rows:
                f.write(json.dumps(r, ensure_ascii=False) + "\n")
    else:
        import csv
        cols = ["bid","created_at","resolved_at","profile","profile_path","run_id","batch_idx","url","method","status_code","block_hint","error","resp_url_final","resp_snippet"]
        with open(out_path, "w", newline="", encoding="utf-8") as f:
            w = csv.DictWriter(f, fieldnames=cols)
            w.writeheader()
            for r in rows:
                w.writerow({c: r.get(c) for c in cols})

    print(_pretty({"db": args.db, "out": out_path, "format": args.format, "rows": len(rows)}, args.pretty))
    return 0


def cmd_blocked_resolve(args: argparse.Namespace) -> int:
    with DualSqliteStore(args.db, extract_spec=_db_stub_extract_spec(), raw_table="items_raw", unique_table="items_unique") as db:
        db.mark_blocked_resolved(bid=int(args.id), note=str(args.note or ""))
    print(_pretty({"db": args.db, "id": int(args.id), "resolved": True}, args.pretty))
    return 0


def cmd_blocked_resume(args: argparse.Namespace) -> int:
    with DualSqliteStore(args.db, extract_spec=_db_stub_extract_spec(), raw_table="items_raw", unique_table="items_unique") as db:
        ev = None
        if args.id:
            ev = db.get_blocked_event(bid=int(args.id))
        elif args.profile_name:
            ev = db.latest_open_blocked(profile=str(args.profile_name))
        if not ev:
            raise CliError("No blocked event found (use --profile-name or --id)", exit_code=2)
        if not ev.get("run_id"):
            raise CliError("Blocked event has no run_id; cannot resume", exit_code=2)
        run_id = str(ev["run_id"])

    ns = argparse.Namespace(
        profile=args.profile,
        defaults=args.defaults,
        db=args.db,
        raw_table=args.raw_table,
        unique_table=args.unique_table,
        run_id=run_id,
        max_items=args.max_items,
        resume=True,
        pretty=args.pretty,
        secrets=getattr(args, "secrets", None),
        diag_http=getattr(args, "diag_http", False),
        cache_dir=getattr(args, "cache_dir", None),
        replay=getattr(args, "replay", False),
    )
    return cmd_run_sqlite(ns)


def cmd_secrets_set(args: argparse.Namespace) -> int:
    path = args.secrets
    if not path:
        raise CliError("--secrets is required for secrets-set", exit_code=2)

    obj = {}
    if os.path.exists(path):
        try:
            obj = _read_json(path)
        except Exception:
            obj = {}
    if not isinstance(obj, dict):
        obj = {}

    ref = str(args.ref)
    typ = str(args.type)

    if typ == "cookies_file":
        entry = {"type": "cookies_file", "path": str(args.cookies_file)}
    elif typ == "bearer":
        entry = {"type": "bearer", "token": str(args.token)}
    elif typ == "api_key_header":
        entry = {"type": "api_key_header", "header": str(args.header), "token": str(args.token)}
    elif typ == "basic":
        entry = {"type": "basic", "username": str(args.username), "password": str(args.password)}
    elif typ == "headers":
        entry = {"type": "headers", "headers": json.loads(args.headers_json)}
    elif typ == "api_key_query":
        entry = {"type": "api_key_query", "param": str(args.param), "token": str(args.token)}
    else:
        raise CliError(f"Unsupported secret type: {typ}", exit_code=2)

    obj[ref] = entry
    _write_json(path, obj, pretty=True)
    print(_pretty({"secrets": path, "ref": ref, "saved": True}, args.pretty))
    return 0



def cmd_farm_resume_open(args: argparse.Namespace) -> int:
    """
    Resume all profiles that currently have open blocked_events in the SQLite DB.

    Strategy:
    - Query distinct profiles with resolved_at IS NULL
    - For each profile, take the latest open blocked event (created_at DESC)
    - Resume using run-sqlite with that run_id and saved state
    - Optionally mark blocked event resolved after a successful resume run
    """
    Path(os.path.dirname(args.db) or ".").mkdir(parents=True, exist_ok=True)

    with DualSqliteStore(args.db, extract_spec=_db_stub_extract_spec(), raw_table="items_raw", unique_table="items_unique") as db:
        rows = db.conn.execute(
            "SELECT profile, MAX(created_at) as mc FROM blocked_events WHERE resolved_at IS NULL GROUP BY profile ORDER BY mc DESC"
        ).fetchall()
        profiles = [r[0] for r in rows if r and r[0]]

    if args.max_profiles and args.max_profiles > 0:
        profiles = profiles[: int(args.max_profiles)]

    results: list[dict[str, Any]] = []
    for prof_name in profiles:
        try:
            with DualSqliteStore(args.db, extract_spec=_db_stub_extract_spec(), raw_table="items_raw", unique_table="items_unique") as db:
                ev = db.latest_open_blocked(profile=str(prof_name))
            if not ev:
                results.append({"profile_name": prof_name, "ok": False, "error": "no_open_event"})
                continue

            profile_path = ev.get("profile_path") or args.profile_path
            if not profile_path:
                results.append({"profile_name": prof_name, "ok": False, "error": "missing_profile_path"})
                continue

            run_id = str(ev.get("run_id") or "")
            if not run_id:
                results.append({"profile_name": prof_name, "ok": False, "error": "missing_run_id"})
                continue

            if args.dry_run:
                results.append({
                    "profile_name": prof_name,
                    "ok": True,
                    "dry_run": True,
                    "profile_path": profile_path,
                    "run_id": run_id,
                    "bid": ev.get("bid"),
                })
                continue

            ns = argparse.Namespace(
                profile=profile_path,
                defaults=args.defaults,
                db=args.db,
                raw_table=args.raw_table,
                unique_table=args.unique_table,
                run_id=run_id,
                max_items=args.max_items,
                resume=True,
                pretty=args.pretty,
                secrets=getattr(args, "secrets", None),
                diag_http=getattr(args, "diag_http", False),
                cache_dir=getattr(args, "cache_dir", None),
                replay=getattr(args, "replay", False),
            )
            rc = cmd_run_sqlite(ns)

            # If run succeeded and user requested auto-resolve, mark event resolved.
            if rc == 0 and args.auto_resolve:
                try:
                    with DualSqliteStore(args.db, extract_spec=_db_stub_extract_spec(), raw_table="items_raw", unique_table="items_unique") as db:
                        db.mark_blocked_resolved(bid=int(ev["bid"]), note=str(args.resolve_note or "auto_resolve"))
                except Exception:
                    pass

            results.append({
                "profile_name": prof_name,
                "ok": (rc == 0),
                "rc": rc,
                "profile_path": profile_path,
                "run_id": run_id,
                "bid": ev.get("bid"),
                "auto_resolve": bool(args.auto_resolve),
            })
        except Exception as e:
            results.append({"profile_name": prof_name, "ok": False, "error": str(e)})

    summary = {
        "db": args.db,
        "count": len(profiles),
        "attempted": len(results),
        "ok": sum(1 for r in results if r.get("ok")),
        "failed": sum(1 for r in results if not r.get("ok")),
        "dry_run": bool(args.dry_run),
        "auto_resolve": bool(args.auto_resolve),
        "results": results,
    }
    print(_pretty(summary, args.pretty))
    return 0


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(prog="tool.py")
    p.add_argument("--pretty", action="store_true", help="pretty JSON output")
    p.add_argument("--defaults", default=None, help="path to _defaults.json")
    p.add_argument("--secrets", default=None, help="path to secrets.json (overrides ENV PARSER_SECRETS_PATH)")
    p.add_argument("--diag-http", action="store_true", help="print short HTTP diagnostics on errors")
    p.add_argument("--cache-dir", default=None, help="cache dir for HTTP responses (optional)")
    p.add_argument("--replay", action="store_true", help="replay from cache only (no network)")

    sub = p.add_subparsers(dest="cmd", required=True)

    # demo
    dm = sub.add_parser("demo", help="1-command portfolio demo: offline-test + export CSV from bundled fixtures")
    dm.add_argument("--name", default="all", help="jsonplaceholder | pokeapi | all")
    dm.add_argument("--out-dir", default="out", help="where to write demo artifacts")
    dm.add_argument("--schema", default=None, help="export schema (default: profile default_schema)")
    dm.add_argument("--all-schemas", action="store_true", help="export all schemas defined in profile")
    dm.add_argument("--items", type=int, default=0, help="limit items written to JSONL/CSV (0=no limit)")
    dm.add_argument("--max-items", type=int, default=50, help="max items checked per case in offline-test")
    dm.add_argument("--fail-fast", action="store_true", help="stop after first failing demo")
    dm.set_defaults(fn=cmd_demo)

    # lint
    l = sub.add_parser("lint", help="static profile validation (no network)")
    l.add_argument("--profile", required=True)
    l.add_argument("--json", action="store_true")
    l.set_defaults(fn=cmd_lint)

    # offline-test
    ot = sub.add_parser("offline-test", help="offline fixtures tests for a profile (no network)")
    ot.add_argument("--profile", required=True)
    ot.add_argument("--fixtures-dir", default=None, help="fixtures dir (overrides _meta.tests.fixtures_dir)")
    ot.add_argument("--case", default=None, help="run only one case by name")
    ot.add_argument("--schema", default=None, help="override export schema for cases")
    ot.add_argument("--max-items", type=int, default=50, help="how many extracted items to validate per case")
    ot.add_argument("--json", action="store_true")
    ot.set_defaults(fn=cmd_offline_test)

    # snapshot
    s = sub.add_parser("snapshot", help="save HTTP responses as offline fixtures (human names)")
    s.add_argument("--profile", required=True)
    s.add_argument("--name", required=True, help="case name (filename without extension)")
    s.add_argument("--fixtures-dir", default=None, help="override fixtures dir (default: _meta.tests.fixtures_dir or tests/fixtures)")
    s.add_argument("--kind", default="auto", choices=["auto", "json", "html"], help="force save as json/html or auto-detect")
    s.add_argument("--batches", type=int, default=1, help="how many pages/batches to snapshot (JSON pagination only)")
    s.add_argument("--state", default=None, help="start state as JSON string or path to JSON file (optional)")
    s.add_argument("--from-cache", action="store_true", help="do not use network; read responses from --cache-dir (replay)")
    s.add_argument("--write-case", action="store_true", help="write case(s) into profile _meta.tests.cases")
    s.add_argument("--schema", default=None, help="export schema name for offline-test cases")
    s.add_argument("--items-min", type=int, default=1)
    s.add_argument("--unique-ids-min", type=int, default=0)
    s.add_argument("--col-nonempty", action="append", default=[], help="repeatable: column that must be often non-empty")
    s.add_argument("--min-nonempty-ratio", type=float, default=0.5)
    s.set_defaults(fn=cmd_snapshot)

    # triage
    t = sub.add_parser("triage", help="fast triage (single profile or dir)")
    t.add_argument("--profile", default=None)
    t.add_argument("--profiles-dir", default=None)
    t.add_argument("--recursive", action="store_true")
    t.add_argument("--smoke", type=int, default=5)
    t.add_argument("--stagnation-window", type=int, default=10)
    t.add_argument("--only", default=None, help="comma list of labels to show")
    t.add_argument("--json", action="store_true")
    t.add_argument("--summary", action="store_true")
    t.set_defaults(fn=cmd_triage)

    # diagnose
    d = sub.add_parser("diagnose", help="full diagnose + hints + optional patch apply")
    d.add_argument("--profile", required=True)
    d.add_argument("--infer", action="store_true")
    d.add_argument("--limit-probe", action="store_true")
    d.add_argument("--apply", action="store_true")
    d.add_argument("--apply-out", default=None)
    d.set_defaults(fn=cmd_diagnose)

    # onboard
    o = sub.add_parser("onboard", help="infer pagination + find limit_param and save profile")
    o.add_argument("--in", dest="in_path", required=True)
    o.add_argument("--out", dest="out_path", required=True)
    o.add_argument("--print-report", action="store_true")
    o.set_defaults(fn=cmd_onboard)

    # run
    r = sub.add_parser("run", help="run one profile and write JSONL")
    r.add_argument("--profile", required=True)
    r.add_argument("--out", required=True)
    r.add_argument("--max-items", type=int, default=0)
    r.set_defaults(fn=cmd_run)

    # run-sqlite
    rs = sub.add_parser("run-sqlite", help="run one profile and write SQLite (.db) with raw+unique tables")
    rs.add_argument("--profile", required=True)
    rs.add_argument("--db", required=True)
    rs.add_argument("--raw-table", default="items_raw")
    rs.add_argument("--unique-table", default="items_unique")
    rs.add_argument("--run-id", default=None, help="optional run id (if not set => UUID)")
    rs.add_argument("--max-items", type=int, default=0)
    rs.add_argument("--resume", action="store_true", help="resume pagination from last saved run_state")
    rs.set_defaults(fn=cmd_run_sqlite)

    # export
    e = sub.add_parser("export", help="export JSONL/SQLite to CSV")
    e.add_argument("--in", dest="in_path", required=True)
    e.add_argument("--out", dest="out_path", required=True)
    e.add_argument("--kind", choices=["auto", "jsonl", "sqlite"], default="auto")
    e.add_argument("--table", default="items_unique", help="SQLite table (items_unique or items_raw)")
    e.add_argument("--fields", default=None, help="comma-separated fields, dot-path allowed")
    e.add_argument("--probe", type=int, default=200, help="how many rows/lines to inspect for auto fields")
    e.add_argument("--limit", type=int, default=0, help="0 = no limit")
    e.add_argument("--profile", default=None, help="profile json to load export schema/ctx_defaults")
    e.add_argument("--schema", default=None, help="schema name from _meta.export.schemas (default or analytics)")
    e.add_argument("--ctx", action="append", default=None, help="extra ctx key=value (repeatable)")
    e.add_argument("--run-id", dest="run_id", default=None)
    e.add_argument("--batch-id", dest="batch_id", default=None)
    e.set_defaults(fn=cmd_export)

    # farm
    f = sub.add_parser("farm", help="run all profiles in dir (JSONL per profile)")
    f.add_argument("--profiles-dir", required=True)
    f.add_argument("--out-dir", required=True)
    f.add_argument("--recursive", action="store_true")
    f.add_argument("--max-items", type=int, default=0)
    f.set_defaults(fn=cmd_farm)

    # farm-sqlite
    fs = sub.add_parser("farm-sqlite", help="run all profiles in dir into ONE SQLite DB (per-profile tables) + blocked_events queue")
    fs.add_argument("--profiles-dir", required=True)
    fs.add_argument("--db", required=True)
    fs.add_argument("--recursive", action="store_true")
    fs.add_argument("--max-items", type=int, default=0, help="max items per profile (0=no limit)")
    fs.add_argument("--resume", action="store_true", help="resume each profile from its latest run_id in DB")
    fs.add_argument("--raw-prefix", default="raw_")
    fs.add_argument("--unique-prefix", default="unique_")
    fs.set_defaults(fn=cmd_farm_sqlite)


    # farm-resume-open
    fro = sub.add_parser("farm-resume-open", help="resume all profiles with open blocked_events in DB")
    fro.add_argument("--db", required=True)
    fro.add_argument("--defaults", default=None)
    fro.add_argument("--profile-path", default=None, help="fallback profile path if blocked_event.profile_path missing")
    fro.add_argument("--max-profiles", type=int, default=0, help="limit profiles to resume (0=no limit)")
    fro.add_argument("--max-items", type=int, default=0, help="max items per profile run (0=no limit)")
    fro.add_argument("--raw-table", default="items_raw")
    fro.add_argument("--unique-table", default="items_unique")
    fro.add_argument("--dry-run", action="store_true", help="only list what would be resumed")
    fro.add_argument("--auto-resolve", action="store_true", help="mark blocked_event resolved after successful resume")
    fro.add_argument("--resolve-note", default="auto_resolve")
    fro.set_defaults(fn=cmd_farm_resume_open)

    # blocked-* (SQLite queue)
    bl = sub.add_parser("blocked-list", help="list blocked_events from SQLite")
    bl.add_argument("--db", required=True)
    bl.add_argument("--profile-name", default=None)
    bl.add_argument("--run-id", default=None)
    bl.add_argument("--all", action="store_true", help="include resolved")
    bl.add_argument("--limit", type=int, default=50)
    bl.add_argument("--offset", type=int, default=0)
    bl.set_defaults(fn=cmd_blocked_list)

    be = sub.add_parser("blocked-export", help="export blocked_events to JSONL/CSV")
    be.add_argument("--db", required=True)
    be.add_argument("--out", required=True)
    be.add_argument("--format", choices=["jsonl","csv"], default="jsonl")
    be.add_argument("--profile-name", default=None)
    be.add_argument("--run-id", default=None)
    be.add_argument("--all", action="store_true")
    be.add_argument("--limit", type=int, default=5000)
    be.add_argument("--offset", type=int, default=0)
    be.set_defaults(fn=cmd_blocked_export)

    br = sub.add_parser("blocked-resolve", help="mark blocked_event as resolved")
    br.add_argument("--db", required=True)
    br.add_argument("--id", required=True, type=int)
    br.add_argument("--note", default="")
    br.set_defaults(fn=cmd_blocked_resolve)

    brr = sub.add_parser("blocked-resume", help="resume run-sqlite by latest open blocked_event")
    brr.add_argument("--db", required=True)
    brr.add_argument("--profile", required=True, help="profile path to resume")
    brr.add_argument("--profile-name", default=None, help="if set, uses latest open blocked for this profile name")
    brr.add_argument("--id", default=None, type=int, help="resume by blocked_event id")
    brr.add_argument("--raw-table", default="items_raw")
    brr.add_argument("--unique-table", default="items_unique")
    brr.add_argument("--max-items", type=int, default=0)
    brr.set_defaults(fn=cmd_blocked_resume)

    ss = sub.add_parser("secrets-set", help="upsert secret entry into secrets.json (local)")
    ss.add_argument("--secrets", required=True, help="path to secrets.json")
    ss.add_argument("--ref", required=True)
    ss.add_argument("--type", required=True, choices=["cookies_file","bearer","api_key_header","basic","headers","api_key_query"])
    ss.add_argument("--cookies-file", default=None)
    ss.add_argument("--token", default=None)
    ss.add_argument("--header", default=None)
    ss.add_argument("--username", default=None)
    ss.add_argument("--password", default=None)
    ss.add_argument("--headers-json", default=None, help='JSON dict, e.g. {"X":"1"}')
    ss.add_argument("--param", default=None)
    ss.set_defaults(fn=cmd_secrets_set)


    # pipeline
    pl = sub.add_parser("pipeline", help="draft->fixed->active->errors (2-pass: minimal then auto-fix)")
    pl.add_argument("--draft", required=True)
    pl.add_argument("--fixed", required=True)
    pl.add_argument("--active", required=True)
    pl.add_argument("--errors", required=True)
    pl.add_argument("--move", action="store_true", help="move files instead of copy")
    pl.add_argument("--recursive", action="store_true")
    pl.add_argument("--smoke", type=int, default=5, help="smoke items count (used in PASS2, and PASS1 unless --smoke0)")
    pl.add_argument("--stagnation-window", type=int, default=10)
    pl.add_argument("--smoke0", action="store_true", help="disable smoke in PASS1")
    pl.add_argument("--no-infer", action="store_true", help="PASS2: do not infer pagination")
    pl.add_argument("--no-limit-probe", action="store_true", help="PASS2: do not probe limit_param")
    pl.add_argument("--reports-dir", default=None, help="optional dir to write per-profile pass reports (JSON)")
    pl.set_defaults(fn=cmd_pipeline)

    return p


def main() -> int:
    p = build_parser()
    args = p.parse_args()
    # propagate pretty into command args
    if not hasattr(args, "pretty"):
        args.pretty = False

    try:
        return int(args.fn(args) or 0)
    except CliError as e:
        print(str(e), file=sys.stderr)
        return int(e.exit_code)


if __name__ == "__main__":
    raise SystemExit(main())
