
from __future__ import annotations

"""
pipeline_microscope_exact.py — “микроскоп” пайплайна на ОДНОМ профиле.

Зачем:
- Pipeline по папкам удобен для потока, но плохо объясняет, *почему* конкретный профиль упал.
- Этот скрипт прогоняет 1 профиль ТОЧНО теми же шагами, что и cmd_pipeline() в tool_pipeline.py:
  PASS1: onboard -> triage -> ACTIVE/FIXED
  PASS2: (если FIXED) auto-fix extract -> infer/limit-probe -> triage -> ACTIVE/ERRORS
- В режиме --write он делает те же временные файлы:
  <fixed>/<stem>.pass1.tmp.json и <fixed>/<stem>.pass2.tmp.json
  и раскладывает профили в active/fixed/errors (move/copy).

Режимы:
- по умолчанию: dry-run (не трогает твою структуру папок, пишет tmp в системную temp-папку)
- с --write: действует как настоящий pipeline (пишет tmp в fixed и раскладывает файлы)
"""

import argparse
import importlib
import json
import shutil
import sys
import tempfile
from pathlib import Path
from typing import Any


def _pretty(obj: Any, pretty: bool) -> str:
    return json.dumps(obj, ensure_ascii=False, indent=(2 if pretty else None))


def _load_tool_module():
    """
    Ищем “пульт” рядом:
    - если есть tool_pipeline.py -> import tool_pipeline
    - иначе, если есть tool.py -> import tool
    """
    here = Path(__file__).resolve().parent
    if (here / "tool_pipeline.py").exists():
        return importlib.import_module("tool_pipeline")
    if (here / "tool.py").exists():
        return importlib.import_module("tool")
    raise RuntimeError("Не найден tool_pipeline.py или tool.py рядом со скриптом.")


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(prog="pipeline_microscope_exact.py")

    p.add_argument("--in", dest="in_path", required=True, help="путь к одному профилю (обычно из profiles/draft)")
    p.add_argument("--defaults", default=None, help="путь к profiles/_defaults.json")
    p.add_argument("--pretty", action="store_true", help="pretty JSON output")
    p.add_argument("--site-patch", action="append", default=None,
                   help="site patch name or path (.patch.json name or explicit path); repeatable")
    p.add_argument("--patches-dir", default=None,
                   help="dir with *.patch.json files (used when --site-patch is a name)")

    # папки стадий (как в pipeline)
    p.add_argument("--draft", default=None, help="папка draft (для отчёта/логики путей); можно не задавать")
    p.add_argument("--fixed", default="profiles/fixed", help="папка fixed")
    p.add_argument("--active", default="profiles/active", help="папка active")
    p.add_argument("--errors", default="profiles/errors", help="папка errors")
    p.add_argument("--reports-dir", default=None, help="куда писать отчёты (как pipeline)")

    # поведение перемещения
    p.add_argument("--write", action="store_true", help="реально писать файлы и раскладывать по папкам (как pipeline)")
    p.add_argument("--move", action="store_true", help="как pipeline --move: переносить, иначе копировать")

    # smoke/зацикливание
    p.add_argument("--smoke", type=int, default=5, help="smoke N (PASS2 и PASS1 если не --smoke0)")
    p.add_argument("--smoke0", action="store_true", help="не делать smoke на PASS1")
    p.add_argument("--stagnation-window", type=int, default=10, help="окно стагнации K")

    # отключалки PASS2 (как pipeline)
    p.add_argument("--no-infer", action="store_true", help="PASS2: не делать infer пагинации")
    p.add_argument("--no-limit-probe", action="store_true", help="PASS2: не искать limit_param")

    return p


def main() -> int:
    args = build_parser().parse_args()
    tool = _load_tool_module()

    # директории стадий
    fixed = Path(args.fixed)
    active = Path(args.active)
    errors = Path(args.errors)
    reports_dir = Path(args.reports_dir) if args.reports_dir else None

    for d in (fixed, active, errors):
        d.mkdir(parents=True, exist_ok=True)
    if reports_dir:
        reports_dir.mkdir(parents=True, exist_ok=True)

    def write_report(name: str, payload: dict[str, Any]) -> None:
        if not reports_dir or not args.write:
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

    src = Path(args.in_path)
    stem = src.stem

    out: dict[str, Any] = {
        "input": str(src),
        "write_mode": bool(args.write),
        "move_mode": bool(args.move),
        "defaults": args.defaults,
        "dirs": {"fixed": str(fixed), "active": str(active), "errors": str(errors), "reports": (str(reports_dir) if reports_dir else None)},
    }

    # ----------------------------
    # PASS 1: onboard -> triage -> ACTIVE/FIXED
    # ----------------------------
    pass1: dict[str, Any] = {"in": str(src), "pass": 1}
    tmp_base_dir: Path
    tmp_ctx = None

    if args.write:
        tmp_base_dir = fixed
    else:
        tmp_ctx = tempfile.TemporaryDirectory(prefix="microscope_")
        tmp_base_dir = Path(tmp_ctx.name)

    try:
        # 1) onboard -> tmp
        tmp1 = tmp_base_dir / f"{stem}.pass1.tmp.json"
        ns_on = argparse.Namespace(
            in_path=str(src),
            defaults=args.defaults,
            out_path=str(tmp1),
            pretty=True,
            print_report=False,
            site_patch=getattr(args, 'site_patch', None),
            patches_dir=getattr(args, 'patches_dir', None),
            secrets=getattr(args, 'secrets', None),
            cache_dir=getattr(args, 'cache_dir', None),
            replay=getattr(args, 'replay', False),
            diag_http=getattr(args, 'diag_http', False),
        )
        tool.cmd_onboard(ns_on)

        # 2) triage onboarded
        prof_tmp = tool.load_profile(str(tmp1), defaults_path=None)
        eng = _make_engine(tool, prof_tmp)
        tri = tool._triage(
            prof_tmp,
            engine=eng,
            smoke=(0 if args.smoke0 else args.smoke),
            stagnation_window=args.stagnation_window,
        )
        pass1["triage"] = tri

        # stage decision
        if tri["label"] == "OK":
            dest = active / src.name
            pass1["stage"] = "ACTIVE"
        else:
            dest = fixed / src.name
            pass1["stage"] = "FIXED"
        pass1["out"] = str(dest)

        # “как pipeline”: в write-режиме реально раскладываем tmp -> dest
        if args.write:
            move_or_copy(tmp1, dest)
            tmp1.unlink(missing_ok=True)
            if args.move:
                src.unlink(missing_ok=True)

        write_report(f"{stem}.pass1.json", pass1)

    except Exception as e:
        pass1["error"] = str(e)
        # pipeline-поведение: PASS1 упал -> кладём исходник в fixed (или в errors если не вышло)
        try:
            dest = fixed / src.name
            pass1["stage"] = "FIXED"
            pass1["out"] = str(dest)
            if args.write:
                move_or_copy(src, dest)
                if args.move:
                    src.unlink(missing_ok=True)
        except Exception as e2:
            dest = errors / src.name
            pass1["stage"] = "ERRORS"
            pass1["out"] = str(dest)
            pass1["error2"] = str(e2)
            if args.write:
                try:
                    move_or_copy(src, dest)
                except Exception:
                    pass

        write_report(f"{stem}.pass1.json", pass1)

    out["pass1"] = pass1

    # закрываем dry-run tempdir после PASS1 (если PASS2 не нужен — сразу освободим)
    def _cleanup_tmp():
        nonlocal tmp_ctx
        if tmp_ctx is not None:
            tmp_ctx.cleanup()
            tmp_ctx = None

    # ----------------------------
    # PASS 2: только если PASS1 вывел FIXED
    # ----------------------------
    if pass1.get("stage") != "FIXED":
        _cleanup_tmp()
        print(_pretty(out, args.pretty))
        return 0

    # источник для PASS2:
    # - write-режим: это fixed/<name>.json (после PASS1)
    # - dry-run: работаем “как будто” fixed/<name>.json, но читаем исходник напрямую
    pass2_src = fixed / src.name if args.write else src

    pass2: dict[str, Any] = {"in": str(pass2_src), "pass": 2}
    try:
        prof = tool.load_profile(str(pass2_src), defaults_path=args.defaults)
        eng = _make_engine(tool, prof)

        # 1) базовый запрос (чтобы подсказать items/id)
        resp, data, err = eng.safe_get_json(
            prof.url,
            method=prof.method,
            params=tool._merge_params(prof),
            headers=prof.headers,
            timeout=prof.timeout,
            force_json=False,
            detect_soft=True,
        )
        pass2["base"] = {
            "status": getattr(resp, "status_code", None) if resp is not None else None,
            "content_type": (resp.headers.get("Content-Type") if resp is not None else None),
            "err": err,
        }

        if err or data is None:
            dest = errors / src.name
            pass2["stage"] = "ERRORS"
            pass2["out"] = str(dest)
            if args.write:
                move_or_copy(Path(pass2_src), dest)
                if args.move:
                    Path(pass2_src).unlink(missing_ok=True)
            write_report(f"{stem}.pass2.json", pass2)
            out["pass2"] = pass2
            _cleanup_tmp()
            print(_pretty(out, args.pretty))
            return 0

        # 2) auto-fix items_path / id_path по первой странице
        patch: dict[str, Any] = {}
        items = tool.extract_items(data, prof.extract)
        if not items:
            guess = tool._guess_items_path(data)
            if guess:
                patch.setdefault("extract", {})["items_path"] = guess
        else:
            ids = tool.ids_of(items, prof.extract)
            if len(ids) == 0:
                guess_id = tool._guess_id_path(items)
                if guess_id:
                    patch.setdefault("extract", {})["id_path"] = guess_id

        # применяем extract-патч в память, чтобы infer работал по правильным путям
        if "extract" in patch:
            d0 = prof.to_dict()
            prof = tool.SiteProfile.from_dict(tool._deep_merge(d0, patch))

        pass2["patch_extract"] = patch or None

        # 3) infer pagination
        if not args.no_infer:
            pag, inf_rep = tool.infer_mod.infer_pagination(prof, engine=eng)
            prof.pagination = pag
            pass2["infer_report"] = inf_rep
            pass2["pagination"] = tool.asdict(pag)

        # 4) probe limit_param
        if not args.no_limit_probe:
            limit_param, lim_rep = tool.onboard_mod.find_limit_param(prof, engine=eng)
            pass2["limit_report"] = lim_rep
            pass2["limit_param_found"] = limit_param
            if limit_param:
                prof.pagination.limit_param = limit_param

        # 5) сохранить применённый профиль во временный файл
        tmp2 = (tmp_base_dir / f"{stem}.pass2.tmp.json")
        tool.save_profile(prof, str(tmp2), pretty=True)

        # 6) финальный triage
        tri2 = tool._triage(
            prof,
            engine=eng,
            smoke=args.smoke,
            stagnation_window=args.stagnation_window,
        )
        pass2["triage"] = tri2

        if tri2["label"] == "OK":
            dest = active / src.name
            pass2["stage"] = "ACTIVE"
        else:
            dest = errors / src.name
            pass2["stage"] = "ERRORS"
        pass2["out"] = str(dest)

        if args.write:
            move_or_copy(tmp2, dest)
            if args.move:
                Path(pass2_src).unlink(missing_ok=True)
            tmp2.unlink(missing_ok=True)

        write_report(f"{stem}.pass2.json", pass2)

    except Exception as e:
        pass2["error"] = str(e)
        dest = errors / src.name
        pass2["stage"] = "ERRORS"
        pass2["out"] = str(dest)
        if args.write:
            try:
                move_or_copy(Path(pass2_src), dest)
            except Exception:
                pass
            if args.move:
                Path(pass2_src).unlink(missing_ok=True)
        write_report(f"{stem}.pass2.json", pass2)

    out["pass2"] = pass2
    _cleanup_tmp()
    print(_pretty(out, args.pretty))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
