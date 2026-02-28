from __future__ import annotations

import json
from pathlib import Path

from web_farm.framework.profile_loader import load_profile_for_runtime
from web_farm.framework.site_patches import list_available_site_patches


def _write_json(path: Path, obj: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(obj, ensure_ascii=False, indent=2), encoding="utf-8")


def test_load_profile_for_runtime_applies_site_patch(tmp_path: Path):
    profile_path = tmp_path / "profiles" / "site.json"
    _write_json(
        profile_path,
        {
            "name": "demo",
            "url": "https://example.com/api/items",
            "headers": {"Authorization": "legacy"},
            "pagination": {"kind": "unknown"},
            "extract": {"items_path": "items", "id_path": "id"},
        },
    )

    patches_dir = tmp_path / "profiles" / "patches"
    patch_path = patches_dir / "sites" / "demo.patch.json"
    _write_json(
        patch_path,
        {
            "name": "demo",
            "merge": {"headers": {"User-Agent": "UA-1"}, "extract": {"id_path": "meta.id"}},
            "set": {"pagination.kind": "offset", "pagination.limit_param": "limit"},
            "delete": ["headers.Authorization"],
        },
    )

    prof = load_profile_for_runtime(
        str(profile_path),
        patch_refs=["demo"],
        patches_dir=str(patches_dir),
    )

    assert prof.pagination.kind == "offset"
    assert prof.pagination.limit_param == "limit"
    assert prof.extract.id_path == "meta.id"
    assert prof.headers.get("User-Agent") == "UA-1"
    assert "Authorization" not in prof.headers


def test_list_available_site_patches_returns_names(tmp_path: Path):
    patches_dir = tmp_path / "profiles" / "patches" / "sites"
    _write_json(patches_dir / "alpha.patch.json", {"name": "alpha"})
    _write_json(patches_dir / "beta.json", {"name": "beta"})

    names = list_available_site_patches(str(tmp_path / "profiles" / "patches"))
    assert "alpha" in names
    assert "beta" in names
