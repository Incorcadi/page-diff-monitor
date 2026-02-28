from __future__ import annotations

from typing import Iterable, Optional

from .site_profile import SiteProfile, load_profile
from .site_patches import apply_site_patches, load_site_patches


def normalize_patch_refs(raw: Optional[Iterable[str]]) -> list[str]:
    out: list[str] = []
    for value in raw or []:
        if not isinstance(value, str):
            continue
        for piece in value.split(","):
            p = piece.strip()
            if p:
                out.append(p)
    return out


def load_profile_for_runtime(
    path: str,
    *,
    defaults_path: Optional[str] = None,
    patch_refs: Optional[Iterable[str]] = None,
    patches_dir: Optional[str] = None,
) -> SiteProfile:
    profile = load_profile(path, defaults_path=defaults_path)
    refs = normalize_patch_refs(patch_refs)
    if not refs:
        return profile

    patches = load_site_patches(refs, patches_dir=patches_dir)
    return apply_site_patches(profile, patches)
