from __future__ import annotations

# Backward/forward compatible import path used by tests and docs.
from web_farm.site_patches import (  # noqa: F401
    SitePatch,
    SetOp,
    apply_site_patches,
    apply_site_patch_dict,
    list_available_site_patches,
    load_site_patches,
    parse_site_patch_dict,
)
