from __future__ import annotations

from copy import deepcopy
from dataclasses import dataclass
import json
import re
from pathlib import Path
from typing import Any, Iterable, Optional

from .site_profile import SiteProfile


@dataclass(frozen=True)
class SetOp:
    path: str
    value: Any


@dataclass(frozen=True)
class SitePatch:
    name: str
    source: str
    enabled: bool
    merge: dict[str, Any]
    set_ops: tuple[SetOp, ...]
    delete_paths: tuple[str, ...]


def _deep_merge(base: dict[str, Any], override: dict[str, Any]) -> dict[str, Any]:
    out: dict[str, Any] = dict(base)
    for key, value in override.items():
        if key in out and isinstance(out[key], dict) and isinstance(value, dict):
            out[key] = _deep_merge(out[key], value)
        else:
            out[key] = value
    return out


def _split_path(path: str) -> list[str]:
    return [part for part in str(path).split(".") if part]


def _set_by_path(obj: dict[str, Any], path: str, value: Any) -> None:
    parts = _split_path(path)
    if not parts:
        raise ValueError("set path must not be empty")

    cur: dict[str, Any] = obj
    for key in parts[:-1]:
        nxt = cur.get(key)
        if not isinstance(nxt, dict):
            nxt = {}
            cur[key] = nxt
        cur = nxt
    cur[parts[-1]] = value


def _delete_by_path(obj: dict[str, Any], path: str) -> None:
    parts = _split_path(path)
    if not parts:
        return

    cur: dict[str, Any] = obj
    for key in parts[:-1]:
        nxt = cur.get(key)
        if not isinstance(nxt, dict):
            return
        cur = nxt
    cur.pop(parts[-1], None)


def _parse_set_ops(raw: Any) -> tuple[SetOp, ...]:
    if raw is None:
        return ()

    out: list[SetOp] = []
    if isinstance(raw, dict):
        for path, value in raw.items():
            if isinstance(path, str) and path.strip():
                out.append(SetOp(path=path.strip(), value=value))
        return tuple(out)

    if isinstance(raw, list):
        for item in raw:
            if not isinstance(item, dict):
                continue
            path = item.get("path")
            if not isinstance(path, str) or not path.strip():
                continue
            out.append(SetOp(path=path.strip(), value=item.get("value")))
        return tuple(out)

    raise ValueError("patch 'set' must be dict[path->value] or list[{path,value}]")


def _parse_delete_paths(raw: Any) -> tuple[str, ...]:
    if raw is None:
        return ()
    if isinstance(raw, str):
        p = raw.strip()
        return (p,) if p else ()
    if isinstance(raw, list):
        out = [str(x).strip() for x in raw if isinstance(x, str) and str(x).strip()]
        return tuple(out)
    raise ValueError("patch 'delete' must be a string or list of strings")


def parse_site_patch_dict(data: dict[str, Any], *, source: str) -> SitePatch:
    if not isinstance(data, dict):
        raise ValueError("patch document must be a JSON object")

    raw_name = data.get("name")
    if isinstance(raw_name, str) and raw_name.strip():
        name = raw_name.strip()
    else:
        stem = Path(source).name
        if stem.endswith(".patch.json"):
            name = stem[: -len(".patch.json")]
        else:
            name = Path(source).stem

    merge = data.get("merge") or {}
    if not isinstance(merge, dict):
        raise ValueError("patch 'merge' must be a JSON object")

    set_ops = _parse_set_ops(data.get("set"))
    delete_paths = _parse_delete_paths(data.get("delete") or data.get("remove"))
    enabled = bool(data.get("enabled", True))

    return SitePatch(
        name=name,
        source=source,
        enabled=enabled,
        merge=merge,
        set_ops=set_ops,
        delete_paths=delete_paths,
    )


def apply_site_patch_dict(profile_dict: dict[str, Any], patch: SitePatch) -> dict[str, Any]:
    out: dict[str, Any] = deepcopy(profile_dict)

    if patch.merge:
        out = _deep_merge(out, deepcopy(patch.merge))
    for op in patch.set_ops:
        _set_by_path(out, op.path, deepcopy(op.value))
    for path in patch.delete_paths:
        _delete_by_path(out, path)

    return out


def _get_meta_dict(profile_dict: dict[str, Any]) -> dict[str, Any]:
    m = profile_dict.get("_meta")
    if isinstance(m, dict):
        return m
    # normalize
    profile_dict["_meta"] = {}
    return profile_dict["_meta"]

def _get_patch_policy(profile_dict: dict[str, Any]) -> dict[str, Any]:
    meta = _get_meta_dict(profile_dict)
    pp = meta.get("patch_policy")
    return pp if isinstance(pp, dict) else {}

def _policy_bool(profile_dict: dict[str, Any], key: str) -> bool:
    pp = _get_patch_policy(profile_dict)
    return bool(pp.get(key))

def _policy_int(profile_dict: dict[str, Any], key: str, default: int) -> int:
    pp = _get_patch_policy(profile_dict)
    try:
        v = int(pp.get(key, default))
        return max(1, min(200, v))
    except Exception:
        return default

def _policy_list(profile_dict: dict[str, Any], key: str) -> list[str]:
    pp = _get_patch_policy(profile_dict)
    raw = pp.get(key)
    if raw is None:
        return []
    if isinstance(raw, str):
        s = raw.strip()
        return [s] if s else []
    if isinstance(raw, list):
        out: list[str] = []
        for x in raw:
            if isinstance(x, str) and x.strip():
                out.append(x.strip())
        return out
    return []

def _policy_str(profile_dict: dict[str, Any], key: str, default: str = "") -> str:
    pp = _get_patch_policy(profile_dict)
    v = pp.get(key)
    return str(v) if v is not None else default

def _policy_max_ignored_report(profile_dict: dict[str, Any]) -> int:
    try:
        n = int(_get_patch_policy(profile_dict).get("max_ignored_report") or 200)
        return max(0, min(5000, n))
    except Exception:
        return 200

def _policy_list_by_domain(profile_dict: dict[str, Any]) -> list[str]:
    # convenience alias for separate by-domain ignore rules
    return _policy_list(profile_dict, "ignore_by_domain")

def _split_domain_pattern(p: str) -> tuple[str, str]:
    # format: "<domain_pattern>:<leaf_pattern>"
    # example: ".example.com:browser_fallback.playwright.timeout_ms"
    s = (p or "").strip()
    if ":" not in s:
        return s, "*"
    dom, leaf = s.split(":", 1)
    dom = dom.strip()
    leaf = leaf.strip() or "*"
    return dom, leaf

def _match_wildcard(pat: str, value: str) -> bool:
    if "*" not in pat:
        return pat == value
    try:
        return _compile_wildcard(pat).match(value) is not None
    except Exception:
        return False

def _is_ignored_by_domain(domain: str, leaf_path: str, ignore_by_domain: list[str]) -> tuple[bool, str]:
    if not ignore_by_domain:
        return False, ""
    d = str(domain)
    leaf = str(leaf_path)
    for raw in ignore_by_domain:
        dom_pat, leaf_pat = _split_domain_pattern(raw)
        if not dom_pat:
            continue
        if _match_wildcard(dom_pat, d) and _match_wildcard(leaf_pat, leaf):
            return True, raw
    return False, ""

def _compile_wildcard(pat: str) -> re.Pattern:
    # wildcard '*' matches any chars
    esc = re.escape(pat)
    esc = esc.replace(r"\*", ".*")
    return re.compile(r"^" + esc + r"$")

def _is_ignored(path: str, ignore_patterns: list[str]) -> bool:
    if not ignore_patterns:
        return False
    p = str(path)
    for pat in ignore_patterns:
        if not pat:
            continue
        # Fast paths
        if "*" not in pat:
            # Support prefix ignore via 'foo.*'
            if pat.endswith(".*"):
                pref = pat[:-2]
                if p == pref or p.startswith(pref + "."):
                    return True
            else:
                if p == pat:
                    return True
            continue
        # wildcard
        try:
            if _compile_wildcard(pat).match(p):
                return True
        except Exception:
            # if regex compile fails, ignore pattern
            continue
    return False

def _by_domain_key(domain: str, leaf_path: str) -> str:
    # Unambiguous string for ignore matching and reporting.
    # Example: by_domain:.example.com:browser_fallback.playwright.mode
    return f"by_domain:{domain}:{leaf_path}"

def _json_norm(value: Any) -> str:
    try:
        return json.dumps(value, ensure_ascii=False, sort_keys=True)
    except Exception:
        return repr(value)

def _iter_merge_leaf_paths(d: Any, prefix: str = "") -> list[tuple[str, Any]]:
    """Return list of (dot_path, leaf_value) for a merge dict.
    - only recurses into dicts
    - lists/strings/numbers are treated as leaves
    """
    out: list[tuple[str, Any]] = []
    if isinstance(d, dict):
        for k, v in d.items():
            p = f"{prefix}.{k}" if prefix else str(k)
            if isinstance(v, dict):
                out.extend(_iter_merge_leaf_paths(v, p))
            else:
                out.append((p, v))
    return out

def _iter_patch_writes(patch: SitePatch) -> list[tuple[str, Any, str]]:
    """Return writes as (path, value, kind) where kind is merge|set|delete."""
    writes: list[tuple[str, Any, str]] = []
    if patch.merge:
        for p, v in _iter_merge_leaf_paths(patch.merge, ""):
            writes.append((p, v, "merge"))
    for op in patch.set_ops:
        writes.append((op.path, op.value, "set"))
    for p in patch.delete_paths:
        writes.append((p, {"__delete__": True}, "delete"))
    return writes

def _extract_by_domain_from_merge(merge: dict[str, Any]) -> dict[str, Any]:
    """If patch.merge contains _meta.http.headers.by_domain dict, return it."""
    if not isinstance(merge, dict):
        return {}
    meta = merge.get("_meta")
    if not isinstance(meta, dict):
        return {}
    http = meta.get("http")
    if not isinstance(http, dict):
        return {}
    headers = http.get("headers")
    if not isinstance(headers, dict):
        return {}
    bd = headers.get("by_domain")
    return bd if isinstance(bd, dict) else {}

def _iter_domain_leaf_paths(obj: Any, prefix: str = "") -> list[tuple[str, Any]]:
    """Traverse domain config dict (keys should NOT contain dots)."""
    out: list[tuple[str, Any]] = []
    if isinstance(obj, dict):
        for k, v in obj.items():
            p = f"{prefix}.{k}" if prefix else str(k)
            if isinstance(v, dict):
                out.extend(_iter_domain_leaf_paths(v, p))
            else:
                out.append((p, v))
    return out

def apply_site_patches(profile: SiteProfile, patches: Iterable[SitePatch]) -> SiteProfile:
    """Apply patches and produce a patch_report with conflict detection.

    Report is stored into effective profile under:
      _meta.patch_report = {
        "applied": [...],
        "conflicts": [...],              # generic leaf-path conflicts
        "by_domain_conflicts": [...],    # domain-scoped conflicts (safe for domain keys with dots)
      }

    Strict policy (optional) — enable via profile/_defaults/_patch:
      _meta.patch_policy.strict_any = true
      _meta.patch_policy.strict_by_domain = true
      _meta.patch_policy.max_conflicts = 20
    """
    data = profile.to_dict()
    applied: list[str] = []
    conflicts: list[dict[str, Any]] = []
    by_domain_conflicts: list[dict[str, Any]] = []
    ignore_patterns = _policy_list(data, "ignore_paths")
    ignored_conflicts = 0
    ignore_by_domain = _policy_list_by_domain(data)
    ignored: list[dict[str, Any]] = []
    max_ignored = _policy_max_ignored_report(data)

    # path -> (json_value, patch_name)
    writes: dict[str, tuple[str, str]] = {}
    # (domain, leaf_path) -> (json_value, patch_name)
    dom_writes: dict[tuple[str, str], tuple[str, str]] = {}

    for patch in patches:
        if not patch.enabled:
            continue
        applied.append(patch.name)

        # --- generic conflicts ---
        for path, value, kind in _iter_patch_writes(patch):
            # by_domain is handled separately: domain keys contain dots, dot-path becomes ambiguous
            if str(path).startswith("_meta.http.headers.by_domain"):
                continue
            vj = _json_norm(value)
            prev = writes.get(path)
            if prev and prev[0] != vj:
                if _is_ignored(path, ignore_patterns):
                    ignored_conflicts += 1
                    if max_ignored == 0 or len(ignored) < max_ignored:
                        ignored.append({
                            "path": path,
                            "kind": kind,
                            "previous_from": prev[1],
                            "previous_value": prev[0],
                            "new_from": patch.name,
                            "new_value": vj,
                            "ignored_by": "ignore_paths",
                        })
                else:
                    conflicts.append({
                        "path": path,
                        "kind": kind,
                        "previous_from": prev[1],
                        "previous_value": prev[0],
                        "new_from": patch.name,
                        "new_value": vj,
                    })
            writes[path] = (vj, patch.name)

        # --- by_domain conflicts (merge-only, safe) ---
        bd = _extract_by_domain_from_merge(patch.merge or {})
        if bd:
            for domain, cfg in bd.items():
                if not isinstance(domain, str):
                    continue
                # cfg is usually dict: {"browser_fallback": {...}}
                for leaf_path, leaf_value in _iter_domain_leaf_paths(cfg, ""):
                    key = (domain, leaf_path)
                    vj = _json_norm(leaf_value)
                    prev = dom_writes.get(key)
                    if prev and prev[0] != vj:
                        k = _by_domain_key(domain, leaf_path)
                        ok1 = _is_ignored(k, ignore_patterns)
                        ok2, pat2 = _is_ignored_by_domain(domain, leaf_path, ignore_by_domain)
                        if ok1 or ok2:
                            ignored_conflicts += 1
                            if max_ignored == 0 or len(ignored) < max_ignored:
                                ignored.append({
                                    "kind": "by_domain",
                                    "domain": domain,
                                    "leaf_path": leaf_path,
                                    "previous_from": prev[1],
                                    "previous_value": prev[0],
                                    "new_from": patch.name,
                                    "new_value": vj,
                                    "ignored_by": "ignore_by_domain" if ok2 else "ignore_paths",
                                    "matched_pattern": pat2 if ok2 else "",
                                })
                        else:
                            by_domain_conflicts.append({
                                "domain": domain,
                                "leaf_path": leaf_path,
                                "previous_from": prev[1],
                                "previous_value": prev[0],
                                "new_from": patch.name,
                                "new_value": vj,
                            })
                    dom_writes[key] = (vj, patch.name)

        # Apply patch
        data = apply_site_patch_dict(data, patch)

    # Attach report
    meta = _get_meta_dict(data)
    pr = {
        "applied": applied,
        "conflicts": conflicts,
        "by_domain_conflicts": by_domain_conflicts,
        "ignore_paths": ignore_patterns,
        "ignore_by_domain": ignore_by_domain,
        "ignored_conflicts": ignored_conflicts,
        "ignored": ignored,
        "max_ignored_report": max_ignored,

    }
    meta["patch_report"] = pr

    # Strict modes
    strict_any = _policy_bool(data, "strict_any")
    strict_by_domain = _policy_bool(data, "strict_by_domain")
    max_c = _policy_int(data, "max_conflicts", 20)

    if strict_by_domain and by_domain_conflicts:
        head = by_domain_conflicts[:max_c]
        raise ValueError(json.dumps({
            "error": "by_domain_patch_conflict",
            "conflicts_count": len(by_domain_conflicts),
            "conflicts_head": head,
            "hint": "Resolve by changing patch order, splitting domains, or consolidating into one patch.",
        }, ensure_ascii=False, indent=2))

    if strict_any and (conflicts or by_domain_conflicts):
        allc = (conflicts + [{"domain": c["domain"], "path": c["leaf_path"], "previous_from": c["previous_from"], "new_from": c["new_from"]} for c in by_domain_conflicts])
        head = allc[:max_c]
        raise ValueError(json.dumps({
            "error": "patch_conflict",
            "conflicts_count": len(allc),
            "conflicts_head": head,
            "hint": "Resolve by changing patch order, removing conflicting patches, or consolidating into one patch.",
        }, ensure_ascii=False, indent=2))

    return SiteProfile.from_dict(data)


def _default_patches_dir(patches_dir: Optional[str]) -> Path:
    if patches_dir:
        return Path(patches_dir)
    return Path("profiles") / "patches"


def resolve_site_patch_path(patch_ref: str, *, patches_dir: Optional[str] = None) -> Path:
    raw = (patch_ref or "").strip()
    if not raw:
        raise FileNotFoundError("empty patch reference")

    candidate = Path(raw)
    if candidate.exists():
        return candidate.resolve()

    root = _default_patches_dir(patches_dir)
    names = [raw, f"{raw}.json", f"{raw}.patch.json"]
    roots = [root, root / "sites"]
    for base in roots:
        for name in names:
            probe = base / name
            if probe.exists():
                return probe.resolve()

    raise FileNotFoundError(
        f"cannot resolve patch '{patch_ref}' in '{root}' or '{root / 'sites'}'"
    )


def load_site_patch_file(path: str | Path) -> SitePatch:
    p = Path(path)
    with p.open("r", encoding="utf-8") as f:
        data = json.load(f)
    if not isinstance(data, dict):
        raise ValueError(f"patch file must contain JSON object: {p}")
    return parse_site_patch_dict(data, source=str(p))


def load_site_patches(
    patch_refs: Iterable[str],
    *,
    patches_dir: Optional[str] = None,
) -> list[SitePatch]:
    out: list[SitePatch] = []
    for ref in patch_refs:
        path = resolve_site_patch_path(ref, patches_dir=patches_dir)
        out.append(load_site_patch_file(path))
    return out


def list_available_site_patches(patches_dir: Optional[str] = None) -> list[str]:
    root = _default_patches_dir(patches_dir)
    if not root.exists():
        return []

    names: set[str] = set()
    for fp in root.rglob("*.json"):
        if fp.name.endswith(".patch.json"):
            names.add(fp.name[: -len(".patch.json")])
        else:
            names.add(fp.stem)
    return sorted(names)
