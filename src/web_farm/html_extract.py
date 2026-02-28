from __future__ import annotations

from dataclasses import dataclass, field
from html.parser import HTMLParser
import re
from typing import Any, Optional, Sequence

from .site_profile import ExtractSpec


_WS_RE = re.compile(r"\s+")
_ATTR_MODE_RE = re.compile(r"^attr\(([^()]+)\)$")


@dataclass
class _HtmlNode:
    tag: str
    attrs: dict[str, str]
    parent: Optional[int]
    children: list[int] = field(default_factory=list)
    text_parts: list[str] = field(default_factory=list)


class _HtmlTreeBuilder(HTMLParser):
    def __init__(self) -> None:
        super().__init__(convert_charrefs=True)
        self.nodes: list[_HtmlNode] = [_HtmlNode(tag="__root__", attrs={}, parent=None)]
        self.stack: list[int] = [0]

    def _push_node(self, tag: str, attrs: Sequence[tuple[str, Optional[str]]], *, self_close: bool) -> None:
        parent = self.stack[-1] if self.stack else 0
        clean_attrs: dict[str, str] = {}
        for k, v in attrs:
            if not k:
                continue
            clean_attrs[str(k).strip().lower()] = "" if v is None else str(v)

        idx = len(self.nodes)
        self.nodes.append(
            _HtmlNode(
                tag=str(tag or "").strip().lower(),
                attrs=clean_attrs,
                parent=parent,
            )
        )
        self.nodes[parent].children.append(idx)
        if not self_close:
            self.stack.append(idx)

    def handle_starttag(self, tag: str, attrs: Sequence[tuple[str, Optional[str]]]) -> None:
        self._push_node(tag, attrs, self_close=False)

    def handle_startendtag(self, tag: str, attrs: Sequence[tuple[str, Optional[str]]]) -> None:
        self._push_node(tag, attrs, self_close=True)

    def handle_endtag(self, tag: str) -> None:
        if len(self.stack) <= 1:
            return
        t = str(tag or "").strip().lower()
        for i in range(len(self.stack) - 1, 0, -1):
            if self.nodes[self.stack[i]].tag == t:
                del self.stack[i:]
                return

    def handle_data(self, data: str) -> None:
        if not data or not self.stack:
            return
        self.nodes[self.stack[-1]].text_parts.append(data)


@dataclass(frozen=True)
class _SimpleSelector:
    tag: Optional[str]
    id_value: Optional[str]
    classes: tuple[str, ...]
    attrs: tuple[tuple[str, Optional[str]], ...]


def _iter_descendants(nodes: list[_HtmlNode], start_id: int) -> list[int]:
    out: list[int] = []
    stack = list(reversed(nodes[start_id].children))
    while stack:
        idx = stack.pop()
        out.append(idx)
        if nodes[idx].children:
            stack.extend(reversed(nodes[idx].children))
    return out


def _parse_html_nodes(html: str) -> list[_HtmlNode]:
    p = _HtmlTreeBuilder()
    p.feed(html or "")
    p.close()
    return p.nodes


def _node_text(nodes: list[_HtmlNode], node_id: int) -> str:
    parts: list[str] = []
    stack = [node_id]
    while stack:
        cur = stack.pop()
        node = nodes[cur]
        for piece in node.text_parts:
            if piece and piece.strip():
                parts.append(piece.strip())
        if node.children:
            stack.extend(reversed(node.children))
    if not parts:
        return ""
    return _WS_RE.sub(" ", " ".join(parts)).strip()


def _split_selector(selector: str) -> list[str]:
    sel = str(selector or "").strip()
    if not sel:
        return []
    out: list[str] = []
    buf: list[str] = []
    depth = 0
    quote: Optional[str] = None

    for ch in sel:
        if quote is not None:
            buf.append(ch)
            if ch == quote:
                quote = None
            continue

        if ch in ("'", '"'):
            quote = ch
            buf.append(ch)
            continue

        if ch == "[":
            depth += 1
            buf.append(ch)
            continue
        if ch == "]":
            if depth > 0:
                depth -= 1
            buf.append(ch)
            continue

        if ch.isspace() and depth == 0:
            if buf:
                out.append("".join(buf))
                buf = []
            continue

        buf.append(ch)

    if buf:
        out.append("".join(buf))
    return out


def _read_ident(token: str, pos: int) -> tuple[str, int]:
    n = len(token)
    i = pos
    while i < n and token[i] not in ".#[":
        i += 1
    return token[pos:i], i


def _parse_simple_selector(token: str) -> Optional[_SimpleSelector]:
    t = str(token or "").strip()
    if not t:
        return None

    i = 0
    n = len(t)
    tag: Optional[str] = None
    id_value: Optional[str] = None
    classes: list[str] = []
    attrs: list[tuple[str, Optional[str]]] = []

    if i < n and (t[i].isalpha() or t[i] == "*"):
        start = i
        i += 1
        while i < n and (t[i].isalnum() or t[i] in ("_", "-")):
            i += 1
        tag = t[start:i].lower()

    while i < n:
        ch = t[i]
        if ch == "#":
            i += 1
            ident, i = _read_ident(t, i)
            if not ident:
                return None
            id_value = ident
            continue
        if ch == ".":
            i += 1
            ident, i = _read_ident(t, i)
            if not ident:
                return None
            classes.append(ident)
            continue
        if ch == "[":
            end = t.find("]", i + 1)
            if end < 0:
                return None
            body = t[i + 1 : end].strip()
            if not body:
                return None
            if "=" in body:
                k, v = body.split("=", 1)
                key = k.strip().lower()
                val = v.strip()
                if (val.startswith('"') and val.endswith('"')) or (val.startswith("'") and val.endswith("'")):
                    val = val[1:-1]
                attrs.append((key, val))
            else:
                attrs.append((body.lower(), None))
            i = end + 1
            continue
        return None

    return _SimpleSelector(
        tag=tag,
        id_value=id_value,
        classes=tuple(classes),
        attrs=tuple(attrs),
    )


def _matches(node: _HtmlNode, sel: _SimpleSelector) -> bool:
    if sel.tag and sel.tag != "*" and node.tag != sel.tag:
        return False

    if sel.id_value is not None and node.attrs.get("id") != sel.id_value:
        return False

    if sel.classes:
        cls = node.attrs.get("class") or ""
        cls_set = {x for x in _WS_RE.split(cls.strip()) if x}
        if any(c not in cls_set for c in sel.classes):
            return False

    for key, expected in sel.attrs:
        if key not in node.attrs:
            return False
        if expected is not None and node.attrs.get(key) != expected:
            return False

    return True


def _select_nodes(nodes: list[_HtmlNode], selector: str, *, contexts: Optional[list[int]] = None) -> list[int]:
    tokens = _split_selector(selector)
    if not tokens:
        return []

    chain: list[_SimpleSelector] = []
    for tok in tokens:
        parsed = _parse_simple_selector(tok)
        if parsed is None:
            return []
        chain.append(parsed)

    current = list(contexts) if contexts else [0]
    for step in chain:
        next_ids: list[int] = []
        seen: set[int] = set()
        for ctx in current:
            for node_id in _iter_descendants(nodes, ctx):
                if node_id in seen:
                    continue
                if _matches(nodes[node_id], step):
                    next_ids.append(node_id)
                    seen.add(node_id)
        current = next_ids
        if not current:
            break
    return current


def _parse_field_expr(expr: str) -> tuple[Optional[str], str, Optional[str]]:
    s = str(expr or "").strip()
    if not s:
        return None, "invalid", None

    selector: Optional[str]
    mode_raw: str
    if "::" in s:
        left, right = s.split("::", 1)
        selector = left.strip() or None
        mode_raw = right.strip()
    else:
        selector = s
        mode_raw = "text"

    if mode_raw == "text":
        return selector, "text", None

    m = _ATTR_MODE_RE.fullmatch(mode_raw)
    if m:
        attr_name = m.group(1).strip().strip('"').strip("'")
        if attr_name:
            return selector, "attr", attr_name.lower()

    return None, "invalid", None


def _extract_field_value(nodes: list[_HtmlNode], item_node_id: int, rule: Any) -> Optional[str]:
    def _one(expr: str) -> Optional[str]:
        selector, mode, attr_name = _parse_field_expr(expr)
        if mode == "invalid":
            return None
        targets = [item_node_id] if selector is None else _select_nodes(nodes, selector, contexts=[item_node_id])
        if not targets:
            return None
        node = nodes[targets[0]]
        if mode == "text":
            txt = _node_text(nodes, targets[0])
            return txt if txt else None
        if attr_name is None:
            return None
        raw = node.attrs.get(attr_name)
        if raw is None:
            return None
        out = _WS_RE.sub(" ", str(raw)).strip()
        return out if out else None

    if isinstance(rule, str):
        return _one(rule)
    if isinstance(rule, (list, tuple)):
        for part in rule:
            if not isinstance(part, str):
                continue
            got = _one(part)
            if got is not None and got != "":
                return got
    return None


def extract_items_from_html(html: str, spec: ExtractSpec) -> list[dict[str, Any]]:
    selector = str(getattr(spec, "html_items_selector", "") or "").strip()
    if not selector:
        return []

    nodes = _parse_html_nodes(html if isinstance(html, str) else "")
    item_nodes = _select_nodes(nodes, selector, contexts=[0])
    if not item_nodes:
        return []

    raw_fields = getattr(spec, "html_fields", {})
    fields: dict[str, Any] = raw_fields if isinstance(raw_fields, dict) else {}
    html_id_attr = str(getattr(spec, "html_id_attr", "") or "").strip().lower()
    out: list[dict[str, Any]] = []

    for node_id in item_nodes:
        row: dict[str, Any] = {}

        for key, rule in fields.items():
            if not isinstance(key, str) or not key.strip():
                continue
            val = _extract_field_value(nodes, node_id, rule)
            if val is not None and val != "":
                row[key] = val

        if html_id_attr and "id" not in row:
            v = nodes[node_id].attrs.get(html_id_attr)
            if isinstance(v, str) and v.strip():
                row["id"] = v.strip()

        links = _select_nodes(nodes, "a[href]", contexts=[node_id])
        if links:
            link_node = nodes[links[0]]
            href = link_node.attrs.get("href")
            if "url" not in row and isinstance(href, str) and href.strip():
                row["url"] = href.strip()
            if "title" not in row:
                title = _node_text(nodes, links[0])
                if title:
                    row["title"] = title

        if "text" not in row:
            txt = _node_text(nodes, node_id)
            if txt:
                row["text"] = txt

        if row:
            out.append(row)

    return out
