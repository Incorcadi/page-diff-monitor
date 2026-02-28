"""
site_profile.py вЂ” вЂњС‚РµС…РєР°СЂС‚Р°вЂќ СЃР°Р№С‚Р° + СЃС‚СЂСѓРєС‚СѓСЂС‹ РѕС‚С‡С‘С‚РѕРІ/СЃС‚СЂР°С‚РµРіРёР№ (СЏРґСЂРѕ РґР°РЅРЅС‹С…).

Р­С‚РѕС‚ РјРѕРґСѓР»СЊ СЃРїРµС†РёР°Р»СЊРЅРѕ РґРµСЂР¶РёС‚ РўРћР›Р¬РљРћ СЃС‚СЂСѓРєС‚СѓСЂС‹ РґР°РЅРЅС‹С… (dataclasses) Рё СЃРµСЂРёР°Р»РёР·Р°С†РёСЋ.
РћРЅ РЅРµ РґРµР»Р°РµС‚ HTTP, РЅРµ РїР°СЂСЃРёС‚ JSON Рё РЅРµ С…РѕРґРёС‚ РїРѕ СЃС‚СЂР°РЅРёС†Р°Рј. Р—Р°С‡РµРј С‚Р°Рє?

РџРѕС‚РѕРјСѓ С‡С‚Рѕ РґР»СЏ вЂњС„РµСЂРјС‹вЂќ СЃР°Р№С‚РѕРІ РєСЂРёС‚РёС‡РЅРѕ СЂР°Р·РґРµР»СЏС‚СЊ:
- Р”РђРќРќР«Р• (С‡С‚Рѕ РјС‹ Р·РЅР°РµРј Рѕ СЃР°Р№С‚Рµ Рё РєР°Рє РµРіРѕ РЅР°РґРѕ РєСЂСѓС‚РёС‚СЊ) -> SiteProfile
- Р›РћР“РРљРЈ (РєР°Рє СЌС‚Рѕ РѕР±РЅР°СЂСѓР¶РёС‚СЊ / РєР°Рє СЌС‚Рѕ РІС‹РїРѕР»РЅСЏС‚СЊ) -> infer.py / onboard.py / runtime.py

РљР»СЋС‡РµРІС‹Рµ СЃСѓС‰РЅРѕСЃС‚Рё
-----------------
1) SiteProfile
   вЂњРџР°СЃРїРѕСЂС‚/С‚РµС…РєР°СЂС‚Р°вЂќ СЃР°Р№С‚Р°:
   - РєСѓРґР° С…РѕРґРёС‚СЊ (url, method),
   - РєР°Рє С…РѕРґРёС‚СЊ (headers, timeout, base_params),
   - РєР°Рє Р»РёСЃС‚Р°С‚СЊ (pagination),
   - РєР°Рє РґРѕСЃС‚Р°РІР°С‚СЊ items Рё id (extract).

2) PaginationSpec
   вЂњРџР°РЅРµР»СЊ СѓРїСЂР°РІР»РµРЅРёСЏ РєРѕРЅРІРµР№РµСЂРѕРјвЂќ:
   - kind: page/offset/cursor_token/next_url/unknown
   - РёРјРµРЅР° РїР°СЂР°РјРµС‚СЂРѕРІ (page_param, offset_param, cursor_param, limit_param)
   - Р»РёРјРёС‚С‹/Р·Р°С‰РёС‚С‹ (limit, max_batches)

3) ExtractSpec
   вЂњРћСЃРЅР°СЃС‚РєР° Р·Р°С…РІР°С‚Р° РґРµС‚Р°Р»РµР№вЂќ:
   - items_path / items_keys / container_keys
   - id_path / id_keys

4) Strategy + РєРѕРЅРєСЂРµС‚РЅС‹Рµ СЃС‚СЂР°С‚РµРіРёРё
   Р РµР·СѓР»СЊС‚Р°С‚ РёРЅС„РµСЂРµРЅСЃР° (infer.py). Р­С‚Рѕ РќР• РїСЂРѕС„РёР»СЊ, Р° вЂњСЂРµС€РµРЅРёРµ РћРўРљвЂќ:
   - РєР°РєР°СЏ СЃС‚СЂР°С‚РµРіРёСЏ,
   - РїРѕС‡РµРјСѓ,
   - СЃ РєР°РєРёРјРё РїР°СЂР°РјРµС‚СЂР°РјРё.

5) ProbeReport / ProbeAttempt / BatchScore
   РћС‚С‡С‘С‚РЅРѕСЃС‚СЊ РёРЅС„РµСЂРµРЅСЃР°: РєР°РєРёРµ РїСЂРѕР±С‹ РґРµР»Р°Р»Рё Рё РїРѕС‡РµРјСѓ СЃРґРµР»Р°Р»Рё РІС‹РІРѕРґ.

Р¤РѕСЂРјР°С‚ JSON РїСЂРѕС„РёР»СЏ (РїСЂРёРјРµСЂ)
----------------------------
{
  "name": "siteA",
  "url": "https://api.site.com/items",
  "method": "GET",
  "timeout": 10.0,
  "headers": {"User-Agent": "..."},
  "base_params": {"q": "laptop"},
  "pagination": {
    "kind": "offset",
    "limit": 50,
    "limit_param": "limit",
    "max_batches": 200,
    "offset_param": "offset",
    "step": 50
  },
  "extract": {
    "items_path": "data.items",
    "id_path": "id"
  }
}
"""

from __future__ import annotations

from dataclasses import dataclass, field, asdict
from typing import Any, Optional, Union, Literal, Tuple, List, Iterable
import json
import os


# JSONType вЂ” вЂњJSON РєР°Рє РІ РѕС‚РІРµС‚Рµ APIвЂќ: Р»РёР±Рѕ dict, Р»РёР±Рѕ list РЅР° РІРµСЂС…РЅРµРј СѓСЂРѕРІРЅРµ.
JSONType = Union[dict[str, Any], list[Any]]

# PaginationKind вЂ” С‚РёРї РїР°РіРёРЅР°С†РёРё, РєРѕС‚РѕСЂС‹Р№ Р±СѓРґРµС‚ РёСЃРїРѕР»СЊР·РѕРІР°С‚СЊ runtime.
PaginationKind = Literal["page", "offset", "cursor_token", "next_url", "unknown"]


@dataclass
class ExtractSpec:
    """
    ExtractSpec вЂ” вЂњРѕСЃРЅР°СЃС‚РєР°вЂќ РґР»СЏ РёР·РІР»РµС‡РµРЅРёСЏ РґР°РЅРЅС‹С… РёР· JSON-РѕС‚РІРµС‚Р°.

    РћСЃРЅРѕРІРЅР°СЏ РёРґРµСЏ:
    - Р•СЃР»Рё С‚С‹ Р·РЅР°РµС€СЊ С‚РѕС‡РЅС‹Р№ РїСѓС‚СЊ Рє items (items_path) вЂ” РёСЃРїРѕР»СЊР·СѓР№ РµРіРѕ (СЃР°РјС‹Р№ РЅР°РґС‘Р¶РЅС‹Р№ РІР°СЂРёР°РЅС‚).
    - Р•СЃР»Рё С‚РѕС‡РЅРѕРіРѕ РїСѓС‚Рё РЅРµС‚ вЂ” extractor РїС‹С‚Р°РµС‚СЃСЏ РЅР°Р№С‚Рё СЃРїРёСЃРѕРє РїРѕ С‚РёРїРѕРІС‹Рј РєР»СЋС‡Р°Рј Рё РєРѕРЅС‚РµР№РЅРµСЂР°Рј.

    ID РІ РѕС‚РІРµС‚Рµ СЃРµСЂРІРµСЂР° вЂ” СЌС‚Рѕ СѓРЅРёРєР°Р»СЊРЅС‹Р№ РёРґРµРЅС‚РёС„РёРєР°С‚РѕСЂ Р·Р°РїРёСЃРё, С‚Рѕ РµСЃС‚СЊ вЂњСЃРµСЂРёР№РЅС‹Р№ РЅРѕРјРµСЂвЂќ
    РєРѕРЅРєСЂРµС‚РЅРѕРіРѕ РѕР±СЉРµРєС‚Р° (С‚РѕРІР°СЂР°, РїРѕСЃС‚Р°, РѕР±СЉСЏРІР»РµРЅРёСЏ), РєРѕС‚РѕСЂС‹Р№ СЃРµСЂРІРµСЂ РёСЃРїРѕР»СЊР·СѓРµС‚, С‡С‚РѕР±С‹ РѕС‚Р»РёС‡Р°С‚СЊ РѕРґРёРЅ 
    РѕР±СЉРµРєС‚ РѕС‚ РґСЂСѓРіРѕРіРѕ.
    Р’ вЂњС„РµСЂРјРµвЂќ СЃР°Р№С‚РѕРІ ID вЂ” СЌС‚Рѕ РѕРґРёРЅ РёР· СЃР°РјС‹С… РІР°Р¶РЅС‹С… СЃРёРіРЅР°Р»РѕРІ, РїРѕС‚РѕРјСѓ С‡С‚Рѕ РѕРЅ СЂРµС€Р°РµС‚ СЃСЂР°Р·Сѓ 3 Р·Р°РґР°С‡Рё:
    - Р”РµРґСѓРїР»РёРєР°С†РёСЏ (РЅРµ СЃРѕС…СЂР°РЅСЏС‚СЊ РѕРґРЅРѕ Рё С‚Рѕ Р¶Рµ 2 СЂР°Р·Р°)
    - РџСЂРѕРІРµСЂРєР°, С‡С‚Рѕ РїР°РіРёРЅР°С†РёСЏ СЂРµР°Р»СЊРЅРѕ РґРІРёРіР°РµС‚СЃСЏ
    - РћР±РЅРѕРІР»РµРЅРёСЏ - вЂњРўРѕС‚ Р¶Рµ ID, РЅРѕ РёР·РјРµРЅРёР»РёСЃСЊ РїРѕР»СЏ (С†РµРЅР°/РѕСЃС‚Р°С‚РѕРє)вЂќ в†’ Р·РЅР°С‡РёС‚ СЌС‚Рѕ РѕР±РЅРѕРІР»РµРЅРёРµ СЃСѓС‰РµСЃС‚РІСѓСЋС‰РµР№ Р·Р°РїРёСЃРё.
    Р’Р°Р¶РЅРѕ: ID Р±С‹РІР°РµС‚ СЂР°Р·РЅС‹Р№ (РЅРµ РїСѓС‚Р°Р№) 
    Р’ API С‡Р°СЃС‚Рѕ РІСЃС‚СЂРµС‡Р°СЋС‚СЃСЏ РµС‰С‘ вЂњРїРѕС…РѕР¶РёРµвЂќ РёРґРµРЅС‚РёС„РёРєР°С‚РѕСЂС‹:
    - request_id / trace_id вЂ” РёРґРµРЅС‚РёС„РёРєР°С‚РѕСЂ Р·Р°РїСЂРѕСЃР° (РґР»СЏ Р»РѕРіРѕРІ СЃРµСЂРІРµСЂР°). РћРЅ РјРµРЅСЏРµС‚СЃСЏ РєР°Р¶РґС‹Р№ Р·Р°РїСЂРѕСЃ Рё С‚РµР±Рµ РґР»СЏ РїР°СЂСЃРёРЅРіР° РїРѕС‡С‚Рё РЅРµ РЅСѓР¶РµРЅ
    - session_id вЂ” РёРґРµРЅС‚РёС„РёРєР°С‚РѕСЂ СЃРµСЃСЃРёРё РїРѕР»СЊР·РѕРІР°С‚РµР»СЏ (РєСѓРєРё).
    - session_id вЂ” РёРґРµРЅС‚РёС„РёРєР°С‚РѕСЂ СЃРµСЃСЃРёРё РїРѕР»СЊР·РѕРІР°С‚РµР»СЏ (РєСѓРєРё).
    РџРѕС‡РµРјСѓ РёРЅРѕРіРґР° ID вЂњРЅРµС‚вЂќ РРЅРѕРіРґР° API РѕС‚РґР°С‘С‚ СЃРїРёСЃРѕРє, РЅРѕ Р±РµР· СЏРІРЅРѕРіРѕ id. РўРѕРіРґР° РІР°СЂРёР°РЅС‚С‹:
    - РёСЃРєР°С‚СЊ РґСЂСѓРіРѕР№ СѓРЅРёРєР°Р»СЊРЅС‹Р№ РєР»СЋС‡: slug, url, code, article, sku
    - РµСЃР»Рё РЅРёС‡РµРіРѕ СѓРЅРёРєР°Р»СЊРЅРѕРіРѕ РЅРµС‚ вЂ” РґРµР»Р°С‚СЊ вЂњСЃР°РјРѕРґРµР»СЊРЅС‹Р№ IDвЂќ (РЅР°РїСЂРёРјРµСЂ, С…СЌС€/СЃРєР»РµР№РєР° РїРѕР»РµР№ РІСЂРѕРґРµ СЃР»Рё РЅРёС‡РµРіРѕ СѓРЅРёРєР°Р»СЊРЅРѕРіРѕ РЅРµС‚ вЂ” РґРµР»Р°С‚СЊ вЂњСЃР°РјРѕРґРµР»СЊРЅС‹Р№ IDвЂќ (РЅР°РїСЂРёРјРµСЂ, С…СЌС€/СЃРєР»РµР№РєР° РїРѕР»РµР№ РІСЂРѕРґРµ

    items_path / id_path РёСЃРїРѕР»СЊР·СѓСЋС‚ вЂњС‚РѕС‡РµС‡РЅС‹Р№ РїСѓС‚СЊвЂќ (dot-path): 
    Dot-path (С‚РѕС‡РµС‡РЅС‹Р№ РїСѓС‚СЊ) вЂ” СЌС‚Рѕ СЃС‚СЂРѕРєР° РІРёРґР° "a.b.0.c", РіРґРµ: Dot-path (С‚РѕС‡РµС‡РЅС‹Р№ РїСѓС‚СЊ) вЂ” СЌС‚Рѕ СЃС‚СЂРѕРєР° РІРёРґР° "a.b.0.c", РіРґРµ:
    С€Р°Рі РјРѕР¶РµС‚ Р±С‹С‚СЊ: РєР»СЋС‡РѕРј dict (СЃР»РѕРІР°СЂСЏ) вЂ” РЅР°РїСЂРёРјРµСЂ data,РёРЅРґРµРєСЃРѕРј list (СЃРїРёСЃРєР°) вЂ” РµСЃР»Рё С€Р°Рі РІС‹РіР»СЏРґРёС‚ РєР°Рє С‡РёСЃР»Рѕ, РЅР°РїСЂРёРјРµСЂ 0,
    - "data.items" Р·РЅР°С‡РёС‚ data["data"]["items"]
    - "payload.results.0" Р·РЅР°С‡РёС‚ payload["results"][0]
      (С†РёС„СЂРѕРІРѕР№ СЃРµРіРјРµРЅС‚ С‚СЂР°РєС‚СѓРµС‚СЃСЏ РєР°Рє РёРЅРґРµРєСЃ СЃРїРёСЃРєР°)
    РљР°Рє СЌС‚Рѕ СЃРІСЏР·Р°РЅРѕ СЃ ExtractSpec Р’ ExtractSpec РµСЃС‚СЊ РґРІР° С‚Р°РєРёС… РїСѓС‚Рё:
    - items_path вЂ” С‚РѕС‡РЅС‹Р№ РїСѓС‚СЊ Рє СЃРїРёСЃРєСѓ items РІ РѕС‚РІРµС‚Рµ
    - id_path вЂ” С‚РѕС‡РЅС‹Р№ РїСѓС‚СЊ Рє ID РІРЅСѓС‚СЂРё РѕРґРЅРѕРіРѕ item item = {"product": {"id": 777, "price": 10}} С‚Рѕ: id_path = "product.id" РѕР·РЅР°С‡Р°РµС‚ item["product"]["id"]

    РџРѕР»СЏ:
    - items_path: С‚РѕС‡РЅС‹Р№ РїСѓС‚СЊ Рє СЃРїРёСЃРєСѓ items (РµСЃР»Рё РёР·РІРµСЃС‚РµРЅ)
    - items_keys: С‚РёРїРѕРІС‹Рµ РєР»СЋС‡Рё, РїРѕРґ РєРѕС‚РѕСЂС‹РјРё С‡Р°СЃС‚Рѕ Р»РµР¶РёС‚ СЃРїРёСЃРѕРє СЃСѓС‰РЅРѕСЃС‚РµР№
    - container_keys: С‚РёРїРѕРІС‹Рµ РєРѕРЅС‚РµР№РЅРµСЂС‹, РєСѓРґР° С‡Р°СЃС‚Рѕ вЂњР·Р°РїР°РєРѕРІС‹РІР°СЋС‚вЂќ РїРѕР»РµР·РЅС‹Рµ РґР°РЅРЅС‹Рµ
    - max_depth: РЅР°СЃРєРѕР»СЊРєРѕ РіР»СѓР±РѕРєРѕ РїСЂРѕРІР°Р»РёРІР°С‚СЊСЃСЏ РїРѕ container_keys

    ID:
    - id_path: С‚РѕС‡РЅС‹Р№ РїСѓС‚СЊ Рє ID РІРЅСѓС‚СЂРё РѕРґРЅРѕРіРѕ item
    - id_keys: fallback-РєР»СЋС‡Рё, РµСЃР»Рё id_path РЅРµ Р·Р°РґР°РЅ id_keys вЂ” СЌС‚Рѕ вЂњР·Р°РїР°СЃРЅС‹Рµ РІР°СЂРёР°РЅС‚С‹, РіРґРµ РёСЃРєР°С‚СЊ IDвЂќ, РµСЃР»Рё С‚С‹ РЅРµ Р·РЅР°РµС€СЊ С‚РѕС‡РЅС‹Р№ РїСѓС‚СЊ id_path
      id_keys вЂ” СЌС‚Рѕ вЂњР·Р°РїР°СЃРЅС‹Рµ РІР°СЂРёР°РЅС‚С‹, РіРґРµ РёСЃРєР°С‚СЊ IDвЂќ, РµСЃР»Рё С‚С‹ РЅРµ Р·РЅР°РµС€СЊ С‚РѕС‡РЅС‹Р№ РїСѓС‚СЊ id_path
    """
    items_path: Optional[str] = None

    items_keys: tuple[str, ...] = ("items", "results", "data", "posts", "products", "rows", "list")
    container_keys: tuple[str, ...] = ("data", "result", "payload", "response", "meta", "pagination")
    max_depth: int = 2
    mode: Literal["json", "html", "auto"] = "json"
    html_items_selector: Optional[str] = None
    html_fields: dict[str, Any] = field(default_factory=dict)
    html_id_attr: Optional[str] = None

    id_path: Optional[str] = "id"
    id_keys: tuple[str, ...] = ("id", "uuid", "guid", "product_id", "item_id", "pk", "slug")

# РѕР·РЅР°С‡Р°РµС‚ Р·Р°РїСЂРµС‚ РЅР° РїСЂРёСЃРІР°РёРІР°РЅРёРµ Р°С‚СЂРёР±СѓС‚РѕРІ СЌРєР·РµРјРїР»СЏСЂСѓ РїРѕСЃР»Рµ СЃРѕР·РґР°РЅРёСЏ.
@dataclass
class PaginationSpec:
    """
    PaginationSpec вЂ” РЅР°СЃС‚СЂРѕР№РєРё РїР°РіРёРЅР°С†РёРё (РєР°Рє вЂњРєСЂСѓС‚РёС‚СЊ РєРѕРЅРІРµР№РµСЂвЂќ РЅР° СЃР°Р№С‚Рµ).

    РџРѕР»СЏ РѕР±С‰РµРіРѕ РЅР°Р·РЅР°С‡РµРЅРёСЏ:
    - kind:
        "page"         вЂ” page=1,2,3...
        "offset"       вЂ” offset=0,limit,2*limit...
        "cursor_token" вЂ” РєСѓСЂСЃРѕСЂ/С‚РѕРєРµРЅ (after/cursor/pageToken)
        "next_url"     вЂ” СЃРµСЂРІРµСЂ РѕС‚РґР°С‘С‚ РіРѕС‚РѕРІСѓСЋ СЃСЃС‹Р»РєСѓ вЂњnextвЂќ
        "unknown"      вЂ” РЅРµ РЅР°СЃС‚СЂРѕРµРЅРѕ/РЅРµ РЅР°Р№РґРµРЅРѕ
    - limit: СЂР°Р·РјРµСЂ вЂњРїР°СЂС‚РёРёвЂќ (СЃРєРѕР»СЊРєРѕ items С…РѕС‚РёРј Р·Р° Р·Р°РїСЂРѕСЃ)
    - limit_param: РєР°Рє РЅР°Р·С‹РІР°РµС‚СЃСЏ РїР°СЂР°РјРµС‚СЂ Р»РёРјРёС‚Р° РЅР° РєРѕРЅРєСЂРµС‚РЅРѕРј API
    - max_batches: Р·Р°С‰РёС‚РЅС‹Р№ РїСЂРµРґРµР», С‡С‚РѕР±С‹ РЅРµ СѓР№С‚Рё РІ Р±РµСЃРєРѕРЅРµС‡РЅС‹Р№ С†РёРєР»

    РЎС‚СЂР°С‚РµРіРёРё:
    - page: page_param, start_from
    - offset: offset_param, step (РµСЃР»Рё step==0 -> Р±РµСЂС‘Рј limit)
    - cursor: cursor_param (РёРјСЏ РїР°СЂР°РјРµС‚СЂР° РІ Р·Р°РїСЂРѕСЃРµ), cursor_field_hint (РіРґРµ РЅР°С€Р»Рё С‚РѕРєРµРЅ)
    - next_url: next_url_field_hint (РѕС‚РєСѓРґР° Р±СЂР°Р»Рё next: Link/json), С‡РёСЃС‚Рѕ РґРёР°РіРЅРѕСЃС‚РёС‡РµСЃРєРѕРµ
    """
    kind: PaginationKind = "unknown"

    limit: int = 20
    limit_param: Optional[str] = None
    max_batches: int = 200

    page_param: str = "page"
    start_from: int = 1

    offset_param: str = "offset"
    step: int = 0

    cursor_param: Optional[str] = None
    cursor_field_hint: Optional[str] = None

    next_url_field_hint: Optional[str] = None


@dataclass
class SiteProfile:
    """
    SiteProfile вЂ” РїСЂРѕС„РёР»СЊ (С‚РµС…РєР°СЂС‚Р°) РѕРґРЅРѕРіРѕ СЃР°Р№С‚Р°/API endpointвЂ™Р°.

    РџСЂР°РєС‚РёС‡РµСЃРєР°СЏ РјС‹СЃР»СЊ:
    - РџСЂРѕС„РёР»СЊ вЂ” СЌС‚Рѕ С‚Рѕ, С‡С‚Рѕ С‚С‹ Р±СѓРґРµС€СЊ С…СЂР°РЅРёС‚СЊ РІ JSON Рё РїСЂРёРјРµРЅСЏС‚СЊ РЅР° СЃРѕС‚РЅСЏС… СЃР°Р№С‚РѕРІ.
    - runtime.py РґРѕР»Р¶РµРЅ СЂР°Р±РѕС‚Р°С‚СЊ РїРѕ РїСЂРѕС„РёР»СЋ Рё РќР• СѓРіР°РґС‹РІР°С‚СЊ.

    РЎРµС‚РµРІС‹Рµ РїРѕР»СЏ:
    - url: Р±Р°Р·РѕРІС‹Р№ endpoint
    - method: GET/POST (РґР»СЏ РїСЂРѕСЃС‚РѕС‚С‹ СЃРµР№С‡Р°СЃ РїСЂРµРґРїРѕР»Р°РіР°РµРј query params)
    РџРћРЇРЎРќР•РќРРЇ РўРћР“Рћ Р§РўРћ Р’Р«РЁР•: Р”Р°Р¶Рµ РµСЃР»Рё method="POST", РєРѕРґ РІСЃС‘ СЂР°РІРЅРѕ РѕС‚РїСЂР°РІР»СЏРµС‚ РїР°РіРёРЅР°С†РёРѕРЅРЅС‹Рµ РїР°СЂР°РјРµС‚СЂС‹ РІ URL (С‡РµСЂРµР· params). Р­С‚Рѕ СЂРµР°Р»СЊРЅРѕ РІСЃС‚СЂРµС‡Р°РµС‚СЃСЏ РІ API: POST РёСЃРїРѕР»СЊР·СѓРµС‚СЃСЏ вЂњРїРѕ С‚СЂР°РґРёС†РёРёвЂќ РёР»Рё СЂР°РґРё Р°РІС‚РѕСЂРёР·Р°С†РёРё, РЅРѕ РїР°РіРёРЅР°С†РёСЏ РІСЃС‘ СЂР°РІРЅРѕ РІ URL.
    - timeout: С‚Р°Р№РјР°СѓС‚ Р·Р°РїСЂРѕСЃРѕРІ
    - headers: Р·Р°РіРѕР»РѕРІРєРё (UA, С‚РѕРєРµРЅ Рё С‚.Рї.)
    - base_params: Р±Р°Р·РѕРІС‹Рµ query РїР°СЂР°РјРµС‚СЂС‹ (РїРѕРёСЃРє, СЃРѕСЂС‚РёСЂРѕРІРєР°...)

    Р’Р»РѕР¶РµРЅРЅС‹Рµ СЃРїРµРєРё:
    - pagination: РЅР°СЃС‚СЂРѕР№РєРё РїР°РіРёРЅР°С†РёРё (PaginationSpec)
    - extract: РЅР°СЃС‚СЂРѕР№РєРё РёР·РІР»РµС‡РµРЅРёСЏ items/id (ExtractSpec)
    """
    name: str
    url: str
    method: str = "GET"
    timeout: float = 10.0
    headers: dict[str, str] = field(default_factory=dict)
    base_params: dict[str, Any] = field(default_factory=dict)


    meta: dict[str, Any] = field(default_factory=dict)
    pagination: PaginationSpec = field(default_factory=PaginationSpec)
    extract: ExtractSpec = field(default_factory=ExtractSpec)

    def to_dict(self) -> dict[str, Any]:
        """РЎРµСЂРёР°Р»РёР·Р°С†РёСЏ РІ РѕР±С‹С‡РЅС‹Р№ dict (JSON-friendly).

        РЎРѕРІРјРµСЃС‚РёРјРѕСЃС‚СЊ:
        - "СЃР»СѓР¶РµР±РЅС‹Рµ" РјРµС‚Р°РґР°РЅРЅС‹Рµ РґРµСЂР¶РёРј РІ РєР»СЋС‡Рµ `_meta`
        - РЅРѕ РЅР° РїРµСЂРµС…РѕРґРЅС‹Р№ РїРµСЂРёРѕРґ РїРёС€РµРј С‚Р°РєР¶Рµ `meta`, РµСЃР»Рё РіРґРµ-С‚Рѕ СЃС‚Р°СЂС‹Р№ РєРѕРґ
          С‡РёС‚Р°РµС‚ РёРјРµРЅРЅРѕ `meta`
        """
        d = asdict(self)
        meta = d.pop("meta", None) or {}
        d["_meta"] = meta
        d["meta"] = meta  # alias for backward compatibility
        return d

    @staticmethod
    def from_dict(d: dict[str, Any]) -> "SiteProfile":
        """
        Р”РµСЃРµСЂРёР°Р»РёР·Р°С†РёСЏ РёР· dict.

        Р—РґРµСЃСЊ РµСЃС‚СЊ РІР°Р¶РЅС‹Р№ РјРѕРјРµРЅС‚: РІ JSON СЃРїРёСЃРєРё РєР»СЋС‡РµР№ РѕР±С‹С‡РЅРѕ С…СЂР°РЅСЏС‚СЃСЏ РєР°Рє list,
        РЅРѕ РІ РєРѕРґРµ РјС‹ РёСЃРїРѕР»СЊР·СѓРµРј tuple[str,...]. РџРѕСЌС‚РѕРјСѓ Р°РєРєСѓСЂР°С‚РЅРѕ РєРѕРЅРІРµСЂС‚РёСЂСѓРµРј.
        """
        pag = d.get("pagination", {}) or {}
        ext = d.get("extract", {}) or {}

        meta_a = d.get("meta") or {}
        meta_b = d.get("_meta") or {}
        if not isinstance(meta_a, dict):
            meta_a = {}
        if not isinstance(meta_b, dict):
            meta_b = {}
        meta = _deep_merge(dict(meta_a), dict(meta_b))

        def to_tuple(x, default: tuple[str, ...]) -> tuple[str, ...]:
            if x is None:
                return tuple(default)
            if isinstance(x, (list, tuple)):
                return tuple(x)
            return tuple(default)

        mode = str(ext.get("mode", "json") or "json").lower()
        if mode not in ("json", "html", "auto"):
            mode = "json"

        html_items_selector = ext.get("html_items_selector")
        if not isinstance(html_items_selector, str) or not html_items_selector.strip():
            html_items_selector = None

        html_id_attr = ext.get("html_id_attr")
        if not isinstance(html_id_attr, str) or not html_id_attr.strip():
            html_id_attr = None

        html_fields: dict[str, Any] = {}
        raw_html_fields = ext.get("html_fields")
        if isinstance(raw_html_fields, dict):
            for k, v in raw_html_fields.items():
                if not isinstance(k, str) or not k.strip():
                    continue
                if isinstance(v, str) and v.strip():
                    html_fields[k] = v
                    continue
                if isinstance(v, (list, tuple)):
                    vv = [x for x in v if isinstance(x, str) and x.strip()]
                    if vv:
                        html_fields[k] = vv

        return SiteProfile(
            name=d.get("name", "unnamed"),
            url=d["url"],
            method=d.get("method", "GET"),
            timeout=float(d.get("timeout", 10.0)),
            headers=dict(d.get("headers", {}) or {}),
            base_params=dict(d.get("base_params", {}) or {}),
            meta=meta,
            pagination=PaginationSpec(
                kind=pag.get("kind", "unknown"),
                limit=int(pag.get("limit", 20)),
                limit_param=pag.get("limit_param"),
                max_batches=int(pag.get("max_batches", 200)),
                page_param=pag.get("page_param", "page"),
                start_from=int(pag.get("start_from", 1)),
                offset_param=pag.get("offset_param", "offset"),
                step=int(pag.get("step", 0)),
                cursor_param=pag.get("cursor_param"),
                cursor_field_hint=pag.get("cursor_field_hint"),
                next_url_field_hint=pag.get("next_url_field_hint"),
            ),
            extract=ExtractSpec(
                items_path=ext.get("items_path"),
                items_keys=to_tuple(ext.get("items_keys"), ("items", "results", "data", "posts", "products", "rows", "list")),
                container_keys=to_tuple(ext.get("container_keys"), ("data", "result", "payload", "response", "meta", "pagination")),
                max_depth=int(ext.get("max_depth", 2)),
                mode=mode,  # type: ignore[arg-type]
                html_items_selector=html_items_selector,
                html_fields=html_fields,
                html_id_attr=html_id_attr,
                id_path=ext.get("id_path", "id"),
                id_keys=to_tuple(ext.get("id_keys"), ("id", "uuid", "guid", "product_id", "item_id", "pk", "slug")),
            ),
        )

    @staticmethod
    def from_json_file(path: str) -> "SiteProfile":
        """Р—Р°РіСЂСѓР·РёС‚СЊ РїСЂРѕС„РёР»СЊ РёР· JSON-С„Р°Р№Р»Р°."""
        with open(path, "r", encoding="utf-8") as f:
            return SiteProfile.from_dict(json.load(f))

    def save_json(self, path: str) -> None:
        """РЎРѕС…СЂР°РЅРёС‚СЊ РїСЂРѕС„РёР»СЊ РІ JSON-С„Р°Р№Р»."""
        with open(path, "w", encoding="utf-8") as f:
            json.dump(self.to_dict(), f, ensure_ascii=False, indent=2)


# =========================
# РЎС‚СЂР°С‚РµРіРёРё (СЂРµР·СѓР»СЊС‚Р°С‚ infer.py)
# =========================

@dataclass(frozen=True)
class Strategy:
    """
    Strategy вЂ” Р±Р°Р·РѕРІС‹Р№ РєР»Р°СЃСЃ СЂРµС€РµРЅРёСЏ РёРЅС„РµСЂРµРЅСЃР°.

    kind вЂ” РІС‹Р±СЂР°РЅРЅС‹Р№ С‚РёРї РїР°РіРёРЅР°С†РёРё,
    detail вЂ” С‡РµР»РѕРІРµРєРѕС‡РёС‚Р°РµРјРѕРµ РѕР±СЉСЏСЃРЅРµРЅРёРµ вЂњРїРѕС‡РµРјСѓ С‚Р°РєвЂќ.
    """
    kind: PaginationKind
    detail: str


@dataclass(frozen=True)
class PageStrategy(Strategy):
    """РЎС‚СЂР°С‚РµРіРёСЏ page-РїР°РіРёРЅР°С†РёРё (page=...)."""
    page_param: str
    start_from: int
    limit_param: Optional[str]
    limit: int


@dataclass(frozen=True)
class OffsetStrategy(Strategy):
    """РЎС‚СЂР°С‚РµРіРёСЏ offset-РїР°РіРёРЅР°С†РёРё (offset=...)."""
    offset_param: str
    step: int
    limit_param: Optional[str]
    limit: int


@dataclass(frozen=True)
class CursorTokenStrategy(Strategy):
    """РЎС‚СЂР°С‚РµРіРёСЏ РєСѓСЂСЃРѕСЂРЅРѕР№ РїР°РіРёРЅР°С†РёРё (cursor/after/pageToken)."""
    cursor_value: str
    cursor_source_field: str
    cursor_param: Optional[str]
    limit_param: Optional[str]
    limit: int


@dataclass(frozen=True)
class NextUrlStrategy(Strategy):
    """РЎС‚СЂР°С‚РµРіРёСЏ next_url: СЃРµСЂРІРµСЂ РѕС‚РґР°С‘С‚ РіРѕС‚РѕРІСѓСЋ СЃСЃС‹Р»РєСѓ СЃР»РµРґСѓСЋС‰РµР№ СЃС‚СЂР°РЅРёС†С‹."""
    next_url: str


def apply_strategy_to_profile(profile: SiteProfile, strat: Strategy) -> SiteProfile:
    """Apply inferred strategy to profile pagination fields."""
    profile.pagination.kind = strat.kind

    if strat.kind == "offset":
        profile.pagination.offset_param = getattr(strat, "offset_param", profile.pagination.offset_param)
        profile.pagination.step = getattr(strat, "step", profile.pagination.limit)

    elif strat.kind == "page":
        profile.pagination.page_param = getattr(strat, "page_param", profile.pagination.page_param)
        profile.pagination.start_from = getattr(strat, "start_from", profile.pagination.start_from)

    elif strat.kind == "cursor_token":
        profile.pagination.cursor_param = getattr(strat, "cursor_param", profile.pagination.cursor_param)
        profile.pagination.cursor_field_hint = getattr(strat, "cursor_source_field", None)

    elif strat.kind == "next_url":
        profile.pagination.next_url_field_hint = getattr(strat, "detail", "link_or_json")

    return profile


# =========================
# РћС‚С‡С‘С‚С‹ РїСЂРѕР± (РґР»СЏ РїСЂРѕР·СЂР°С‡РЅРѕСЃС‚Рё СЂРµС€РµРЅРёР№)
# =========================

@dataclass(frozen=True)
class BatchScore:
    """
    BatchScore вЂ” РѕС†РµРЅРєР° вЂњРґРІРёР¶РµРЅРёСЏ РїР°СЂС‚РёРёвЂќ РїРѕ ID.

    rank вЂ” РєРѕСЂС‚РµР¶ РґР»СЏ СЃСЂР°РІРЅРµРЅРёСЏ РїРѕРїС‹С‚РѕРє:
      (quality, fresh, -overlap, -size_penalty)
    РіРґРµ:
      quality: 2 = С…РѕСЂРѕС€Рѕ, 1 = СЃР»Р°Р±РѕРµ РґРІРёР¶РµРЅРёРµ, 0 = СЃРѕРјРЅРёС‚РµР»СЊРЅРѕ, -1 = РїР»РѕС…РѕР№ РїСЂРёР·РЅР°Рє
      fresh: СЃРєРѕР»СЊРєРѕ РЅРѕРІС‹С… ID РїРѕСЏРІРёР»РѕСЃСЊ
      overlap: СЃРєРѕР»СЊРєРѕ РїРµСЂРµСЃРµС‡РµРЅРёР№ СЃ РїСЂРµРґС‹РґСѓС‰РµР№ РїР°СЂС‚РёРµР№
      size_penalty: РЅР°СЃРєРѕР»СЊРєРѕ СЂР°Р·РјРµСЂ РїР°СЂС‚РёРё РґР°Р»С‘Рє РѕС‚ РѕР¶РёРґР°РµРјРѕРіРѕ limit

    reason вЂ” РєРѕСЂРѕС‚РєР°СЏ РїСЂРёС‡РёРЅР° (РґР»СЏ РѕС‚С‡С‘С‚Р°/Р»РѕРіРѕРІ).
    """
    rank: Tuple[int, int, int, int]
    reason: str
    fresh: int
    overlap: int
    size: int


@dataclass(frozen=True)
class ProbeAttempt:
    """
    ProbeAttempt вЂ” РѕРґРЅР° РїСЂРѕР±Р° (РѕРґРёРЅ С‚РµСЃС‚РѕРІС‹Р№ Р·Р°РїСЂРѕСЃ) РІ infer.py.

    ok:
      True  -> Р·Р°РїСЂРѕСЃ СѓСЃРїРµС€РµРЅ Рё JSON РїРѕР»СѓС‡РµРЅ
      False -> РѕС€РёР±РєР° СЃРµС‚Рё/HTTP/not_json

    score:
      None -> РµСЃР»Рё ok=False РёР»Рё РјС‹ РЅРµ СЃРјРѕРіР»Рё РєРѕСЂСЂРµРєС‚РЅРѕ РѕС†РµРЅРёС‚СЊ РґРІРёР¶РµРЅРёРµ РїР°СЂС‚РёРё
      BatchScore -> РµСЃР»Рё СѓРґР°Р»РѕСЃСЊ РёР·РІР»РµС‡СЊ items/id Рё РїРѕСЃС‡РёС‚Р°С‚СЊ РјРµС‚СЂРёРєРё
    """
    name: str
    url_used: str
    params_used: dict[str, Any]
    ok: bool
    status: Optional[int]
    error: Optional[str]
    score: Optional[BatchScore]


@dataclass(frozen=True)
class ProbeReport:
    """
    ProbeReport вЂ” РѕР±С‰РёР№ РѕС‚С‡С‘С‚ РёРЅС„РµСЂРµРЅСЃР°.

    base_* вЂ” РґР°РЅРЅС‹Рµ Р±Р°Р·РѕРІРѕРіРѕ Р·Р°РїСЂРѕСЃР° (Р±РµР· вЂњСЃРґРІРёРіР°вЂќ):
      base_url, base_params, base_status, base_error, base_ids_count

    attempts вЂ” СЃРїРёСЃРѕРє РїСЂРѕР±РЅС‹С… Р·Р°РїСЂРѕСЃРѕРІ (ProbeAttempt).
    """
    base_url: str
    base_params: dict[str, Any]
    base_status: Optional[int]
    base_error: Optional[str]
    base_ids_count: int
    attempts: List[ProbeAttempt]

# =========================
# РџСЂРѕС„РёР»СЊ IO: defaults / extends
# =========================

def _deep_merge(base: dict[str, Any], override: dict[str, Any]) -> dict[str, Any]:
    """
    Р“Р»СѓР±РѕРєРѕРµ СЃР»РёСЏРЅРёРµ СЃР»РѕРІР°СЂРµР№:
    - dict + dict -> СЂРµРєСѓСЂСЃРёРІРЅРѕ
    - РѕСЃС‚Р°Р»СЊРЅС‹Рµ С‚РёРїС‹ -> override Р·Р°РјРµРЅСЏРµС‚ base

    Р—Р°С‡РµРј:
    - С‡С‚РѕР±С‹ РѕР±С‰РёР№ _defaults.json Р·Р°РґР°РІР°Р» "СЂР°РјРєСѓ" (headers/timeout/items_keys),
      Р° РєРѕРЅРєСЂРµС‚РЅС‹Р№ РїСЂРѕС„РёР»СЊ РїРµСЂРµРѕРїСЂРµРґРµР»СЏР» С‚РѕР»СЊРєРѕ РѕС‚Р»РёС‡РёСЏ.
    """
    out: dict[str, Any] = dict(base)
    for k, v in (override or {}).items():
        if k in out and isinstance(out[k], dict) and isinstance(v, dict):
            out[k] = _deep_merge(out[k], v)
        else:
            out[k] = v
    return out


def _load_json_dict(path: str) -> dict[str, Any]:
    with open(path, "r", encoding="utf-8") as f:
        obj = json.load(f)
    if not isinstance(obj, dict):
        raise ValueError(f"profile must be dict JSON: {path}")
    return obj


# =========================
# Site patches (overlay layer): merge + set(dot-path) + delete(dot-path)
# =========================

def _dot_split(path: str) -> list[str]:
    p = str(path or "").strip()
    if not p:
        raise ValueError("dot-path is empty")
    return [seg for seg in p.split(".") if seg]


def _dot_set(d: dict[str, Any], path: str, value: Any) -> None:
    parts = _dot_split(path)
    cur: Any = d
    for seg in parts[:-1]:
        if not isinstance(cur, dict):
            raise ValueError(f"dot-set failed: parent is not dict (path={path!r})")
        nxt = cur.get(seg)
        if nxt is None:
            nxt = {}
            cur[seg] = nxt
        elif not isinstance(nxt, dict):
            raise ValueError(f"dot-set failed: segment {seg!r} is not dict (path={path!r})")
        cur = nxt
    if not isinstance(cur, dict):
        raise ValueError(f"dot-set failed: parent is not dict (path={path!r})")
    cur[parts[-1]] = value


def _dot_delete(d: dict[str, Any], path: str) -> None:
    parts = _dot_split(path)
    cur: Any = d
    for seg in parts[:-1]:
        if not isinstance(cur, dict):
            return
        cur = cur.get(seg)
        if cur is None:
            return
    if isinstance(cur, dict):
        cur.pop(parts[-1], None)


def apply_site_patch(profile_dict: dict[str, Any], patch_dict: dict[str, Any]) -> dict[str, Any]:
    """
    Apply one site patch overlay.

    Patch format (see README):
      - enabled: bool (optional, default True)
      - merge: dict -> deep merge into profile
      - set: { "a.b.c": value, ... } -> assign by dot-path
      - delete: ["a.b.c", ...] -> delete by dot-path (no-op if missing)

    Order: merge -> set -> delete.
    """
    if not isinstance(patch_dict, dict):
        raise ValueError("patch must be a dict JSON object")

    if patch_dict.get("enabled") is False:
        return dict(profile_dict)

    out: dict[str, Any] = dict(profile_dict)

    merge_part = patch_dict.get("merge") or {}
    if merge_part:
        if not isinstance(merge_part, dict):
            raise ValueError("patch.merge must be a dict")
        out = _deep_merge(out, merge_part)

    set_part = patch_dict.get("set") or {}
    if set_part:
        if not isinstance(set_part, dict):
            raise ValueError("patch.set must be a dict of dot-path -> value")
        for k, v in set_part.items():
            if isinstance(k, str) and k.strip():
                _dot_set(out, k, v)

    del_part = patch_dict.get("delete") or []
    if del_part:
        if not isinstance(del_part, list):
            raise ValueError("patch.delete must be a list of dot-path strings")
        for p in del_part:
            if isinstance(p, str) and p.strip():
                _dot_delete(out, p)

    return out


def _resolve_patch_path(spec: str, *, patches_dir: Optional[str], base_dir: str) -> str:
    """
    Patch spec can be:
      - explicit JSON path (endswith .json): relative -> base_dir
      - patch name without extension: "example_site" -> <patches_dir>/example_site.patch.json
        (if patches_dir omitted -> base_dir/example_site.patch.json)
    """
    s = str(spec or "").strip()
    if not s:
        raise ValueError("empty patch spec")

    if s.lower().endswith(".json"):
        p = s
        if not os.path.isabs(p):
            p = os.path.normpath(os.path.join(base_dir, p))
        return p

    fname = f"{s}.patch.json"
    if patches_dir and str(patches_dir).strip():
        pd = patches_dir
        if not os.path.isabs(pd):
            pd = os.path.normpath(os.path.join(base_dir, pd))
        return os.path.join(pd, fname)

    return os.path.normpath(os.path.join(base_dir, fname))


def apply_site_patches(
    profile_dict: dict[str, Any],
    *,
    site_patches: Optional[Iterable[str]],
    patches_dir: Optional[str],
    base_dir: str,
) -> dict[str, Any]:
    out = dict(profile_dict)
    for spec in (site_patches or []):
        path = _resolve_patch_path(str(spec), patches_dir=patches_dir, base_dir=base_dir)
        patch = _load_json_dict(path)
        out = apply_site_patch(out, patch)
    return out

def load_profile(
    path: str,
    *,
    defaults_path: str | None = None,
    base_dir: str | None = None,
    site_patches: Optional[List[str]] = None,
    patches_dir: str | None = None,
) -> SiteProfile:
    """
    Р—Р°РіСЂСѓР·РёС‚СЊ РїСЂРѕС„РёР»СЊ РёР· JSON СЃ РїРѕРґРґРµСЂР¶РєРѕР№:
    - defaults_path (РѕР±С‰РёР№ defaults-С„Р°Р№Р»)
    - extends (РІРЅСѓС‚СЂРё РїСЂРѕС„РёР»СЏ): СЃРїРёСЃРѕРє/СЃС‚СЂРѕРєР° РїСѓС‚РµР№ Рє Р±Р°Р·РѕРІС‹Рј РїСЂРѕС„РёР»СЏРј

    РџРѕСЂСЏРґРѕРє СЃР»РёСЏРЅРёСЏ:
      defaults -> extends[0] -> extends[1] -> ... -> СЃР°Рј РїСЂРѕС„РёР»СЊ
    """
    if base_dir is None:
        base_dir = os.path.dirname(path) or "."

    raw = _load_json_dict(path)

    merged: dict[str, Any] = {}
    if defaults_path:
        merged = _deep_merge(merged, _load_json_dict(defaults_path))

    extends = raw.get("extends") or raw.get("_extends")
    if extends:
        if isinstance(extends, str):
            extends_list = [extends]
        elif isinstance(extends, list):
            extends_list = [x for x in extends if isinstance(x, str)]
        else:
            extends_list = []
        for rel in extends_list:
            p = rel
            if not os.path.isabs(p):
                p = os.path.normpath(os.path.join(base_dir, rel))
            merged = _deep_merge(merged, _load_json_dict(p))

    # РїСЂРѕС„РёР»СЊ РїРѕРІРµСЂС… РІСЃРµРіРѕ
    raw2 = dict(raw)
    raw2.pop("extends", None)
    raw2.pop("_extends", None)
    merged = _deep_merge(merged, raw2)

    # apply site-specific patch overlays (if any)
    merged = apply_site_patches(
        merged,
        site_patches=site_patches,
        patches_dir=patches_dir,
        base_dir=base_dir,
    )

    return SiteProfile.from_dict(merged)


def save_profile(profile: SiteProfile, path: str, *, pretty: bool = True) -> None:
    with open(path, "w", encoding="utf-8") as f:
        json.dump(profile.to_dict(), f, ensure_ascii=False, indent=(2 if pretty else None))

