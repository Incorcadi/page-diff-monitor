from __future__ import annotations

from pathlib import Path

from web_farm.site_profile import ExtractSpec
from web_farm.extractors import ids_of
from web_farm.storage_jsonl import extract_item_id, make_item_key


def test_ids_of_works_with_id_path_dotpath():
    items = [
        {"meta": {"id": 123}, "title": "A"},
        {"meta": {"id": "xyz"}, "title": "B"},
    ]
    spec = ExtractSpec(items_path="items", id_path="meta.id")
    got = ids_of(items, spec)
    assert got == {"123", "xyz"}


def test_extract_item_id_matches_ids_of_first_item():
    item = {"x": {"uuid": "u-777"}}
    spec = ExtractSpec(items_path="items", id_path=None, id_keys=("x.uuid", "id"))
    s = extract_item_id(item, spec)
    assert s == "u-777"

    got = ids_of([item], spec)
    assert got == {"u-777"}


def test_make_item_key_falls_back_to_stable_hash_when_no_id():
    # одинаковый смысл, разный порядок ключей -> ключ должен совпадать
    item1 = {"b": 2, "a": 1, "nested": {"z": 9, "y": 8}}
    item2 = {"a": 1, "nested": {"y": 8, "z": 9}, "b": 2}
    spec = ExtractSpec(items_path="items", id_path=None, id_keys=("id",))

    k1 = make_item_key(item1, spec)
    k2 = make_item_key(item2, spec)

    assert k1.startswith("sha1:")
    assert k1 == k2
