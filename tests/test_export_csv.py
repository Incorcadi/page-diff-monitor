from __future__ import annotations

import csv
from pathlib import Path

    
from web_farm import export_csv


def test_export_csv_supports_dotpath_fields(tmp_path: Path):
    in_jsonl = tmp_path / "sample.jsonl"
    in_jsonl.write_text(
        '{"id": "1", "meta": {"id": "a-1", "tags": ["x","y"]}, "arr": [{"v": 7}], "price": {"value": "10 000"}}\n'
        '{"id": "2", "meta": {"id": "a-2", "tags": []}, "arr": [{"v": "8"}], "price": {"value": 2500}}\n',
        encoding="utf-8",
    )
    out_csv = tmp_path / "out.csv"

    rep = export_csv.jsonl_to_csv(
        str(in_jsonl),
        str(out_csv),
        fields=["id", "meta.id", "arr.0.v", "price.value", "meta.tags"],
    )
    assert rep["rows"] == 2
    assert rep["fields"] == ["id", "meta.id", "arr.0.v", "price.value", "meta.tags"]

    with open(out_csv, "r", encoding="utf-8") as f:
        rows = list(csv.DictReader(f))

    assert rows[0]["id"] == "1"
    assert rows[0]["meta.id"] == "a-1"
    assert rows[0]["arr.0.v"] == "7"
    # price.value пока строкой (типизация добавим позже)
    assert rows[0]["price.value"] == "10 000"
    # список сериализуется в JSON-строку
    assert rows[0]["meta.tags"].startswith("[") and rows[0]["meta.tags"].endswith("]")


def test_export_csv_stringifies_objects(tmp_path: Path):
    in_jsonl = tmp_path / "x.jsonl"
    in_jsonl.write_text('{"a": {"b": 1}, "c": [1,2]}\n', encoding="utf-8")
    out_csv = tmp_path / "x.csv"

    export_csv.jsonl_to_csv(str(in_jsonl), str(out_csv), fields=["a", "c"])

    with open(out_csv, "r", encoding="utf-8") as f:
        rows = list(csv.DictReader(f))

    assert rows[0]["a"].startswith("{") and rows[0]["a"].endswith("}")
    assert rows[0]["c"].startswith("[") and rows[0]["c"].endswith("]")
