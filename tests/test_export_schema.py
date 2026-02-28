import csv
from pathlib import Path
from web_farm import export_csv


def test_export_columns_schema_jsonl(tmp_path: Path):
    # simple JSONL fixture created on the fly
    in_jsonl = tmp_path / "sample.jsonl"
    in_jsonl.write_text(
        '{"meta": {"id": "a-1"}, "price": {"value": "10 000"}, "arr": [{"v": 7}], "title": "Hello"}\n'
        '{"meta": {"id": "a-2"}, "price": {"value": 2500}, "arr": [{"v": "8"}], "title": "World"}\n',
        encoding="utf-8",
    )

    out_csv = tmp_path / "out.csv"

    columns = [
        {"name": "id", "path": "meta.id", "type": "str"},
        {"name": "price_int", "path": "price.value", "type": "int"},
        {"name": "v0", "path": "arr.0.v", "type": "int"},
        {"name": "raw", "path": "", "type": "json"},
    ]

    rep = export_csv.jsonl_to_csv(str(in_jsonl), str(out_csv), columns=columns)
    assert rep["rows"] == 2

    rows = list(csv.DictReader(out_csv.open("r", encoding="utf-8")))
    assert rows[0]["id"] == "a-1"
    assert rows[0]["price_int"] == "10000"
    assert rows[0]["v0"] == "7"
    assert rows[0]["raw"].startswith("{")
