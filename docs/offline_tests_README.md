# Offline test-runner (fixtures + _meta.tests)

## Что даёт

- **Быстрый контроль качества** профиля и контракта экспорта **без сети**.
- Можно править ядро (extractors/json_path/keying/export) и не бояться, что «упадут все профили».

## Как включить

1) Положи файл `offline_tests.py` рядом с `tool_pipeline_read.py` (или в пакет `scripts/scripts_pagination/`).

2) В `tool_pipeline_read.py` уже добавлена команда:

```bash
python tool_pipeline_read.py offline-test --profile profiles/olx_api.json --defaults profiles/_defaults.json
```

## Как описывать тесты прямо в профиле

В профиль добавь:

```json
{
  "_meta": {
    "tests": {
      "fixtures_dir": "tests/fixtures/olx_api",
      "cases": [
        {
          "name": "list_page_1",
          "file": "list_page_1.json",
          "kind": "json",
          "assert": {
            "items_min": 20,
            "unique_ids_min": 10,
            "schema": "default",
            "columns_nonempty": ["item_key", "item_id"],
            "min_nonempty_ratio": 0.5
          }
        }
      ]
    }
  }
}
```

### Примечания

- Если `cases` не задан — runner возьмёт **все `*.json`** из `fixtures_dir` и прогонит как отдельные кейсы.
- `schema` берётся из кейса, иначе из `_meta.export.default_schema`, иначе `default`.
- `columns_nonempty` — колонки, которые должны быть заполнены «достаточно часто».

## CLI опции

- `--fixtures-dir DIR` — переопределить `fixtures_dir` из профиля.
- `--case NAME` — прогнать только один кейс.
- `--schema NAME` — переопределить schema для всех кейсов.
- `--max-items N` — сколько items проверять на кейс (ускоряет тесты).
- `--json` — вывести отчёт в JSON.

