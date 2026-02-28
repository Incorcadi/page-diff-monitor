# Snapshot (fixtures) + auto-cases + from-cache

В `tool_pipeline.py` добавлена команда:

## snapshot
Сохраняет ответ как офлайн-фикстуру с человеческим именем и печатает/создаёт кейсы для offline-test.

### Live (сеть)
python tool_pipeline.py snapshot --profile profiles/site.json --name list_page_1 --write-case

### From-cache (без сети)
1) Сначала собери кэш (любой run/triage/diagnose/snapshot) с --cache-dir:
python tool_pipeline.py triage --profile profiles/site.json --smoke 1 --cache-dir .cache

2) Потом снимай фикстуру без сети:
python tool_pipeline.py snapshot --profile profiles/site.json --name list_page_1 --from-cache --cache-dir .cache --write-case

### Поля для проверок (кейсы в профиле)
--schema default
--items-min 20
--unique-ids-min 10
--col-nonempty item_id --col-nonempty price
--min-nonempty-ratio 0.6

Файлы сохраняются в:
- _meta.tests.fixtures_dir (если задано)
- иначе tests/fixtures
