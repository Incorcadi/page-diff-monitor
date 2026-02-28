# web_farm — profile‑driven web/API scraping pipeline

[Українська](README.uk.md) · [Русский](README.ru.md)

`web_farm` is a **profile-driven** scraping pipeline intended for real freelance work: pagination, extraction, deduplication, offline replay/tests, and convenient exports.

## What you get

- **Profiles (JSON)** describe: request, pagination, extraction rules, keying/IDs, export schema.
- HTTP engine with **rate limiting**, **retries/backoff**, and `Retry-After` handling.
- Pagination kinds: `page`, `offset`, `cursor_token`, `next_url`, `cursor_next`.
- Outputs:
  - **JSONL** stream (`run`, `farm`)
  - **SQLite** store with `items_raw`, `items_unique`, `run_state`, and **blocked_events** queue (`run-sqlite`, `farm-sqlite`)
- “No-network” workflows:
  - snapshot responses into **fixtures**
  - run **offline-test** on fixtures
  - replay from cache (`--replay`)

## Install

```bash
python -m venv .venv
# activate venv
pip install -e .
```

Optional Playwright fallback (when HTTP is not enough):

```bash
pip install -e ".[browser]"
playwright install
```

## 1‑command demo (no network)

Export a tiny sample JSONL into CSV:

```bash
web-farm export --in examples/sample.jsonl --out out.csv \
  --fields id,meta.id,price.value,price.currency,arr.0.v
```

You’ll get `out.csv` with flattened dot-path fields.

## Portfolio demo (no network, fixtures)

Run bundled demo profiles **without network** and generate ready-to-show artifacts in `out/`:

```bash
web-farm demo --name all
```

This will:
- run `offline-test` for each demo profile
- write `out/<profile>.example.jsonl`
- export `out/<profile>.<schema>.csv` using the profile’s export schema

## CLI overview

> Tip: `web-farm <command> --help` shows all flags.

- Validate profile (static, no network):
  ```bash
  web-farm lint --profile path/to/profile.json
  ```

- Run one profile → JSONL:
  ```bash
  web-farm run --profile path/to/profile.json --out results.jsonl
  ```

- Run one profile → SQLite (raw+unique+state+blocked queue):
  ```bash
  web-farm run-sqlite --profile path/to/profile.json --db results.db --resume
  ```

- Export JSONL/SQLite → CSV:
  ```bash
  web-farm export --in results.db --kind sqlite --table items_unique --out results.csv
  ```

- Snapshot (save responses as fixtures) + offline-test:
  ```bash
  web-farm snapshot --profile path/to/profile.json --name case1 --batches 3 --write-case
  web-farm offline-test --profile path/to/profile.json
  ```

- Farm: run all profiles in a directory:
  ```bash
  web-farm farm --profiles-dir profiles/ --out-dir out/
  ```

## Secrets and auth

Secrets are stored **outside git** and referenced from profiles (by `ref` / `by_domain`).

- set via `PARSER_SECRETS_PATH`, or `--secrets path/to/secrets.json`
- helper command:
  ```bash
  web-farm secrets-set --secrets secrets.json --ref my_api --type headers --headers-json '{"Authorization":"Bearer ..."}'
  ```

## Project layout

- `src/web_farm/` — core runtime (HTTP, pagination, extractors, storage, export)
- `src/web_farm/framework/` — patch overlays + profile loader helpers
- `examples/` — small JSONL + patch examples
- `docs/` — deeper notes (SQLite blocked queue, offline snapshotting)
- `tests/` — unit tests

## Portfolio demo profiles (public APIs, no auth)

Two ready-to-run profiles are included:

1) **JSONPlaceholder posts** — `page` pagination via `_page` + `_limit`  
   Profile: `examples/profiles/jsonplaceholder_posts_page.json`

2) **PokéAPI pokemon list** — `next_url` pagination via `next` field  
   Profile: `examples/profiles/pokeapi_pokemon_next.json`

Run online (fetch live data):

```bash
web-farm run --profile examples/profiles/jsonplaceholder_posts_page.json --out out_posts.jsonl --max-items 12
web-farm run --profile examples/profiles/pokeapi_pokemon_next.json --out out_pokemon.jsonl --max-items 12
```

Run offline (no network) against bundled fixtures:

```bash
web-farm offline-test --profile examples/profiles/jsonplaceholder_posts_page.json
web-farm offline-test --profile examples/profiles/pokeapi_pokemon_next.json
```

## Notes for publishing to GitHub

- Add `.gitignore` for `secrets.json`, `*.db`, and any cookies/session files.
- Include at least one runnable demo profile using **public** endpoints or offline fixtures.

---
