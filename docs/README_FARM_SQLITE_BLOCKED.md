# Farm SQLite + blocked_events (human-in-the-loop)

## Apply into your project (scripts/scripts_pagination)
Replace / add:

- runtime.py         <- runtime.py (this bundle)
- block_detect.py    <- block_detect.py (new)
- storage_sqlite.py  <- storage_sqlite.py (this bundle)
- tool_pipeline.py   <- tool_pipeline.py (this bundle)

## Usage

### Single profile to SQLite (with blocked queue)
python -m scripts.scripts_pagination.tool_pipeline run-sqlite --profile profiles/site.json --db out/farm.db

If blocked, output contains:
- blocked=true
- blocked_bid=<id>

### List blocked events
python -m scripts.scripts_pagination.tool_pipeline blocked-list --db out/farm.db --profile-name <ProfileName>

### Resolve
python -m scripts.scripts_pagination.tool_pipeline blocked-resolve --db out/farm.db --id 12 --note "ok"

### Resume (retries the blocked request)
python -m scripts.scripts_pagination.tool_pipeline blocked-resume --db out/farm.db --profile profiles/site.json --profile-name <ProfileName>

### Farm (one DB, per-profile tables)
python -m scripts.scripts_pagination.tool_pipeline farm-sqlite --profiles-dir profiles/active --db out/farm.db --recursive

Use --resume to continue each profile from its latest run_id in DB.

### Farm resume: retry all currently blocked profiles
After you updated cookies/tokens (legit access), you can retry all open blocked events:

Dry-run (see what will be resumed):
python -m scripts.scripts_pagination.tool_pipeline farm-resume-open --db out/farm.db --dry-run

Resume and auto-resolve on success:
python -m scripts.scripts_pagination.tool_pipeline farm-resume-open --db out/farm.db --auto-resolve --resolve-note "cookies_updated"

Limit scope:
python -m scripts.scripts_pagination.tool_pipeline farm-resume-open --db out/farm.db --max-profiles 10 --max-items 500
