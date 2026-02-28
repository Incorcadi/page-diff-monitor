from __future__ import annotations

"""
storage_sqlite.py — SQLite-хранилище с двумя таблицами:

1) items_raw     — "сырые события": сохраняем КАЖДУЮ встречу карточки (повторы не теряются)
2) items_unique  — "витрина": уникальные карточки + счётчик встреч + last_payload

Это решает проблему:
- дедуп нужен, чтобы выдать заказчику "чистый список"
- но повторы тоже могут быть важной инфой => держим raw-след

Термины:
- SQLite: база данных в одном файле .db, без отдельного сервера
- upsert: INSERT если нет, иначе UPDATE (в SQLite через ON CONFLICT DO UPDATE)
"""

from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
import json
import sqlite3
import uuid
from typing import Any, Optional

from .site_profile import ExtractSpec
from .keying import extract_item_id, make_item_key


@dataclass
class DualWriteStats:
    items_seen: int = 0
    raw_inserted: int = 0
    unique_inserted: int = 0
    unique_updated: int = 0


class DualSqliteStore:
    """
    Хранилище "raw + unique".

    По умолчанию таблицы:
      - items_raw
      - items_unique

    raw: сохраняем каждую встречу (seq, run_id)
    unique: уникальная карточка (по item_key), плюс:
      - seen_count (сколько раз встречалась)
      - first_seen_at / last_seen_at
      - payload (последняя версия payload)
    """

    def __init__(
        self,
        db_path: str,
        *,
        extract_spec: ExtractSpec,
        raw_table: str = "items_raw",
        unique_table: str = "items_unique",
    ) -> None:
        self.db_path = str(db_path)
        self.extract_spec = extract_spec
        self.raw_table = raw_table
        self.unique_table = unique_table

        Path(self.db_path).parent.mkdir(parents=True, exist_ok=True)
        self.conn = sqlite3.connect(self.db_path)
        self.conn.execute("PRAGMA journal_mode=WAL;")
        self.conn.execute("PRAGMA synchronous=NORMAL;")
        self.conn.execute("PRAGMA temp_store=MEMORY;")
        self._ensure_schema()

    def close(self) -> None:
        try:
            self.conn.commit()
        finally:
            self.conn.close()

    def __enter__(self) -> "DualSqliteStore":
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        self.close()

    def _ensure_schema(self) -> None:
        # RAW: каждая встреча отдельной строкой
        self.conn.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {self.raw_table} (
                rid INTEGER PRIMARY KEY AUTOINCREMENT,
                run_id TEXT NOT NULL,
                seq INTEGER NOT NULL,
                item_key TEXT NOT NULL,
                item_id TEXT,
                payload TEXT NOT NULL,
                seen_at TEXT NOT NULL
            );
            """
        )
        self.conn.execute(f"CREATE INDEX IF NOT EXISTS idx_{self.raw_table}_item_key ON {self.raw_table}(item_key);")
        self.conn.execute(f"CREATE INDEX IF NOT EXISTS idx_{self.raw_table}_run_id ON {self.raw_table}(run_id);")

        # UNIQUE: ключ = item_key
        self.conn.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {self.unique_table} (
                item_key TEXT PRIMARY KEY,
                item_id  TEXT,
                payload TEXT NOT NULL,
                first_seen_at TEXT NOT NULL,
                last_seen_at  TEXT NOT NULL,
                seen_count INTEGER NOT NULL DEFAULT 1
            );
            """
        )
        self.conn.execute(f"CREATE INDEX IF NOT EXISTS idx_{self.unique_table}_item_id ON {self.unique_table}(item_id);")

        # RUN STATE: последняя сохранённая точка пагинации (для resume)
        self.conn.execute(
            """
            CREATE TABLE IF NOT EXISTS run_state (
                profile TEXT NOT NULL,
                run_id   TEXT NOT NULL,
                state_json TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                batch_idx INTEGER,
                last_seq  INTEGER,
                items_seen INTEGER,
                PRIMARY KEY(profile, run_id)
            );
            """
        )
        self.conn.execute("CREATE INDEX IF NOT EXISTS idx_run_state_profile_updated ON run_state(profile, updated_at);")

        # BLOCKED EVENTS: очередь "нужен человек" (anti-bot / captcha / cloudflare / auth)
        self.conn.execute(
            """
            CREATE TABLE IF NOT EXISTS blocked_events (
                bid INTEGER PRIMARY KEY AUTOINCREMENT,
                created_at TEXT NOT NULL,
                resolved_at TEXT,
                resolved_note TEXT,

                profile TEXT NOT NULL,
                profile_path TEXT,
                run_id TEXT,
                batch_idx INTEGER,

                url TEXT NOT NULL,
                method TEXT,
                params_json TEXT,
                pagination_state_json TEXT,

                status_code INTEGER,
                block_hint TEXT,
                error TEXT,

                resp_url_final TEXT,
                resp_headers_json TEXT,
                resp_snippet TEXT
            );
            """
        )
        self.conn.execute("CREATE INDEX IF NOT EXISTS idx_blocked_events_profile_created ON blocked_events(profile, created_at);")
        self.conn.execute("CREATE INDEX IF NOT EXISTS idx_blocked_events_open_profile ON blocked_events(profile, resolved_at);")
        self.conn.execute("CREATE INDEX IF NOT EXISTS idx_blocked_events_run_id ON blocked_events(run_id);")
        self.conn.execute("CREATE INDEX IF NOT EXISTS idx_blocked_events_hint ON blocked_events(block_hint);")

    @staticmethod
    def new_run_id() -> str:
        return str(uuid.uuid4())

    @staticmethod
    def _now_iso() -> str:
        return datetime.now(timezone.utc).isoformat()

    def _insert_raw(self, *, run_id: str, seq: int, item_key: str, item_id: Optional[str], payload: str, seen_at: str) -> None:
        self.conn.execute(
            f"INSERT INTO {self.raw_table}(run_id, seq, item_key, item_id, payload, seen_at) VALUES(?,?,?,?,?,?)",
            (run_id, int(seq), item_key, item_id, payload, seen_at),
        )

    def _upsert_unique(self, *, item_key: str, item_id: Optional[str], payload: str, seen_at: str) -> bool:
        """
        Возвращает True, если вставили НОВЫЙ unique.
        False, если обновили существующий (то есть это повтор / новая встреча).

        Делаем надёжно:
          - try INSERT
          - если конфликт ключа => UPDATE (seen_count + 1, last_seen_at, payload)
        """
        try:
            self.conn.execute(
                f"""
                INSERT INTO {self.unique_table}(item_key, item_id, payload, first_seen_at, last_seen_at, seen_count)
                VALUES(?,?,?,?,?,1)
                """,
                (item_key, item_id, payload, seen_at, seen_at),
            )
            return True
        except sqlite3.IntegrityError:
            self.conn.execute(
                f"""
                UPDATE {self.unique_table}
                SET last_seen_at=?,
                    seen_count=seen_count+1,
                    payload=?,
                    item_id=COALESCE(?, item_id)
                WHERE item_key=?
                """,
                (seen_at, payload, item_id, item_key),
            )
            return False

    def put_both(self, item: dict[str, Any], *, run_id: str, seq: int) -> tuple[bool, str]:
        """
        Сохраняет:
        - RAW (всегда)
        - UNIQUE (upsert)

        Возвращает:
          (unique_inserted, item_key)
        """
        item_id = extract_item_id(item, self.extract_spec)
        item_key = make_item_key(item, self.extract_spec)
        payload = json.dumps(item, ensure_ascii=False)
        seen_at = self._now_iso()

        self._insert_raw(run_id=run_id, seq=seq, item_key=item_key, item_id=item_id, payload=payload, seen_at=seen_at)
        inserted = self._upsert_unique(item_key=item_key, item_id=item_id, payload=payload, seen_at=seen_at)
        return inserted, item_key

    def count_raw(self) -> int:
        row = self.conn.execute(f"SELECT COUNT(*) FROM {self.raw_table}").fetchone()
        return int(row[0]) if row else 0

    def count_unique(self) -> int:
        row = self.conn.execute(f"SELECT COUNT(*) FROM {self.unique_table}").fetchone()
        return int(row[0]) if row else 0

    # -----------------
    # resume/state
    # -----------------

    def save_state(
        self,
        *,
        profile: str,
        run_id: str,
        state: dict[str, Any],
        batch_idx: int,
        last_seq: int,
        items_seen: int,
    ) -> None:
        """Сохранить последнюю точку.

        Принцип: это технический "checkpoint". Он не влияет на уникальность items,
        а только позволяет продолжить прогон с места, где остановились.
        """
        payload = json.dumps(state, ensure_ascii=False)
        updated_at = self._now_iso()
        self.conn.execute(
            """
            INSERT INTO run_state(profile, run_id, state_json, updated_at, batch_idx, last_seq, items_seen)
            VALUES(?,?,?,?,?,?,?)
            ON CONFLICT(profile, run_id) DO UPDATE SET
                state_json=excluded.state_json,
                updated_at=excluded.updated_at,
                batch_idx=excluded.batch_idx,
                last_seq=excluded.last_seq,
                items_seen=excluded.items_seen
            """,
            (str(profile), str(run_id), payload, updated_at, int(batch_idx), int(last_seq), int(items_seen)),
        )
        self.conn.commit()

    def load_state(self, *, profile: str, run_id: str) -> Optional[dict[str, Any]]:
        row = self.conn.execute(
            "SELECT state_json FROM run_state WHERE profile=? AND run_id=?",
            (str(profile), str(run_id)),
        ).fetchone()
        if not row:
            return None
        try:
            return json.loads(row[0])
        except Exception:
            return None

    def latest_run_id(self, *, profile: str) -> Optional[str]:
        row = self.conn.execute(
            "SELECT run_id FROM run_state WHERE profile=? ORDER BY updated_at DESC LIMIT 1",
            (str(profile),),
        ).fetchone()
        return str(row[0]) if row and row[0] else None


# -----------------
# blocked events
# -----------------

@staticmethod
def _json_or_none(s: Any) -> Any:
    if not s:
        return None
    try:
        return json.loads(s)
    except Exception:
        return None

@classmethod
def _row_to_blocked(cls, row: Any) -> dict[str, Any]:
    return {
        "bid": int(row[0]),
        "created_at": row[1],
        "resolved_at": row[2],
        "resolved_note": row[3],
        "profile": row[4],
        "profile_path": row[5],
        "run_id": row[6],
        "batch_idx": row[7],
        "url": row[8],
        "method": row[9],
        "params": cls._json_or_none(row[10]),
        "pagination_state": cls._json_or_none(row[11]),
        "status_code": row[12],
        "block_hint": row[13],
        "error": row[14],
        "resp_url_final": row[15],
        "resp_headers": cls._json_or_none(row[16]),
        "resp_snippet": row[17],
    }

def add_blocked_event(
    self,
    *,
    profile: str,
    profile_path: Optional[str],
    run_id: Optional[str],
    batch_idx: Optional[int],
    url: str,
    method: Optional[str],
    params: Optional[dict[str, Any]],
    pagination_state: Optional[dict[str, Any]],
    status_code: Optional[int],
    block_hint: Optional[str],
    error: Optional[str],
    resp_url_final: Optional[str],
    resp_headers: Optional[dict[str, Any]],
    resp_snippet: Optional[str],
) -> int:
    created_at = self._now_iso()
    params_json = json.dumps(params, ensure_ascii=False) if params is not None else None
    st_json = json.dumps(pagination_state, ensure_ascii=False) if pagination_state is not None else None
    h_json = json.dumps(resp_headers, ensure_ascii=False) if resp_headers is not None else None

    cur = self.conn.execute(
        """
        INSERT INTO blocked_events(
            created_at, resolved_at, resolved_note,
            profile, profile_path, run_id, batch_idx,
            url, method, params_json, pagination_state_json,
            status_code, block_hint, error,
            resp_url_final, resp_headers_json, resp_snippet
        ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        """,
        (
            created_at, None, None,
            str(profile), str(profile_path) if profile_path else None,
            str(run_id) if run_id else None,
            int(batch_idx) if batch_idx is not None else None,
            str(url), str(method) if method else None,
            params_json, st_json,
            int(status_code) if status_code is not None else None,
            str(block_hint) if block_hint else None,
            str(error) if error else None,
            str(resp_url_final) if resp_url_final else None,
            h_json,
            (resp_snippet or None),
        ),
    )
    self.conn.commit()
    return int(cur.lastrowid)

def mark_blocked_resolved(self, *, bid: int, note: str = "") -> None:
    self.conn.execute(
        "UPDATE blocked_events SET resolved_at=?, resolved_note=? WHERE bid=?",
        (self._now_iso(), (note or None), int(bid)),
    )
    self.conn.commit()

def get_blocked_event(self, *, bid: int) -> Optional[dict[str, Any]]:
    row = self.conn.execute(
        "SELECT bid, created_at, resolved_at, resolved_note, profile, profile_path, run_id, batch_idx, url, method, params_json, pagination_state_json, status_code, block_hint, error, resp_url_final, resp_headers_json, resp_snippet FROM blocked_events WHERE bid=?",
        (int(bid),),
    ).fetchone()
    return self._row_to_blocked(row) if row else None

def list_blocked_events(
    self,
    *,
    profile: Optional[str] = None,
    run_id: Optional[str] = None,
    only_open: bool = True,
    limit: int = 50,
    offset: int = 0,
) -> list[dict[str, Any]]:
    wh: list[str] = []
    args: list[Any] = []
    if profile:
        wh.append("profile=?")
        args.append(str(profile))
    if run_id:
        wh.append("run_id=?")
        args.append(str(run_id))
    if only_open:
        wh.append("resolved_at IS NULL")
    where = (" WHERE " + " AND ".join(wh)) if wh else ""
    q = "SELECT bid, created_at, resolved_at, resolved_note, profile, profile_path, run_id, batch_idx, url, method, params_json, pagination_state_json, status_code, block_hint, error, resp_url_final, resp_headers_json, resp_snippet FROM blocked_events" + where + " ORDER BY created_at DESC LIMIT ? OFFSET ?"
    args.extend([int(limit), int(offset)])
    rows = self.conn.execute(q, tuple(args)).fetchall()
    return [self._row_to_blocked(r) for r in rows]

def latest_open_blocked(self, *, profile: str) -> Optional[dict[str, Any]]:
    row = self.conn.execute(
        "SELECT bid, created_at, resolved_at, resolved_note, profile, profile_path, run_id, batch_idx, url, method, params_json, pagination_state_json, status_code, block_hint, error, resp_url_final, resp_headers_json, resp_snippet FROM blocked_events WHERE profile=? AND resolved_at IS NULL ORDER BY created_at DESC LIMIT 1",
        (str(profile),),
    ).fetchone()
    return self._row_to_blocked(row) if row else None
