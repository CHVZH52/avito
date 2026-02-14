import sqlite3
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import Optional, Dict, List

from dto import SearchQuery

DEFAULT_INTERVAL_SECONDS = 90
MIN_INTERVAL_SECONDS = 20
MAX_INTERVAL_SECONDS: Optional[int] = None


def _clamp_interval_value(value: int) -> int:
    clamped = max(MIN_INTERVAL_SECONDS, value)
    if MAX_INTERVAL_SECONDS is not None:
        clamped = min(MAX_INTERVAL_SECONDS, clamped)
    return clamped


@dataclass
class UserProfile:
    chat_id: int
    username: Optional[str]
    default_region: str = "all"
    default_delivery: str = "any"
    default_track_price: bool = True
    default_sort_new: bool = False


class UserFiltersStorage:
    def __init__(self, db_path: Optional[Path] = None):
        self.db_path = self._resolve_db_path(db_path)
        self._init_db()

    def _resolve_db_path(self, custom_path: Optional[Path]) -> Path:
        if custom_path:
            custom_path.parent.mkdir(parents=True, exist_ok=True)
            custom_path.touch(exist_ok=True)
            return custom_path
        default = Path(__file__).resolve().parent / "user_filters.db"
        try:
            default.touch(exist_ok=True)
            return default
        except PermissionError:
            fallback = Path("/tmp/avito_parser_user_filters.db")
            fallback.parent.mkdir(parents=True, exist_ok=True)
            fallback.touch(exist_ok=True)
            return fallback

    @contextmanager
    def _connect(self):
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA foreign_keys=ON")
        try:
            yield conn
        finally:
            try:
                conn.close()
            except Exception:
                pass

    def _init_db(self):
        with self._connect() as conn:
            cursor = conn.cursor()
            cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS users (
                    chat_id INTEGER PRIMARY KEY,
                    username TEXT,
                    default_region TEXT DEFAULT 'all',
                    default_delivery TEXT DEFAULT 'any',
                    default_track_price INTEGER DEFAULT 1,
                    default_sort_new INTEGER DEFAULT 0
                )
                """
            )
            cursor.execute(
                f"""
                CREATE TABLE IF NOT EXISTS filters (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    chat_id INTEGER NOT NULL,
                    text TEXT NOT NULL,
                    region TEXT DEFAULT 'all',
                    min_price INTEGER,
                    max_price INTEGER,
                    delivery TEXT DEFAULT 'any',
                    sort_new INTEGER,
                    track_price_changes INTEGER DEFAULT 1,
                    max_age_seconds INTEGER,
                    interval_minutes INTEGER DEFAULT {DEFAULT_INTERVAL_SECONDS},
                    initial_summary_sent INTEGER DEFAULT 0,
                    FOREIGN KEY(chat_id) REFERENCES users(chat_id) ON DELETE CASCADE
                )
                """
            )
            self._ensure_column(
                conn,
                "filters",
                "interval_minutes",
                f"INTEGER DEFAULT {DEFAULT_INTERVAL_SECONDS}",
            )
            self._ensure_interval_seconds(conn)
            self._ensure_column(
                conn,
                "filters",
                "initial_summary_sent",
                "INTEGER DEFAULT 0",
            )
            self._ensure_column(
                conn,
                "filters",
                "max_age_seconds",
                "INTEGER",
            )
            conn.commit()

    @staticmethod
    def _ensure_column(conn, table: str, column: str, definition: str) -> None:
        columns = {row["name"] for row in conn.execute(f"PRAGMA table_info({table})").fetchall()}
        if column not in columns:
            conn.execute(f"ALTER TABLE {table} ADD COLUMN {column} {definition}")

    def _ensure_interval_seconds(self, conn) -> None:
        info = conn.execute("PRAGMA table_info(filters)").fetchall()
        columns = {row["name"] for row in info}
        if "interval_seconds" not in columns:
            conn.execute(
                f"ALTER TABLE filters ADD COLUMN interval_seconds INTEGER DEFAULT {DEFAULT_INTERVAL_SECONDS}"
            )
            conn.execute(
                "UPDATE filters SET interval_seconds = interval_minutes * 60 "
                "WHERE interval_minutes IS NOT NULL"
            )
        conn.execute(
            f"UPDATE filters SET interval_seconds = {DEFAULT_INTERVAL_SECONDS} WHERE interval_seconds IS NULL"
        )

    def ensure_user(self, chat_id: int, username: Optional[str] = None) -> None:
        with self._connect() as conn:
            conn.execute(
                """
                INSERT INTO users (chat_id, username)
                VALUES (?, ?)
                ON CONFLICT(chat_id) DO UPDATE SET username=excluded.username
                """,
                (chat_id, username),
            )
            conn.commit()

    def get_user_profile(self, chat_id: int) -> UserProfile:
        with self._connect() as conn:
            row = conn.execute(
                "SELECT * FROM users WHERE chat_id=?",
                (chat_id,),
            ).fetchone()
            if not row:
                self.ensure_user(chat_id, None)
                return UserProfile(chat_id=chat_id, username=None)
            return UserProfile(
                chat_id=row["chat_id"],
                username=row["username"],
                default_region=row["default_region"] or "all",
                default_delivery=row["default_delivery"] or "any",
                default_track_price=bool(row["default_track_price"]),
                default_sort_new=bool(row["default_sort_new"]),
            )

    def update_user_defaults(
        self,
        chat_id: int,
        *,
        default_region: Optional[str] = None,
        default_delivery: Optional[str] = None,
        default_track_price: Optional[bool] = None,
        default_sort_new: Optional[bool] = None,
    ) -> None:
        self.ensure_user(chat_id, None)
        updates = []
        params = []
        if default_region is not None:
            updates.append("default_region=?")
            params.append(default_region)
        if default_delivery is not None:
            updates.append("default_delivery=?")
            params.append(default_delivery)
        if default_track_price is not None:
            updates.append("default_track_price=?")
            params.append(int(default_track_price))
        if default_sort_new is not None:
            updates.append("default_sort_new=?")
            params.append(int(default_sort_new))
        if not updates:
            return
        params.append(chat_id)
        with self._connect() as conn:
            conn.execute(
                f"UPDATE users SET {' , '.join(updates)} WHERE chat_id=?",
                params,
            )
            conn.commit()

    def add_filter(
        self,
        chat_id: int,
        *,
        text: str,
        region: str,
        min_price: Optional[int],
        max_price: Optional[int],
        delivery: str,
        sort_new: Optional[bool],
        track_price_changes: bool,
        max_age_seconds: Optional[int] = None,
        interval_seconds: Optional[int] = None,
    ) -> int:
        self.ensure_user(chat_id, None)
        interval = interval_seconds if interval_seconds is not None else DEFAULT_INTERVAL_SECONDS
        interval = _clamp_interval_value(interval)
        with self._connect() as conn:
            cursor = conn.cursor()
            cursor.execute(
                """
                INSERT INTO filters (chat_id, text, region, min_price, max_price, delivery, sort_new, track_price_changes, max_age_seconds, interval_seconds, initial_summary_sent)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 0)
                """,
                (
                    chat_id,
                    text,
                    region,
                    min_price,
                    max_price,
                    delivery,
                    None if sort_new is None else int(sort_new),
                    int(track_price_changes),
                    max_age_seconds,
                    interval,
                ),
            )
            conn.commit()
            return cursor.lastrowid

    def list_filters(self, chat_id: int) -> List[sqlite3.Row]:
        with self._connect() as conn:
            rows = conn.execute(
                "SELECT * FROM filters WHERE chat_id=? ORDER BY id DESC",
                (chat_id,),
            ).fetchall()
            return rows

    def get_filter(self, filter_id: int, chat_id: Optional[int] = None) -> Optional[sqlite3.Row]:
        query = "SELECT * FROM filters WHERE id=?"
        params = [filter_id]
        if chat_id is not None:
            query += " AND chat_id=?"
            params.append(chat_id)
        with self._connect() as conn:
            return conn.execute(query, params).fetchone()

    def delete_filter(self, filter_id: int, chat_id: int) -> None:
        with self._connect() as conn:
            conn.execute(
                "DELETE FROM filters WHERE id=? AND chat_id=?",
                (filter_id, chat_id),
            )
            conn.commit()

    def update_filter(self, filter_id: int, chat_id: int, **fields) -> None:
        updates = []
        params = []
        for key, value in fields.items():
            updates.append(f"{key}=?")
            if key in {"sort_new", "track_price_changes"} and value is not None:
                params.append(int(value))
            elif key == "interval_seconds" and value is not None:
                try:
                    numeric = int(value)
                except (TypeError, ValueError):
                    numeric = DEFAULT_INTERVAL_SECONDS
                params.append(_clamp_interval_value(numeric))
            else:
                params.append(value)
        if not updates:
            return
        params.extend([filter_id, chat_id])
        with self._connect() as conn:
            conn.execute(
                f"UPDATE filters SET {', '.join(updates)} WHERE id=? AND chat_id=?",
                params,
            )
            conn.commit()

    def toggle_delivery_only(self, filter_id: int, chat_id: int) -> str:
        row = self.get_filter(filter_id, chat_id)
        if not row:
            return "any"
        new_value = "delivery_only" if row["delivery"] != "delivery_only" else "any"
        self.update_filter(filter_id, chat_id, delivery=new_value)
        return new_value

    def toggle_price_tracking(self, filter_id: int, chat_id: int) -> bool:
        row = self.get_filter(filter_id, chat_id)
        if not row:
            return False
        new_value = not bool(row["track_price_changes"])
        self.update_filter(filter_id, chat_id, track_price_changes=new_value)
        return new_value

    def get_all_searches(self) -> Dict[int, List[SearchQuery]]:
        with self._connect() as conn:
            rows = conn.execute(
                "SELECT * FROM filters"
            ).fetchall()
        result: Dict[int, List[SearchQuery]] = {}
        for row in rows:
            search = SearchQuery(
                text=row["text"],
                region=row["region"] or "all",
                min_price=row["min_price"],
                max_price=row["max_price"],
                delivery=row["delivery"] or "any",
                sort_new=None if row["sort_new"] is None else bool(row["sort_new"]),
                track_price_changes=bool(row["track_price_changes"]),
                max_age_seconds=row["max_age_seconds"] if "max_age_seconds" in row.keys() else None,
            )
            result.setdefault(row["chat_id"], []).append(search)
        return result

    def get_filters_for_scheduler(self) -> List[sqlite3.Row]:
        with self._connect() as conn:
            rows = conn.execute(
                """
                SELECT filters.*, users.username
                FROM filters
                LEFT JOIN users ON filters.chat_id = users.chat_id
                ORDER BY filters.id DESC
                """
            ).fetchall()
        return rows

    def mark_initial_summary_sent(self, chat_id: int, filter_id: int) -> None:
        self.update_filter(filter_id, chat_id, initial_summary_sent=1)
