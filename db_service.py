import sqlite3

from models import Item


class SQLiteDBHandler:
    """Работа с БД sqlite"""
    _instance = None

    def __new__(cls, *args, **kwargs):
        if not cls._instance:
            cls._instance = super(SQLiteDBHandler, cls).__new__(cls)
        return cls._instance

    def __init__(self, db_name="database.db"):
        if not hasattr(self, "_initialized"):
            self.db_name = db_name
            self._create_table()
            self._initialized = True

    def _create_table(self):
        """Создает таблицу viewed, если она не существует."""
        with sqlite3.connect(self.db_name) as conn:
            cursor = conn.cursor()
            cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS viewed (
                    chat_id TEXT NOT NULL,
                    id INTEGER NOT NULL,
                    price INTEGER,
                    PRIMARY KEY(chat_id, id)
                )
                """
            )
            columns = [row[1] for row in cursor.execute("PRAGMA table_info(viewed)").fetchall()]
            if "chat_id" not in columns:
                cursor.execute("ALTER TABLE viewed RENAME TO viewed_old")
                cursor.execute(
                    """
                    CREATE TABLE viewed (
                        chat_id TEXT NOT NULL,
                        id INTEGER NOT NULL,
                        price INTEGER,
                        PRIMARY KEY(chat_id, id)
                    )
                    """
                )
                cursor.execute(
                    "INSERT INTO viewed (chat_id, id, price) SELECT 'global', id, price FROM viewed_old"
                )
                cursor.execute("DROP TABLE viewed_old")
            conn.commit()

    def add_record(self, ad: Item, chat_id: str = "global"):
        """Добавляет новую запись в таблицу viewed."""
        value = self._extract_price(ad)
        with sqlite3.connect(self.db_name) as conn:
            cursor = conn.cursor()
            cursor.execute(
                """
                INSERT INTO viewed (chat_id, id, price)
                VALUES (?, ?, ?)
                ON CONFLICT(chat_id, id) DO UPDATE SET price = excluded.price
                """,
                (chat_id, ad.id, value),
            )
            conn.commit()

    def add_record_from_page(self, ads: list[Item], chat_id: str = "global"):
        """Добавляет несколько записей в таблицу viewed."""
        records = [
            (chat_id, ad.id, self._extract_price(ad))
            for ad in ads
            if ad.id
        ]

        with sqlite3.connect(self.db_name) as conn:
            cursor = conn.cursor()
            cursor.executemany(
                """
                INSERT INTO viewed (chat_id, id, price)
                VALUES (?, ?, ?)
                ON CONFLICT(chat_id, id) DO UPDATE SET price = excluded.price
                """,
                records,
            )
            conn.commit()

    def record_exists(self, record_id, price, chat_id: str = "global", track_price_changes: bool = True):
        """Проверяет, существует ли запись с заданными параметрами"""
        if record_id is None:
            return False
        with sqlite3.connect(self.db_name) as conn:
            cursor = conn.cursor()
            if track_price_changes:
                cursor.execute(
                    "SELECT 1 FROM viewed WHERE chat_id = ? AND id = ? AND price = ?",
                    (chat_id, record_id, price),
                )
            else:
                cursor.execute(
                    "SELECT 1 FROM viewed WHERE chat_id = ? AND id = ?",
                    (chat_id, record_id),
                )
            return cursor.fetchone() is not None

    def has_history(self, chat_id: str = "global") -> bool:
        with sqlite3.connect(self.db_name) as conn:
            cursor = conn.cursor()
            cursor.execute(
                "SELECT 1 FROM viewed WHERE chat_id = ? LIMIT 1",
                (chat_id,),
            )
            return cursor.fetchone() is not None

    def get_price(self, record_id, chat_id: str = "global") -> int | None:
        if record_id is None:
            return None
        with sqlite3.connect(self.db_name) as conn:
            cursor = conn.cursor()
            cursor.execute(
                "SELECT price FROM viewed WHERE chat_id = ? AND id = ?",
                (chat_id, record_id),
            )
            row = cursor.fetchone()
            if row is None:
                return None
            return row[0]

    @staticmethod
    def _extract_price(ad: Item) -> int:
        try:
            return int(ad.priceDetailed.value)
        except Exception:
            return 0
