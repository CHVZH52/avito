import re
from pathlib import Path


def _sanitize_segment(value: str | int) -> str:
    raw = str(value) if value is not None else ""
    slug = re.sub(r"[^a-zA-Z0-9_-]+", "-", raw).strip("-")
    return slug or "user"


def user_xlsx_path(chat_identifier: str | int, base_dir: Path | None = None) -> Path:
    """Возвращает путь к XLSX-файлу пользователя."""
    root = Path(base_dir) if base_dir else Path(__file__).resolve().parent / "result"
    segment = _sanitize_segment(chat_identifier)
    return root / "users" / segment / "monitoring.xlsx"


def user_stats_xlsx_path(chat_identifier: str | int, base_dir: Path | None = None) -> Path:
    """Возвращает путь к XLSX-файлу статистики пользователя."""
    root = Path(base_dir) if base_dir else Path(__file__).resolve().parent / "result"
    segment = _sanitize_segment(chat_identifier)
    return root / "users" / segment / "stats.xlsx"


def user_cookies_path(chat_identifier: str | int, base_dir: Path | None = None) -> Path:
    """Возвращает путь к файлу cookies пользователя."""
    root = Path(base_dir) if base_dir else Path(__file__).resolve().parent / "cookies"
    segment = _sanitize_segment(chat_identifier)
    return root / f"{segment}.json"
