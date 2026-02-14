import tempfile
import unittest
from datetime import datetime, timedelta
from pathlib import Path

from dto import AvitoConfig, SearchQuery
from models import Item
from parser_cls import AvitoParse
from scheduler import _build_search
from user_filters import UserFiltersStorage


class _DummyParser:
    """Minimal stand-in to unit-test AvitoParse age filtering without heavy init."""

    _filter_by_recent_time = AvitoParse._filter_by_recent_time
    _active_max_age_seconds = AvitoParse._active_max_age_seconds

    @staticmethod
    def _is_recent(timestamp_ms: int, max_age_seconds: int) -> bool:
        return AvitoParse._is_recent(timestamp_ms=timestamp_ms, max_age_seconds=max_age_seconds)

    def __init__(self, config: AvitoConfig, active_search: SearchQuery | None):
        self.config = config
        self.active_search = active_search


class TestMaxAgePerQuery(unittest.TestCase):
    def test_filter_uses_search_override(self):
        cfg = AvitoConfig(urls=[], max_age=0, proxy_string="", use_free_proxies=False)
        search = SearchQuery(text="q", max_age_seconds=7 * 24 * 60 * 60)
        parser = _DummyParser(cfg, search)

        now = datetime.utcnow()
        recent_ms = int((now - timedelta(days=2)).timestamp() * 1000)
        old_ms = int((now - timedelta(days=20)).timestamp() * 1000)
        ads = [Item(id=1, sortTimeStamp=recent_ms), Item(id=2, sortTimeStamp=old_ms)]

        out = parser._filter_by_recent_time(ads)
        self.assertEqual([a.id for a in out], [1])

    def test_filter_falls_back_to_global_when_override_none(self):
        cfg = AvitoConfig(urls=[], max_age=3 * 24 * 60 * 60, proxy_string="", use_free_proxies=False)
        search = SearchQuery(text="q", max_age_seconds=None)
        parser = _DummyParser(cfg, search)

        now = datetime.utcnow()
        recent_ms = int((now - timedelta(days=2)).timestamp() * 1000)
        old_ms = int((now - timedelta(days=10)).timestamp() * 1000)
        ads = [Item(id=1, sortTimeStamp=recent_ms), Item(id=2, sortTimeStamp=old_ms)]

        out = parser._filter_by_recent_time(ads)
        self.assertEqual([a.id for a in out], [1])

    def test_override_zero_disables_filter(self):
        cfg = AvitoConfig(urls=[], max_age=3 * 24 * 60 * 60, proxy_string="", use_free_proxies=False)
        search = SearchQuery(text="q", max_age_seconds=0)
        parser = _DummyParser(cfg, search)

        now = datetime.utcnow()
        recent_ms = int((now - timedelta(days=2)).timestamp() * 1000)
        old_ms = int((now - timedelta(days=10)).timestamp() * 1000)
        ads = [Item(id=1, sortTimeStamp=recent_ms), Item(id=2, sortTimeStamp=old_ms)]

        out = parser._filter_by_recent_time(ads)
        self.assertEqual([a.id for a in out], [1, 2])

    def test_user_filters_storage_persists_max_age(self):
        with tempfile.TemporaryDirectory() as td:
            db_path = Path(td) / "user_filters.db"
            storage = UserFiltersStorage(db_path)

            chat_id = 1
            storage.ensure_user(chat_id, "u")
            fid = storage.add_filter(
                chat_id=chat_id,
                text="q",
                region="all",
                min_price=None,
                max_price=None,
                delivery="any",
                sort_new=None,
                track_price_changes=True,
                max_age_seconds=7 * 24 * 60 * 60,
                interval_seconds=90,
            )
            row = storage.get_filter(fid, chat_id)
            self.assertIsNotNone(row)
            self.assertEqual(int(row["max_age_seconds"]), 7 * 24 * 60 * 60)

            search = _build_search(row)
            self.assertEqual(int(search.max_age_seconds), 7 * 24 * 60 * 60)
