import asyncio
from copy import deepcopy
import hashlib
import html
import json
import random
import re
import threading
import sys
import tempfile
import time
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional
from urllib.parse import unquote, urlparse, parse_qs, urlencode, urlunparse

from bs4 import BeautifulSoup
from curl_cffi import requests
import requests as std_requests
import os
from loguru import logger
from pydantic import ValidationError
from requests.cookies import RequestsCookieJar

from common_data import HEADERS
from db_service import SQLiteDBHandler
from dto import Proxy, AvitoConfig, SearchQuery
from get_cookies import get_cookies
from hide_private_data import log_config, mask_sensitive_data
from load_config import load_avito_config
from stats_service import StatsDB
from user_filters import UserFiltersStorage
from models import ItemsResponse, Item
from tg_sender import SendAdToTg
from version import VERSION
from xlsx_service import XLSXHandler
from paths_helper import user_xlsx_path, user_cookies_path
from user_agents import random_user_agent
from proxy_utils import proxy_label, proxy_to_url
from free_proxy_robin import get_free_proxy_pool

DEBUG_MODE = False

REGION_URL_MAP = {
    "all": "all",
    "moscow": "moskva",
    "moscow_mo": "moskva_i_mo",
    "mo": "moskovskaya_oblast",
    "moscow_region": "moskovskaya_oblast",
}

REGION_SLUG_TO_KEY = {slug: key for key, slug in REGION_URL_MAP.items()}

REGION_LABELS = {
    "all": "Все регионы",
    "moscow": "Москва",
    "moscow_mo": "Москва и МО",
    "mo": "Московская область",
    "moscow_region": "Московская область",
}

BLOCK_PAGE_MARKERS = (
    "проблема с ip",
    "captcha",
    "доступ ограничен",
    "подозрительная активность",
    "/blocked",
    "/security",
)

PROXY_ERROR_MARKERS = (
    "proxy",
    "tunnel",
    "connect",
    "407",
    "proxy authentication required",
)


IMPERSONATE_OPTIONS: list[str] = [
    "chrome",
    "chrome110",
    "chrome120",
    "firefox118",
    "edge106",
]


_PROXY_STATE_LOCK = threading.Lock()
_PROXY_DEAD_UNTIL: dict[str, float] = {}

_AVITO_RATE_LIMIT_LOCK = threading.Lock()
_AVITO_RATE_LIMIT_UNTIL_TS: float = 0.0
_AVITO_RATE_LIMIT_HITS: int = 0


def _set_global_rate_limit(seconds: float) -> None:
    """Global cooldown shared across threads to prevent drift under 429 storms."""
    global _AVITO_RATE_LIMIT_UNTIL_TS
    try:
        seconds = float(seconds or 0.0)
    except Exception:
        seconds = 0.0
    if seconds <= 0:
        return
    until = time.time() + seconds
    with _AVITO_RATE_LIMIT_LOCK:
        _AVITO_RATE_LIMIT_UNTIL_TS = max(_AVITO_RATE_LIMIT_UNTIL_TS, until)


def _global_rate_limited_for() -> float:
    with _AVITO_RATE_LIMIT_LOCK:
        until = float(_AVITO_RATE_LIMIT_UNTIL_TS or 0.0)
    remaining = until - time.time()
    return remaining if remaining > 0 else 0.0


def _global_rate_limit_backoff_seconds(retry_after: str | None = None) -> float:
    """
    Global backoff for 429 shared across scheduler runs.
    Uses Retry-After when present; otherwise exponential with jitter.
    """
    if retry_after:
        try:
            val = int(str(retry_after).strip())
            if 0 < val < 3600:
                return float(val)
        except Exception:
            pass
    global _AVITO_RATE_LIMIT_HITS
    with _AVITO_RATE_LIMIT_LOCK:
        _AVITO_RATE_LIMIT_HITS = min(int(_AVITO_RATE_LIMIT_HITS or 0) + 1, 8)
        hits = int(_AVITO_RATE_LIMIT_HITS)
    base = 5 * (2 ** (hits - 1))
    jitter = random.uniform(0.0, 2.0)
    return float(min(180, base + jitter))

def _configure_logging() -> None:
    def _try_add(path: Path) -> bool:
        try:
            path.parent.mkdir(parents=True, exist_ok=True)
            logger.add(path, rotation="5 MB", retention="5 days", level="DEBUG")
            return True
        except PermissionError:
            return False

    project_log = Path(__file__).resolve().parent / "logs" / "app.log"
    if _try_add(project_log):
        return

    fallback_log = Path(tempfile.gettempdir()) / "avito_parser_logs" / "app.log"
    if _try_add(fallback_log):
        logger.info(f"Логи сохраняются в {fallback_log} из-за ограничений доступа")
        return

    logger.add(sys.stderr, level="DEBUG")
    logger.warning("Не удалось создать файл журнала — пишем в stdout")

_configure_logging()


class AvitoParse:
    def __init__(
            self,
            config: AvitoConfig,
            stop_event=None
    ):
        self.config = config
        self._user_proxy_pool = self._parse_proxy_pool(self.config.proxy_string)
        self._free_proxy_pool: list[str] = []
        self._free_proxy_set: set[str] = set()
        self._free_proxies_enabled = bool(getattr(self.config, "use_free_proxies", False))
        self._free_proxies_mix = bool(getattr(self.config, "free_proxies_mix_with_user_proxies", False))
        self._free_proxies_last_refresh_ts: float = 0.0
        self.proxy_pool: list[str] = []
        self.proxy_cursor = 0
        self._rebuild_proxy_pool()
        if self._free_proxies_enabled and (not self._user_proxy_pool or self._free_proxies_mix):
            self._refresh_free_proxy_pool(force=False, headers_for_avito=dict(HEADERS))
        self._proxy_407_count = 0
        self.proxy_obj = self.get_proxy_obj()
        self.active_search = None
        self.result_dir = self._ensure_result_dir()
        self.db_path = self._resolve_db_path()
        self.db_handler = SQLiteDBHandler(db_name=str(self.db_path))
        self.stats_db = StatsDB(self.db_path)
        self.chat_owner = getattr(self.config, "chat_owner", None) or self._resolve_chat_owner()
        self.filter_title = getattr(self.config, "filter_title", None)
        self.filter_interval_seconds = getattr(self.config, "filter_interval_seconds", None)
        self.skip_initial_notifications = getattr(self.config, "skip_first_notifications", False)
        self.export_user_id = getattr(self.config, "export_user_id", None)
        self.filters_storage = UserFiltersStorage()

        self._cookies_file_base = self._resolve_cookies_path()
        self.cookies_file = self._cookies_file_base
        self.initial_summary_sent = getattr(self.config, "initial_summary_sent", False)
        self._has_history_flag = self._has_history()
        self.initial_batch_mode = (
            not self.skip_initial_notifications
            and not (self.initial_summary_sent and self._has_history_flag)
            and self.chat_owner not in {None, "global"}
        )
        self.initial_batch_buffer: list[Item] = []
        self.tg_handler = self.get_tg_handler()
        self.xlsx_handler = XLSXHandler(self.__get_file_title())
        self.stop_event = stop_event
        self.cookies = None

        self._sessions_by_key: dict[str, requests.Session] = {}
        self._loaded_cookie_keys: set[str] = set()
        self._active_session_key: str = "direct"
        self.session = requests.Session()  
        self.headers = dict(HEADERS)

        try:
            self._set_user_agent(random_user_agent())
        except Exception:
            pass
        self.good_request_count = 0
        self.bad_request_count = 0
        self._last_cookie_refresh_ts = 0.0
        self._rate_limit_hits = 0
        self.notifications_ready = self._initial_notifications_ready()
        self._initial_skip_logged = False
        self._activate_session_key("direct")

        log_config(config=self.config, version=VERSION)
        if self._free_proxies_enabled:
            logger.info(
                "Proxy pool: user={} free={} total={} (mix={})",
                len(self._user_proxy_pool),
                len(self._free_proxy_pool),
                len(self.proxy_pool),
                "yes" if self._free_proxies_mix else "no",
            )

    def _rebuild_proxy_pool(self) -> None:
        if self._user_proxy_pool and not self._free_proxies_mix:
            self.proxy_pool = list(self._user_proxy_pool)
            return
        combined: list[str] = []
        seen: set[str] = set()
        for p in list(self._user_proxy_pool) + list(self._free_proxy_pool):
            s = str(p or "").strip()
            if not s or s in seen:
                continue
            seen.add(s)
            combined.append(s)
        self.proxy_pool = combined

    def _refresh_free_proxy_pool(self, *, force: bool, headers_for_avito: dict | None = None) -> bool:
        if not self._free_proxies_enabled:
            return False
        now = time.time()

        if not force and self._free_proxies_last_refresh_ts and (now - self._free_proxies_last_refresh_ts) < 30:
            return False

        try:
            pool = get_free_proxy_pool(
                self.config,
                force_refresh=bool(force),
                headers_for_avito=headers_for_avito,
            )
        except Exception as e:
            logger.warning(f"free_proxy_robin failed: {mask_sensitive_data(str(e))[:200]}")
            return False

        self._free_proxy_pool = list(pool or [])
        self._free_proxy_set = set(self._free_proxy_pool)
        self._free_proxies_last_refresh_ts = now
        self._rebuild_proxy_pool()
        # Reset cursor to avoid out-of-range after rebuild.
        if self.proxy_cursor >= len(self.proxy_pool):
            self.proxy_cursor = 0
        return bool(self._free_proxy_pool)

    @staticmethod
    def _ua_platform(ua: str) -> str:
        u = (ua or "").lower()
        if "windows" in u:
            return "Windows"
        if "mac os x" in u or "macintosh" in u:
            return "macOS"
        if "linux" in u:
            return "Linux"
        return "Windows"

    @staticmethod
    def _ua_major_version(ua: str, token: str) -> int | None:
        try:
            m = re.search(re.escape(token) + r"(\d+)", ua or "")
            return int(m.group(1)) if m else None
        except Exception:
            return None

    def _set_user_agent(self, ua: str) -> None:
        ua = (ua or "").strip()
        if not ua:
            return
        self.headers["user-agent"] = ua

        low = ua.lower()
        is_chromium = ("chrome/" in low) or ("edg/" in low)
        if not is_chromium:
            for k in ("sec-ch-ua", "sec-ch-ua-mobile", "sec-ch-ua-platform"):
                self.headers.pop(k, None)
            return

        platform = self._ua_platform(ua)
        self.headers["sec-ch-ua-mobile"] = "?0"
        self.headers["sec-ch-ua-platform"] = f"\"{platform}\""

        if "edg/" in low:
            v = self._ua_major_version(ua, "Edg/") or self._ua_major_version(ua, "Chrome/") or 120
            self.headers["sec-ch-ua"] = (
                f"\"Chromium\";v=\"{v}\", \"Not=A?Brand\";v=\"24\", \"Microsoft Edge\";v=\"{v}\""
            )
        else:
            v = self._ua_major_version(ua, "Chrome/") or 120
            self.headers["sec-ch-ua"] = (
                f"\"Chromium\";v=\"{v}\", \"Not=A?Brand\";v=\"24\", \"Google Chrome\";v=\"{v}\""
            )

    def _identity_key_for_proxy(self, proxy_string: str | None) -> str:
        if not proxy_string:
            return "direct"
        raw = str(proxy_string).strip()
        if not raw:
            return "direct"
        h = hashlib.sha1(raw.encode("utf-8", errors="ignore")).hexdigest()[:12]
        return f"p_{h}"

    def _cookies_file_for_key(self, key: str) -> Path:
        base = self._cookies_file_base
        if key == "direct":
            return base
        return base.with_name(f"{base.stem}__{key}{base.suffix}")

    def _activate_session_key(self, key: str) -> None:
        key = key or "direct"
        self._active_session_key = key
        if key not in self._sessions_by_key:
            self._sessions_by_key[key] = requests.Session()
        self.session = self._sessions_by_key[key]
        self.cookies_file = self._cookies_file_for_key(key)
        if key not in self._loaded_cookie_keys:
            self.load_cookies()
            self._loaded_cookie_keys.add(key)
        try:
            self.cookies = self.session.cookies.get_dict()
        except Exception:
            self.cookies = None

    def get_tg_handler(self) -> SendAdToTg | None:
        if all([self.config.tg_token, self.config.tg_chat_id]):
            return SendAdToTg(bot_token=self.config.tg_token, chat_id=self.config.tg_chat_id)
        return None

    def _send_to_tg(self, ads: list[Item]) -> None:
        if not self.tg_handler:
            return
        if self.initial_batch_mode:
            self.initial_batch_buffer.extend(ads)
            return
        if not self.notifications_ready:
            if not self._initial_skip_logged:
                logger.info(f"Пропускаю уведомления для первого запуска ({self.chat_owner})")
                self._initial_skip_logged = True
            return
        for ad in ads:
            self._annotate_ad(ad)
            self.tg_handler.send_to_tg(ad=ad)

    def _annotate_ad(self, ad: Item) -> None:
        if hasattr(ad, "filter_title"):
            ad.filter_title = self.filter_title or (self.active_search.text if self.active_search else None)
        if hasattr(ad, "filter_interval_seconds"):
            ad.filter_interval_seconds = self.filter_interval_seconds
        if hasattr(ad, "filter_region_label"):
            ad.filter_region_label = self._current_region_label()

    def _send_initial_batch_summary(self, ads: list[Item]) -> None:
        if not ads:
            return
        chunk_size = 5
        total = len(ads)
        chunks = [ads[i:i + chunk_size] for i in range(0, len(ads), chunk_size)]
        title = self.filter_title or (self.active_search.text if self.active_search else "Запрос")
        for idx, chunk in enumerate(chunks, 1):
            lines = [
                f"✨ Стартовый пакет {idx}/{len(chunks)} для запроса {SendAdToTg._escape(title)} "
                f"({len(chunk)} из {total})"
            ]
            for offset, ad in enumerate(chunk, 1):
                title_text = SendAdToTg._escape(ad.title or "Без названия")
                price_value = getattr(getattr(ad, "priceDetailed", None), "value", 0)
                price_text = SendAdToTg._format_price(price_value)
                full_url = f"https://www.avito.ru/{ad.urlPath}" if ad.urlPath else f"https://www.avito.ru/{ad.id}"
                number = (idx - 1) * chunk_size + offset
                lines.append(f"{number}. [{title_text}]({full_url}) — {price_text} ₽")
            lines.append("Дальше я буду присылать объявления по одному с фото и ссылкой.")
            logger.info(
                "Отправляю стартовый пакет {}/{} для запроса '{}' ({} объявлений)",
                idx,
                len(chunks),
                title,
                len(chunk),
            )
            self.tg_handler.send_to_tg(msg="\n".join(lines))
        self.initial_summary_sent = True
        chat_value = self.export_user_id
        try:
            if chat_value and self.config.filter_id:
                self.filters_storage.mark_initial_summary_sent(int(chat_value), self.config.filter_id)
        except Exception as err:
            logger.debug("Не удалось отметить отправку стартового пакета: {}", err)

    def get_proxy_obj(self) -> Proxy | None:
        current_proxy = self._current_proxy()
        if current_proxy:
            return Proxy(
                proxy_string=current_proxy,
                change_ip_link=self.config.proxy_change_url or "",
            )
        logger.info("Работаем без прокси")
        return None

    @staticmethod
    def _parse_proxy_pool(raw_proxy_string: str | None) -> list[str]:
        if not raw_proxy_string:
            return []
        items = [part.strip() for part in re.split(r"[\n,;]+", raw_proxy_string) if part.strip()]
        return [item for item in items if item]

    def _current_proxy(self) -> str | None:
        if not self.proxy_pool:
            if self._free_proxies_enabled:
                if self._refresh_free_proxy_pool(force=False, headers_for_avito=self.headers) and self.proxy_pool:
                    return self.proxy_pool[0]
            return None
        now = time.time()
        # Pick next non-dead proxy.
        for i in range(len(self.proxy_pool)):
            idx = (self.proxy_cursor + i) % len(self.proxy_pool)
            candidate = self.proxy_pool[idx]
            with _PROXY_STATE_LOCK:
                dead_until = _PROXY_DEAD_UNTIL.get(candidate, 0.0)
            if dead_until and dead_until > now:
                continue
            self.proxy_cursor = idx
            return candidate

        # All proxies are in cooldown; if free proxies are enabled, try refreshing once.
        if self._free_proxies_enabled:
            if self._refresh_free_proxy_pool(force=True, headers_for_avito=self.headers):
                # If user proxies exist and mix is disabled, include free proxies as a fallback when everything is dead.
                if self._user_proxy_pool and (not self._free_proxies_mix) and self._free_proxy_pool:
                    combined: list[str] = []
                    seen: set[str] = set()
                    for p in list(self._user_proxy_pool) + list(self._free_proxy_pool):
                        s = str(p or "").strip()
                        if not s or s in seen:
                            continue
                        seen.add(s)
                        combined.append(s)
                    self.proxy_pool = combined
                    self.proxy_cursor = 0

                for i in range(len(self.proxy_pool)):
                    idx = (self.proxy_cursor + i) % len(self.proxy_pool)
                    candidate = self.proxy_pool[idx]
                    with _PROXY_STATE_LOCK:
                        dead_until = _PROXY_DEAD_UNTIL.get(candidate, 0.0)
                    if dead_until and dead_until > now:
                        continue
                    self.proxy_cursor = idx
                    return candidate
        return None

    def _mark_proxy_dead(self, proxy_string: str, *, cooldown_seconds: int = 600) -> None:
        if not proxy_string:
            return
        with _PROXY_STATE_LOCK:
            _PROXY_DEAD_UNTIL[proxy_string] = time.time() + max(5, cooldown_seconds)

    def _proxy_to_requests_url(self, proxy_string: str) -> str:
        default_scheme = getattr(self.config, "proxy_default_scheme", None)
        return proxy_to_url(proxy_string, default_scheme=default_scheme)

    @staticmethod
    def _proxy_label(proxy_string: str) -> str:
        return proxy_label(proxy_string)

    def _build_proxy_data(self) -> dict | None:
        current_proxy = self._current_proxy()
        return self._build_proxy_data_for(current_proxy)

    def _build_proxy_data_for(self, proxy_string: str | None) -> dict | None:
        if not proxy_string:
            return None
        proxy_url = self._proxy_to_requests_url(proxy_string)
        return {
            "http": proxy_url,
            "https": proxy_url,
        }

    def _rotate_proxy(self) -> bool:
        if len(self.proxy_pool) <= 1:
            return False
        self.proxy_cursor = (self.proxy_cursor + 1) % len(self.proxy_pool)
        self.proxy_obj = self.get_proxy_obj()
        # Do not recreate session when switching proxies
        nxt = self._current_proxy()
        if not nxt:
            if self._free_proxies_enabled:
                self._refresh_free_proxy_pool(force=True, headers_for_avito=self.headers)
                nxt = self._current_proxy()
            if not nxt:
                logger.warning("Все прокси в cooldown/недоступны, временно работаю без прокси")
                return False
        self._activate_session_key(self._identity_key_for_proxy(nxt))
        logger.warning(f"Переключаюсь на следующий прокси {self._proxy_label(nxt)}")
        return True

    def _apply_cookies(self, cookies: dict) -> None:
        if not cookies:
            return
        jar = RequestsCookieJar()
        for key, value in cookies.items():
            jar.set(key, value)
        self.session.cookies.update(jar)
        self.cookies = dict(cookies)

    @staticmethod
    def _extract_location(response) -> str:
        try:
            return str(response.headers.get("location", "")).lower()
        except Exception:
            return ""

    def _is_block_page(self, response) -> bool:
        url_text = str(getattr(response, "url", "")).lower()
        location = self._extract_location(response)
        if any(marker in url_text for marker in BLOCK_PAGE_MARKERS):
            return True
        if any(marker in location for marker in BLOCK_PAGE_MARKERS):
            return True
        body = str(getattr(response, "text", "") or "")[:8000].lower()
        return any(marker in body for marker in BLOCK_PAGE_MARKERS)

    def _refresh_cookies_with_cooldown(self, min_interval: int = 20) -> None:
        now = time.time()
        if self._last_cookie_refresh_ts and (now - self._last_cookie_refresh_ts) < min_interval:
            logger.debug("Пропускаю обновление cookies: с прошлого обновления прошло мало времени")
            return
        fresh = self.get_cookies(max_retries=2)
        if fresh:
            self._apply_cookies(fresh)
            self.save_cookies()
        self._last_cookie_refresh_ts = now

    def _is_scheduler_mode(self) -> bool:
        # Scheduler creates configs with filter_interval_seconds set.
        return getattr(self, "filter_interval_seconds", None) is not None

    def _stats_chat_id(self) -> str:
        value = self.export_user_id or self._primary_user_chat_id() or "global"
        return str(value)

    def _stats_filter_title(self) -> str | None:
        return self.filter_title or (self.active_search.text if self.active_search else None)

    def _stats_record_request(
        self,
        *,
        url: str,
        proxy_used: str | None,
        status_code: int | None,
        outcome: str,
        duration_ms: int | None = None,
        error: str | None = None,
    ) -> None:
        try:
            db = getattr(self, "stats_db", None)
            if not db:
                return
            if not proxy_used:
                proxy_kind = "direct"
            elif proxy_used in self._free_proxy_set:
                proxy_kind = "free"
            else:
                proxy_kind = "user"
            db.record_request(
                chat_id=self._stats_chat_id(),
                filter_id=getattr(self.config, "filter_id", None),
                filter_title=self._stats_filter_title(),
                url=url,
                proxy_kind=proxy_kind,
                status_code=status_code,
                outcome=outcome,
                duration_ms=duration_ms,
                scheduler_mode=self._is_scheduler_mode(),
                error=error,
            )
        except Exception as err:
            logger.debug("stats record failed: {}", str(err)[:160])

    def _stats_record_items(self, ads: list[Item]) -> None:
        if not ads:
            return
        try:
            db = getattr(self, "stats_db", None)
            if not db:
                return
            chat = self._stats_chat_id()
            filt_id = getattr(self.config, "filter_id", None)
            filt_title = self._stats_filter_title()
            fallback_region = self._current_region_label()
            for ad in ads:
                try:
                    item_url = (
                        f"https://www.avito.ru{ad.urlPath}"
                        if getattr(ad, "urlPath", None)
                        else f"https://www.avito.ru/{ad.id}"
                    )
                    price_value = 0
                    try:
                        price_value = int(getattr(getattr(ad, "priceDetailed", None), "value", 0) or 0)
                    except Exception:
                        price_value = 0
                    region = (
                        getattr(ad, "filter_region_label", None)
                        or getattr(getattr(ad, "location", None), "name", None)
                        or getattr(getattr(ad, "geo", None), "formattedAddress", None)
                        or fallback_region
                        or ""
                    )
                    db.record_item(
                        chat_id=chat,
                        filter_id=filt_id,
                        filter_title=filt_title,
                        item_id=str(getattr(ad, "id", "") or ""),
                        item_url=item_url,
                        title=str(getattr(ad, "title", "") or ""),
                        price=price_value,
                        region=str(region or ""),
                    )
                except Exception:
                    continue
        except Exception as err:
            logger.debug("stats record items failed: {}", str(err)[:160])

    def _rate_limit_backoff_seconds(self, retry_after: str | None = None) -> float:
        """Backoff for 429. Prefer Retry-After when present; otherwise exponential with jitter."""
        if retry_after:
            try:
                val = int(str(retry_after).strip())
                if 0 < val < 3600:
                    return float(val)
            except Exception:
                pass
        self._rate_limit_hits = min(int(self._rate_limit_hits or 0) + 1, 6)
        base = 5 * (2 ** (self._rate_limit_hits - 1))
        jitter = random.uniform(0.0, 2.0)
        return float(min(120, base + jitter))

    def get_cookies(self, max_retries: int = 1, delay: float = 2.0) -> dict | None:
        for attempt in range(1, max_retries + 1):
            if self.stop_event and self.stop_event.is_set():
                return None

            try:
                current_proxy = self._current_proxy()
                self._activate_session_key(self._identity_key_for_proxy(current_proxy))
                # Ensure cookie fetch uses the same proxy as the active identity.
                proxy_obj = None
                if current_proxy:
                    proxy_obj = Proxy(
                        proxy_string=current_proxy,
                        change_ip_link=self.config.proxy_change_url or "",
                    )
                self.proxy_obj = proxy_obj
                cookies, user_agent = asyncio.run(
                    get_cookies(
                        proxy=proxy_obj,
                        headless=True,
                        stop_event=self.stop_event,
                        user_agent=str(self.headers.get("user-agent") or ""),
                        proxy_default_scheme=getattr(self.config, "proxy_default_scheme", None),
                    ))
                if cookies:
                    logger.info(f"[get_cookies] Успешно получены cookies с попытки {attempt}")

                    self._set_user_agent(user_agent)
                    self._apply_cookies(cookies)
                    self._last_cookie_refresh_ts = time.time()
                    self.save_cookies()
                    return cookies
                else:
                    raise ValueError("Пустой результат cookies")
            except Exception as e:
                logger.warning(f"[get_cookies] Попытка {attempt} не удалась: {e}")
                if attempt < max_retries:
                    time.sleep(delay * attempt)  # увеличиваем задержку
                else:
                    logger.error(f"[get_cookies] Все {max_retries} попытки не удались")
                    return None

    def save_cookies(self) -> None:
        """Сохраняет cookies из requests.Session в JSON-файл."""
        try:
            with self.cookies_file.open("w", encoding="utf-8") as f:
                payload = {
                    "cookies": self.session.cookies.get_dict(),
                    "user_agent": self.headers.get("user-agent"),
                    "ts": int(time.time()),
                }
                json.dump(payload, f, ensure_ascii=False)
        except PermissionError:
            fallback = Path(tempfile.gettempdir()) / "avito_parser_cookies.json"
            fallback.parent.mkdir(parents=True, exist_ok=True)
            with fallback.open("w", encoding="utf-8") as f:
                payload = {
                    "cookies": self.session.cookies.get_dict(),
                    "user_agent": self.headers.get("user-agent"),
                    "ts": int(time.time()),
                }
                json.dump(payload, f, ensure_ascii=False)

    def load_cookies(self) -> None:
        """Loads cookies for the currently active identity (direct or a proxy)."""
        try:
            with self.cookies_file.open("r", encoding="utf-8") as f:
                raw = json.load(f)
        except (FileNotFoundError, PermissionError, json.JSONDecodeError):
            return

        if not isinstance(raw, dict):
            return

        # Backward compatible: old cookie file format was a plain dict of cookies.
        if "cookies" in raw and isinstance(raw.get("cookies"), dict):
            cookies = raw.get("cookies") or {}
            ua = raw.get("user_agent")
            if ua:
                try:
                    self._set_user_agent(str(ua))
                except Exception:
                    pass
        else:
            cookies = raw

        if not isinstance(cookies, dict):
            return

        self._apply_cookies(cookies)
        if cookies:
            logger.info("Cookies restored from file")

    def fetch_data(self, url, retries=3, backoff_factor=1):
        global_wait = _global_rate_limited_for()
        direct_disabled = global_wait > 0
        if global_wait > 0:
            # Global backoff is primarily for direct requests. If proxies are configured,
            # keep trying them but avoid falling back to direct while the cooldown is active.
            if self._is_scheduler_mode() and (not self.proxy_pool):
                logger.debug(f"Global rate-limit active for {global_wait:.1f}s; skip request")
                return None
            if not self.proxy_pool:
                time.sleep(min(global_wait, 5))

        # Keep scheduler runs fast: fewer retries and shorter backoffs.
        if self._is_scheduler_mode():
            retries = min(int(retries or 1), 2)
            backoff_factor = min(float(backoff_factor or 0), 0.5)

        timeout = 15 if self._is_scheduler_mode() else 20

        for attempt in range(1, int(retries or 1) + 1):
            if self.stop_event and self.stop_event.is_set():
                return None

            ua = str(self.headers.get("user-agent", "") or "")
            ua_low = ua.lower()
            if "firefox/" in ua_low:
                impersonate_choice = "firefox118"
            elif "edg/" in ua_low:
                impersonate_choice = random.choice(["edge106", "chrome", "chrome120"])
            elif "chrome/" in ua_low:
                impersonate_choice = random.choice(["chrome", "chrome110", "chrome120"])
            else:
                impersonate_choice = "chrome"

            impersonate_fallback_used = False

            def _do_request(proxy_data: dict | None):
                return self.session.get(
                    url=url,
                    headers=self.headers,
                    proxies=proxy_data,
                    impersonate=impersonate_choice,
                    timeout=timeout,
                    verify=False,
                    allow_redirects=True,
                )

            # Old behavior iterated the entire proxy pool per attempt which created multi-minute drift.
            # New behavior: up to 2 proxy attempts (only for proxy-connect errors), then 1 direct attempt.
            use_free_pool = bool(self._free_proxy_pool)
            if self.proxy_pool and use_free_pool:
                proxy_attempts_left = min(len(self.proxy_pool), 6)
            else:
                proxy_attempts_left = 2 if self.proxy_pool else 0
            tried_direct = False
            allow_direct = True
            if self._is_scheduler_mode() and self.proxy_pool:
                allow_direct = False
            if direct_disabled:
                allow_direct = False

            while True:
                if self.stop_event and self.stop_event.is_set():
                    return None

                proxy_used = None
                proxy_data = None
                if (not tried_direct) and proxy_attempts_left > 0 and self.proxy_pool:
                    proxy_used = self._current_proxy()
                    if not proxy_used:
                        if not allow_direct:
                            return None
                        tried_direct = True
                        continue
                    proxy_data = self._build_proxy_data_for(proxy_used)
                else:
                    if not allow_direct:
                        break
                    tried_direct = True

                try:
                    req_started = time.monotonic()
                    self._activate_session_key(self._identity_key_for_proxy(proxy_used))
                    response = _do_request(proxy_data)
                    elapsed_ms = int((time.monotonic() - req_started) * 1000)
                    logger.debug(f"Attempt {attempt}: status={response.status_code}")

                    if response.status_code >= 500:
                        raise requests.RequestsError(f"Server error: {response.status_code}")

                    status = int(response.status_code)
                    is_redirect = status in {301, 302, 303, 307, 308}
                    is_blocked_status = status in {403, 429}
                    is_blocked_redirect = is_redirect and self._is_block_page(response)

                    if status == 429:
                        self.bad_request_count += 1
                        self._stats_record_request(
                            url=url,
                            proxy_used=proxy_used,
                            status_code=status,
                            outcome="rate_limited",
                            duration_ms=elapsed_ms,
                        )
                        if (
                            proxy_used
                            and self.proxy_pool
                            and (not tried_direct)
                            and proxy_attempts_left > 0
                        ):
                            # When using proxies, switching IP is often better than waiting.
                            if proxy_used in self._free_proxy_set:
                                self._mark_proxy_dead(proxy_used, cooldown_seconds=30 * 60)
                            elif len(self.proxy_pool) > 1:
                                self._mark_proxy_dead(proxy_used, cooldown_seconds=10 * 60)
                            proxy_attempts_left -= 1
                            if len(self.proxy_pool) > 1:
                                if not self._rotate_proxy():
                                    tried_direct = True
                            else:
                                self.change_ip()
                            if not self._is_scheduler_mode():
                                self._refresh_cookies_with_cooldown(min_interval=300)
                            continue
                        retry_after = None
                        try:
                            retry_after = response.headers.get('retry-after')
                        except Exception:
                            retry_after = None
                        wait_seconds = _global_rate_limit_backoff_seconds(retry_after)
                        _set_global_rate_limit(wait_seconds)
                        logger.warning(
                            f"Rate limited (429). Backoff {wait_seconds:.1f}s (scheduler={self._is_scheduler_mode()})"
                        )
                        return None

                    if is_blocked_status or is_blocked_redirect:
                        self.bad_request_count += 1
                        self._stats_record_request(
                            url=url,
                            proxy_used=proxy_used,
                            status_code=status,
                            outcome="blocked",
                            duration_ms=elapsed_ms,
                        )
                        if proxy_used and self.proxy_pool and (not tried_direct) and proxy_attempts_left > 0:
                            if proxy_used in self._free_proxy_set:
                                self._mark_proxy_dead(proxy_used, cooldown_seconds=30 * 60)
                            elif len(self.proxy_pool) > 1:
                                self._mark_proxy_dead(proxy_used, cooldown_seconds=10 * 60)
                            proxy_attempts_left -= 1
                            if len(self.proxy_pool) > 1:
                                if not self._rotate_proxy():
                                    tried_direct = True
                            else:
                                self.change_ip()
                            if not self._is_scheduler_mode():
                                self._refresh_cookies_with_cooldown(min_interval=300)
                            continue
                        if not self._rotate_proxy():
                            self.change_ip()
                        if not self._is_scheduler_mode():
                            self._refresh_cookies_with_cooldown(min_interval=300)
                        if self._is_scheduler_mode():
                            return None
                        raise requests.RequestsError(f"Blocked/redirect: {status}")

                    if self._is_block_page(response):
                        self.bad_request_count += 1
                        self._stats_record_request(
                            url=url,
                            proxy_used=proxy_used,
                            status_code=status,
                            outcome="blocked",
                            duration_ms=elapsed_ms,
                        )
                        if proxy_used and self.proxy_pool and (not tried_direct) and proxy_attempts_left > 0:
                            if proxy_used in self._free_proxy_set:
                                self._mark_proxy_dead(proxy_used, cooldown_seconds=30 * 60)
                            elif len(self.proxy_pool) > 1:
                                self._mark_proxy_dead(proxy_used, cooldown_seconds=10 * 60)
                            proxy_attempts_left -= 1
                            if len(self.proxy_pool) > 1:
                                if not self._rotate_proxy():
                                    tried_direct = True
                            else:
                                self.change_ip()
                            if not self._is_scheduler_mode():
                                self._refresh_cookies_with_cooldown(min_interval=300)
                            continue
                        if not self._rotate_proxy():
                            self.change_ip()
                        if not self._is_scheduler_mode():
                            self._refresh_cookies_with_cooldown(min_interval=300)
                        if self._is_scheduler_mode():
                            return None
                        raise requests.RequestsError("Blocked page detected")

                    # Success path.
                    self._rate_limit_hits = 0
                    # Reset global 429 streak on any successful response.
                    global _AVITO_RATE_LIMIT_HITS
                    with _AVITO_RATE_LIMIT_LOCK:
                        _AVITO_RATE_LIMIT_HITS = 0
                    self.save_cookies()
                    self.good_request_count += 1
                    self._stats_record_request(
                        url=url,
                        proxy_used=proxy_used,
                        status_code=status,
                        outcome="ok",
                        duration_ms=elapsed_ms,
                    )
                    return response.text

                except Exception as e:
                    err_text = str(e).lower()
                    if (
                        (not impersonate_fallback_used)
                        and ("impersonat" in err_text)
                        and ("not supported" in err_text)
                        and (impersonate_choice != "chrome")
                    ):
                        impersonate_choice = "chrome"
                        impersonate_fallback_used = True
                        continue
                    is_proxy_error = any(marker in err_text for marker in PROXY_ERROR_MARKERS)
                    is_407 = ("response 407" in err_text) or (" 407" in err_text) or err_text.strip().endswith("407")
                    try:
                        elapsed_ms = int((time.monotonic() - req_started) * 1000)
                    except Exception:
                        elapsed_ms = None
                    self._stats_record_request(
                        url=url,
                        proxy_used=proxy_used,
                        status_code=None,
                        outcome="proxy_error" if is_proxy_error else "other_error",
                        duration_ms=elapsed_ms,
                        error=str(e),
                    )

                    if is_proxy_error and proxy_used and self.proxy_pool and (not tried_direct) and proxy_attempts_left > 0:
                        self.bad_request_count += 1
                        safe_err = mask_sensitive_data(str(e))[:300]
                        logger.warning(f"Proxy error via {self._proxy_label(proxy_used)}: {safe_err}")
                        if is_407:
                            self._proxy_407_count += 1
                            self._mark_proxy_dead(proxy_used, cooldown_seconds=30 * 60)
                        elif proxy_used in self._free_proxy_set:
                            # Free proxies are volatile; keep them in cooldown to avoid rapid reuse loops.
                            self._mark_proxy_dead(proxy_used, cooldown_seconds=10 * 60)
                        else:
                            # Prevent tight loops on dead user proxies; short cooldown is enough.
                            self._mark_proxy_dead(proxy_used, cooldown_seconds=3 * 60)
                        proxy_attempts_left -= 1
                        rotated = self._rotate_proxy()
                        if not rotated:
                            tried_direct = True
                        time.sleep(0.05)
                        continue

                    # If proxies were used but we haven't tried direct yet, try direct once.
                    if self.proxy_pool and not tried_direct:
                        if allow_direct:
                            tried_direct = True
                            continue
                        break

                    logger.debug(f"Attempt {attempt} failed: {str(e)[:200]}")
                    break

                if tried_direct:
                    break

            if attempt < retries:
                sleep_time = float(backoff_factor or 0) * attempt
                if sleep_time > 0:
                    time.sleep(sleep_time)
            else:
                return None

    def _fetch_via_std_requests(self, url: str) -> str | None:
        """Fallback fetch for proxy-auth edge cases. Not as stealthy as curl_cffi."""
        proxy_used = self._current_proxy()
        self._activate_session_key(self._identity_key_for_proxy(proxy_used))
        proxy_data = self._build_proxy_data_for(proxy_used)
        # std_requests needs extra deps for SOCKS; keep this fallback HTTP-only.
        try:
            purl = str((proxy_data or {}).get("http") or "")
        except Exception:
            purl = ""
        if purl.startswith(("socks4://", "socks5://", "socks5h://")):
            return None
        try:
            r = std_requests.get(
                url,
                headers=self.headers,
                proxies=proxy_data,
                cookies=self.session.cookies.get_dict(),
                timeout=20,
                verify=False,
                allow_redirects=True,
            )
            logger.debug("fallback std_requests status={}", r.status_code)
            if r.status_code >= 400:
                return None
            return r.text
        except Exception as err:
            logger.debug("fallback std_requests failed: {}", str(err)[:200])
            return None

    def parse(self):
        if self.config.one_file_for_link:
            self.xlsx_handler = None
        if not self.cookies:
            logger.info("Cookies отсутствуют, получаю новую сессию")
            self.get_cookies(max_retries=2)
        resolved_targets = self._resolve_input_links()

        for _index, (search_meta, url) in enumerate(resolved_targets):
            self.active_search = search_meta
            logger.info(f"Старт обработки: {url}")
            ads_in_link = []
            for i in range(0, self.config.count):
                if self.stop_event and self.stop_event.is_set():
                    return
                if DEBUG_MODE:
                    html_code = open("response.txt", "r", encoding="utf-8").read()
                else:
                    html_code = self.fetch_data(url=url, retries=self.config.max_count_of_retry)

                if not html_code:
                    logger.warning(
                        f"Не удалось получить HTML для {url}, пробую заново через {self.config.pause_between_links} сек.")
                    time.sleep(self.config.pause_between_links)
                    continue

                if not self.xlsx_handler and self.config.one_file_for_link:
                    self.xlsx_handler = XLSXHandler(self._single_file_path(_index, search_meta))

                data_from_page = self.find_json_on_page(html_code=html_code)
                try:
                    catalog = data_from_page.get("data", {}).get("catalog") or {}
                    ads_models = ItemsResponse(**catalog)
                except ValidationError as err:
                    logger.error(f"При валидации объявлений произошла ошибка: {err}")
                    continue

                ads = self._clean_null_ads(ads=ads_models.items)

                ads = self._add_seller_to_ads(ads=ads)

                if not ads:
                    logger.info("Объявления закончились, заканчиваю работу с данной ссылкой")
                    break

                filter_ads = self.filter_ads(ads=ads)

                self._stats_record_items(filter_ads)

                if self.tg_handler and not self.config.one_time_start:
                    self._send_to_tg(ads=filter_ads)

                filter_ads = self.parse_views(ads=filter_ads)

                if filter_ads:
                    self.__save_viewed(ads=filter_ads, chat_owner=self.chat_owner)

                    if self.config.save_xlsx:
                        ads_in_link.extend(filter_ads)

                url = self.get_next_page_url(url=url)
                if url:
                    logger.info(f"Следующая страница: {url}")

                logger.info(f"Пауза {self.config.pause_between_links} сек.")
                time.sleep(self.config.pause_between_links)

            if ads_in_link:
                logger.info(f"Сохраняю в Excel {len(ads_in_link)} объявлений")
                self.__save_data(ads=ads_in_link)
            else:
                logger.info("Сохранять нечего")

            if self.initial_batch_mode and self.initial_batch_buffer:
                self._send_initial_batch_summary(self.initial_batch_buffer)
                self.initial_batch_buffer.clear()
                self.initial_batch_mode = False

            if self.config.one_file_for_link:
                self.xlsx_handler = None
            self.active_search = None

        logger.info(f"Хорошие запросы: {self.good_request_count}шт, плохие: {self.bad_request_count}шт")

        if self.config.one_time_start and self.tg_handler:
            self.tg_handler.send_to_tg(msg="Парсинг Авито завершён. Все ссылки обработаны")
            self.stop_event = True

    @staticmethod
    def _clean_null_ads(ads: list[Item]) -> list[Item]:
        return [ad for ad in ads if ad.id]

    @staticmethod
    def find_json_on_page(html_code, data_type: str = "mime") -> dict:
        soup = BeautifulSoup(html_code, "html.parser")
        try:
            for _script in soup.select('script'):
                script_type = _script.get('type')

                if data_type == 'mime' and script_type == 'mime/invalid':
                    script_content = html.unescape(_script.text)
                    parsed_data = json.loads(script_content)

                    if 'state' in parsed_data:
                        return parsed_data['state']

                    elif 'data' in parsed_data:
                        logger.info("data")
                        return parsed_data['data']

                    else:
                        return parsed_data

        except Exception as err:
            logger.error(f"Ошибка при поиске информации на странице: {err}")
        return {}

    def filter_ads(self, ads: list[Item]) -> list[Item]:
        """Сортирует объявления"""
        filters = [
            self._filter_viewed,
            self._filter_by_price_range,
            self._filter_by_black_keywords,
            self._filter_by_white_keyword,
            self._filter_by_address,
            self._filter_by_delivery,
            self._filter_by_seller,
            self._filter_by_recent_time,
            self._filter_by_reserve,
            self._filter_by_promotion,
        ]

        for filter_fn in filters:
            ads = filter_fn(ads)
            logger.info(f"После фильтрации {filter_fn.__name__} осталось {len(ads)}")
            if not len(ads):
                return ads
        return ads

    def _filter_by_price_range(self, ads: list[Item]) -> list[Item]:
        min_price, max_price = self._get_active_price_bounds()
        if min_price is None and max_price is None:
            return ads
        filtered = []
        for ad in ads:
            try:
                price_value = ad.priceDetailed.value
            except Exception:
                continue
            if min_price is not None and price_value < min_price:
                continue
            if max_price is not None and price_value > max_price:
                continue
            filtered.append(ad)
        return filtered

    def _filter_by_black_keywords(self, ads: list[Item]) -> list[Item]:
        if not self.config.keys_word_black_list:
            return ads
        try:
            return [ad for ad in ads if not self._is_phrase_in_ads(ad=ad, phrases=self.config.keys_word_black_list)]
        except Exception as err:
            logger.debug(f"Ошибка при проверке объявлений по списку стоп-слов: {err}")
            return ads

    def _filter_by_white_keyword(self, ads: list[Item]) -> list[Item]:
        if not self.config.keys_word_white_list:
            return ads
        try:
            return [ad for ad in ads if self._is_phrase_in_ads(ad=ad, phrases=self.config.keys_word_white_list)]
        except Exception as err:
            logger.debug(f"Ошибка при проверке объявлений по списку обязательных слов: {err}")
            return ads

    def _filter_by_address(self, ads: list[Item]) -> list[Item]:
        if not self.config.geo:
            return ads
        try:
            return [ad for ad in ads if self.config.geo in ad.geo.formattedAddress]
        except Exception as err:
            logger.debug(f"Ошибка при проверке объявлений по адресу: {err}")
            return ads

    def _filter_by_delivery(self, ads: list[Item]) -> list[Item]:
        mode = self._active_delivery_mode()
        if mode == "any":
            return ads
        try:
            if mode == "delivery_only":
                return [ad for ad in ads if ad.contacts and ad.contacts.delivery]
            return [ad for ad in ads if not (ad.contacts and ad.contacts.delivery)]
        except Exception as err:
            logger.debug(f"Ошибка при фильтрации по доставке: {err}")
            return ads

    def _filter_viewed(self, ads: list[Item]) -> list[Item]:
        track_price_changes = self._should_track_price_changes()
        try:
            return [ad for ad in ads if not self.is_viewed(ad=ad, track_price_changes=track_price_changes)]
        except Exception as err:
            logger.debug(f"Ошибка при проверке объявления по признаку смотрели или не смотрели: {err}")
            return ads

    def _add_seller_to_ads(self, ads: list[Item]) -> list[Item]:
        for ad in ads:
            if seller_id := self._extract_seller_slug(data=ad):
                ad.sellerId = seller_id
        return ads

    @staticmethod
    def _add_promotion_to_ads(ads: list[Item]) -> list[Item]:
        for ad in ads:
            ad.isPromotion = any(
                v.get("title") == "Продвинуто"
                for step in (ad.iva or {}).get("DateInfoStep", [])
                for v in step.payload.get("vas", [])
            )
        return ads

    def _filter_by_seller(self, ads: list[Item]) -> list[Item]:
        if not self.config.seller_black_list:
            return ads
        try:
            return [ad for ad in ads if not ad.sellerId or ad.sellerId not in self.config.seller_black_list]
        except Exception as err:
            logger.debug(f"Ошибка при отсеивании объявления с продавцами из черного списка : {err}")
            return ads

    def _filter_by_recent_time(self, ads: list[Item]) -> list[Item]:
        max_age = self._active_max_age_seconds()
        if not max_age:
            return ads
        try:
            filtered: list[Item] = []
            for ad in ads:
                ts = getattr(ad, "sortTimeStamp", None)
                if not ts:
                    # If the timestamp is missing, keep the item (we can't reliably age-filter it).
                    filtered.append(ad)
                    continue
                if self._is_recent(timestamp_ms=int(ts), max_age_seconds=max_age):
                    filtered.append(ad)
            return filtered
        except Exception as err:
            logger.debug(f"Ошибка при отсеивании слишком старых объявлений: {err}")
            return ads

    def _filter_by_reserve(self, ads: list[Item]) -> list[Item]:
        if not self.config.ignore_reserv:
            return ads
        try:
            return [ad for ad in ads if not ad.isReserved]
        except Exception as err:
            logger.debug(f"Ошибка при отсеивании объявлений в резерве: {err}")
            return ads

    def _filter_by_promotion(self, ads: list[Item]) -> list[Item]:
        ads = self._add_promotion_to_ads(ads=ads)
        if not self.config.ignore_promotion:
            return ads
        try:
            return [ad for ad in ads if not ad.isPromotion]
        except Exception as err:
            logger.debug(f"Ошибка при отсеивании продвинутых объявлений: {err}")
            return ads

    def parse_views(self, ads: list[Item]) -> list[Item]:
        if not self.config.parse_views:
            return ads

        logger.info("Начинаю парсинг просмотров")

        for ad in ads:
            try:
                html_code_full_page = self.fetch_data(url=f"https://www.avito.ru{ad.urlPath}")
                ad.total_views, ad.today_views = self._extract_views(html=html_code_full_page)
                delay = random.uniform(0.1, 0.9)
                time.sleep(delay)
            except Exception as err:
                logger.warning(f"Ошибка при парсинге {ad.urlPath}: {err}")
                continue

        return ads

    @staticmethod
    def _extract_views(html: str) -> tuple:
        soup = BeautifulSoup(html, "html.parser")

        def extract_digits(element):
            return int(''.join(filter(str.isdigit, element.get_text()))) if element else None

        total = extract_digits(soup.select_one('[data-marker="item-view/total-views"]'))
        today = extract_digits(soup.select_one('[data-marker="item-view/today-views"]'))

        return total, today

    def change_ip(self, max_retries: int = 3) -> bool:
        if not self.config.proxy_change_url:
            logger.info("Смена IP пропущена: не задан proxy_change_url")
            return False
        logger.info("Пробую сменить IP")
        for attempt in range(1, max_retries + 1):
            try:
                res = requests.get(url=self.config.proxy_change_url, verify=False, timeout=20)
                if res.status_code == 200:
                    logger.info("IP изменен")
                    return True
                logger.warning(f"[{attempt}/{max_retries}] Не удалось сменить IP: HTTP {res.status_code}")
            except Exception as err:
                logger.warning(f"[{attempt}/{max_retries}] Ошибка при смене IP: {err}")
            if attempt < max_retries:
                time.sleep(min(3 * attempt, 10))
        return False

    @staticmethod
    def _extract_seller_slug(data):
        match = re.search(r"/brands/([^/?#]+)", str(data))
        if match:
            return match.group(1)
        return None

    @staticmethod
    def _is_phrase_in_ads(ad: Item, phrases: list) -> bool:
        full_text_from_ad = (ad.title + ad.description).lower()
        return any(phrase.lower() in full_text_from_ad for phrase in phrases)

    def is_viewed(self, ad: Item, track_price_changes: bool = True) -> bool:
        """Проверяет, смотрели мы это или нет"""
        try:
            price_value = int(getattr(getattr(ad, "priceDetailed", None), "value", 0))
        except Exception:
            price_value = 0
        previous_price = self.db_handler.get_price(record_id=ad.id, chat_id=self.chat_owner)
        if previous_price is None:
            return False
        if track_price_changes and previous_price != price_value:
            if hasattr(ad, "price_change_from"):
                ad.price_change_from = previous_price
            return False
        return True

    @staticmethod
    def _is_recent(timestamp_ms: int, max_age_seconds: int) -> bool:
        now = datetime.utcnow()
        published_time = datetime.utcfromtimestamp(timestamp_ms / 1000)
        return (now - published_time) <= timedelta(seconds=max_age_seconds)

    def __get_file_title(self) -> str:
        """Определяет название файла"""
        user_id = self.export_user_id or self._primary_user_chat_id()
        if user_id:
            user_path = user_xlsx_path(user_id, base_dir=self.result_dir)
            user_path.parent.mkdir(parents=True, exist_ok=True)
            return str(user_path)

        title_file = 'all'
        if getattr(self.config, "searches", None):
            parts = [self._slugify(search.text, fallback=f"query-{idx + 1}") for idx, search in enumerate(self.config.searches)]
            title_file = "-".join(parts)[:50] or "searches"
        elif getattr(self.config, "queries", None):
            title_file = "-".join(self._slugify(q) for q in self.config.queries)[:50]
        elif self.config.keys_word_white_list:
            title_file = "-".join(self._slugify(word) for word in self.config.keys_word_white_list)[:50]

        return str(self.result_dir / f"{title_file}.xlsx")

    def _ensure_result_dir(self) -> Path:
        default = Path(__file__).resolve().parent / "result"
        try:
            default.mkdir(parents=True, exist_ok=True)
            if not os.access(default, os.W_OK | os.X_OK):
                raise PermissionError("Нет прав на запись/вход в result/")
            return default
        except PermissionError as err:
            fallback = Path(tempfile.gettempdir()) / "avito_parser_result"
            fallback.mkdir(parents=True, exist_ok=True)
            logger.info(f"{err} — сохраняем в {fallback}")
            return fallback

    def _resolve_db_path(self) -> Path:
        default = Path(__file__).resolve().parent / "database.db"
        try:
            default.touch(exist_ok=True)
            if not os.access(default, os.W_OK | os.R_OK):
                raise PermissionError("Нет прав на запись в database.db")
            return default
        except PermissionError as err:
            fallback = Path(tempfile.gettempdir()) / "avito_parser_database.db"
            fallback.touch(exist_ok=True)
            logger.info(f"{err} — используем {fallback}")
            return fallback

    def _resolve_chat_owner(self) -> str:
        chats = self.config.tg_chat_id or []
        if len(chats) == 1:
            return str(chats[0])
        return "global"

    def _initial_notifications_ready(self) -> bool:
        if not self.skip_initial_notifications:
            return True
        return self._has_history()

    def _has_history(self) -> bool:
        chat_owner = getattr(self, "chat_owner", None)
        if not chat_owner or chat_owner == "global":
            return True
        try:
            return self.db_handler.has_history(chat_owner)
        except Exception:
            return True

    def _primary_user_chat_id(self) -> str | None:
        chats = self.config.tg_chat_id or []
        if chats:
            return str(chats[0])
        return None

    def _resolve_cookies_path(self) -> Path:
        owner = getattr(self, "chat_owner", None)
        base_dir = Path(__file__).resolve().parent
        if owner and owner != "global":
            path = user_cookies_path(owner)
        else:
            path = base_dir / "cookies.json"
        try:
            path.parent.mkdir(parents=True, exist_ok=True)
            path.touch(exist_ok=True)
            return path
        except PermissionError:
            fallback_base = Path(tempfile.gettempdir()) / "avito_parser_cookies"
            fallback_base.mkdir(parents=True, exist_ok=True)
            if owner and owner != "global":
                fallback = user_cookies_path(owner, base_dir=fallback_base)
            else:
                fallback = fallback_base / "cookies.json"
            fallback.touch(exist_ok=True)
            logger.info(f"Используем {fallback} для cookies из-за прав доступа")
            return fallback

    def _single_file_path(self, index: int, search_meta: SearchQuery | None) -> str:
        name = self._slugify(search_meta.text) if search_meta else f"link-{index + 1}"
        return str(self.result_dir / f"{name}.xlsx")

    @staticmethod
    def _slugify(value: str | None, fallback: str = "query") -> str:
        if not value:
            return fallback
        slug = re.sub(r"[^a-z0-9]+", "-", value.lower()).strip("-")
        slug = slug or fallback
        return slug[:50]

    def _get_active_price_bounds(self) -> tuple[Optional[int], Optional[int]]:
        min_price = None
        max_price = None
        if self.active_search:
            if self.active_search.min_price is not None:
                min_price = self.active_search.min_price
            if self.active_search.max_price is not None:
                max_price = self.active_search.max_price
        if min_price is None and self.config.min_price:
            min_price = self.config.min_price
        if max_price is None and self.config.max_price and self.config.max_price < 999_999_999:
            max_price = self.config.max_price
        return min_price, max_price

    def _active_max_age_seconds(self) -> int:
        """
        Effective max_age for the current search.
        - If SearchQuery.max_age_seconds is set (including 0), use it.
        - Otherwise fall back to AvitoConfig.max_age.
        """
        if self.active_search is not None:
            try:
                v = getattr(self.active_search, "max_age_seconds", None)
            except Exception:
                v = None
            if v is not None:
                try:
                    return int(v)
                except Exception:
                    return 0
        try:
            return int(getattr(self.config, "max_age", 0) or 0)
        except Exception:
            return 0

    def _current_region_label(self) -> str:
        if self.active_search and getattr(self.active_search, "region", None):
            key = self.active_search.region
        else:
            base_slug = getattr(self.config, "region_slug", None)
            if base_slug:
                key = REGION_SLUG_TO_KEY.get(base_slug, "all")
            else:
                key = "all"
        return REGION_LABELS.get(key, "Все регионы")

    def _active_delivery_mode(self) -> str:
        if self.active_search and self.active_search.delivery:
            return self.active_search.delivery
        if self.config.delivery_only:
            return "delivery_only"
        return "any"

    def _should_track_price_changes(self) -> bool:
        if self.active_search and self.active_search.track_price_changes is not None:
            return self.active_search.track_price_changes
        return True

    def __save_data(self, ads: list[Item]) -> None:
        """Сохраняет результат в файл keyword*.xlsx и в БД"""
        try:
            self.xlsx_handler.append_data_from_page(ads=ads)
        except Exception as err:
            logger.info(f"При сохранении в Excel ошибка {err}")

    def __save_viewed(self, ads: list[Item], chat_owner: str) -> None:
        """Сохраняет просмотренные объявления"""
        try:
            self.db_handler.add_record_from_page(ads=ads, chat_id=chat_owner)
            if self.skip_initial_notifications and not self.notifications_ready and chat_owner not in {None, "global"}:
                self.notifications_ready = True
        except Exception as err:
            logger.info(f"При сохранении в БД ошибка {err}")

    def get_next_page_url(self, url: str):
        """Получает следующую страницу"""
        try:
            url_parts = urlparse(url)
            query_params = parse_qs(url_parts.query)
            current_page = int(query_params.get('p', [1])[0])
            query_params['p'] = current_page + 1
            if self.config.one_time_start:
                logger.debug(f"Страница {current_page}")

            new_query = urlencode(query_params, doseq=True)
            next_url = urlunparse((url_parts.scheme, url_parts.netloc, url_parts.path, url_parts.params, new_query,
                                   url_parts.fragment))
            return next_url
        except Exception as err:
            logger.error(f"Не смог сформировать ссылку на следующую страницу для {url}. Ошибка: {err}")

    # формирования ссылок по запросу 
    def _resolve_input_links(self) -> list[tuple[SearchQuery | None, str]]:
        links: list[tuple[SearchQuery | None, str]] = []
        searches = getattr(self.config, "searches", None) or []
        if searches:
            for search in searches:
                search_url = self._build_search_url(search)
                logger.info(f"Сформирована поисковая ссылка для запроса '{search.text}': {search_url}")
                links.append((search, search_url))
            return links

        queries = getattr(self.config, "queries", None) or []
        for q in queries:
            q = (q or "").strip()
            if not q:
                continue
            search_stub = SearchQuery(text=q)
            search_url = self._build_search_url(search_stub)
            logger.info(f"Сформирована поисковая ссылка для запроса '{q}': {search_url}")
            links.append((search_stub, search_url))

        if links:
            return links

        return [(None, url) for url in (self.config.urls or [])]

    def _build_search_url(self, search: SearchQuery) -> str:
        region_key = getattr(search, "region", "all") or "all"
        region_slug = REGION_URL_MAP.get(region_key, region_key) or "all"
        base = f"https://www.avito.ru/{region_slug}"

        params_items: list[tuple[str, str | int]] = [("cd", 1), ("q", search.text.lower())]

        min_price = search.min_price if search.min_price is not None else (self.config.min_price or None)
        max_price = search.max_price if search.max_price is not None else (
            self.config.max_price if self.config.max_price and self.config.max_price < 999_999_999 else None
        )
        if min_price is not None:
            params_items.append(("pmin", min_price))
        if max_price is not None:
            params_items.append(("pmax", max_price))

        if search.delivery == "delivery_only":
            params_items.append(("d", 1))

        sort_flag = search.sort_new
        if sort_flag is None:
            sort_flag = self.config.sort_new
        if sort_flag:
            params_items.append(("s", 104))

        query_str = urlencode(params_items, doseq=True)
        return f"{base}?{query_str}"


def build_user_configs(base_config: AvitoConfig, storage: UserFiltersStorage) -> list[AvitoConfig]:
    user_map = storage.get_all_searches()
    if not user_map:
        return [base_config]

    configs: list[AvitoConfig] = []
    for chat_id, searches in user_map.items():
        if not searches:
            continue
        cfg = deepcopy(base_config)
        cfg.searches = searches
        cfg.queries = [search.text for search in searches]
        cfg.tg_chat_id = [str(chat_id)]
        cfg.chat_owner = str(chat_id)
        configs.append(cfg)

    if not configs:
        configs.append(base_config)
    return configs


if __name__ == "__main__":
    try:
        config = load_avito_config("config.toml")
    except Exception as err:
        logger.error(f"Ошибка загрузки конфига: {err}")
        exit(1)

    filters_storage = UserFiltersStorage()

    while True:
        try:
            configs_to_run = build_user_configs(config, filters_storage)
            for cfg in configs_to_run:
                parser = AvitoParse(cfg)
                parser.parse()
            if config.one_time_start:
                logger.info("Парсинг завершен т.к. включён one_time_start в настройках")
                break
            logger.info(f"Парсинг завершен. Пауза {config.pause_general} сек")
            time.sleep(config.pause_general)
        except Exception as err:
            logger.error(f"Произошла ошибка {err}. Будет повторный запуск через 30 сек.")
            time.sleep(30)
