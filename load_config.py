import os
import re
from pathlib import Path

import tomllib

from dto import AvitoConfig, SearchQuery, RegionPreset, DeliveryMode

REGION_ALIASES: dict[str, RegionPreset] = {
    "all": "all",
    "все регионы": "all",
    "any": "all",
    "moscow": "moscow",
    "москва": "moscow",
    "moskva": "moscow",
    "moscow_only": "moscow",
    "mo": "mo",
    "московская область": "mo",
    "moscow_region": "mo",
    "moskovskaya_oblast": "mo",
    "moscow_and_mo": "moscow_mo",
    "moscow+mo": "moscow_mo",
    "moskva_i_mo": "moscow_mo",
    "москва и мо": "moscow_mo",
    "moscow_mo": "moscow_mo",
}

DELIVERY_ALIASES: dict[str, DeliveryMode] = {
    "any": "any",
    "все": "any",
    "delivery": "delivery_only",
    "delivery_only": "delivery_only",
    "with_delivery": "delivery_only",
    "доставка": "delivery_only",
    "pickup": "pickup_only",
    "no_delivery": "pickup_only",
    "pickup_only": "pickup_only",
    "без доставки": "pickup_only",
}


def _load_dotenv_simple(start_dir: Path | None = None):
    def parse_and_set(env_path: Path):
        try:
            for line in env_path.read_text(encoding="utf-8").splitlines():
                line = line.strip()
                if not line or line.startswith("#"):
                    continue
                if "=" not in line:
                    continue
                key, value = line.split("=", 1)
                key = key.strip()
                value = value.strip().strip('"').strip("'")
                os.environ.setdefault(key, value)
        except Exception:
            pass

    start = start_dir or Path.cwd()
    for parent in [start, *start.parents]:
        env_file = parent / ".env"
        if env_file.exists() and env_file.is_file():
            parse_and_set(env_file)
            break


def load_avito_config(path: str = "config.toml") -> AvitoConfig:
    with open(path, "rb") as f:
        data = tomllib.load(f)

    avito_section = data["avito"]
    avito_section = avito_section.copy()
    # Allow proxy_string to be either a single string or a list of strings in TOML.
    proxy_value = avito_section.get("proxy_string")
    if isinstance(proxy_value, list):
        avito_section["proxy_string"] = "\n".join(str(x) for x in proxy_value if str(x).strip())
    avito_section["searches"] = _parse_searches(avito_section)
    cfg = AvitoConfig(**avito_section)

    if not cfg.searches and cfg.queries:
        cfg.searches = [
            SearchQuery(
                text=query,
                region=_normalize_region(cfg.region_slug),
                min_price=cfg.min_price if cfg.min_price else None,
                max_price=cfg.max_price if cfg.max_price else None,
                delivery="delivery_only" if cfg.delivery_only else "any",
                sort_new=cfg.sort_new,
            )
            for query in cfg.queries
        ]

    _load_dotenv_simple(Path(path).resolve().parent)

    env_token = os.getenv("AVITO_TG_TOKEN") or os.getenv("TG_TOKEN") or os.getenv("TG_BOT_TOKEN")
    if env_token:
        cfg.tg_token = env_token
    env_chat_ids = os.getenv("TG_CHAT_IDS") or os.getenv("TG_CHAT_ID")
    if env_chat_ids:
        parsed_chat_ids = _parse_chat_ids(env_chat_ids)
        if parsed_chat_ids:
            cfg.tg_chat_id = parsed_chat_ids
    env_skip = os.getenv("SKIP_FIRST_NOTIFICATIONS")
    if env_skip is not None:
        cfg.skip_first_notifications = _to_bool(env_skip, default=cfg.skip_first_notifications)

    # Proxies via env 
    env_proxies = None
    for k in ("AVITO_PROXIES", "AVITO_PROXY_STRING", "PROXY_STRING"):
        if k in os.environ:
            env_proxies = os.environ.get(k)
            break
    if env_proxies is not None:
        val = str(env_proxies)
        if val.strip().lower() in {"-", "0", "false", "off", "none"}:
            val = ""
        cfg.proxy_string = val

    env_change_url = None
    for k in ("AVITO_PROXY_CHANGE_URL", "AVITO_PROXY_CHANGE_IP_URL", "PROXY_CHANGE_URL", "PROXY_CHANGE_IP_URL"):
        if k in os.environ:
            env_change_url = os.environ.get(k)
            break
    if env_change_url is not None:
        val = str(env_change_url)
        if val.strip().lower() in {"-", "0", "false", "off", "none"}:
            val = ""
        cfg.proxy_change_url = val

    # Default proxy scheme for scheme-less proxy strings (e.g. "ip:port:user:pass").
    # Env takes precedence over config.toml.
    env_scheme = os.getenv("AVITO_PROXY_DEFAULT_SCHEME")
    if env_scheme is not None:
        v = str(env_scheme or "").strip().lower()
        if v in {"-", "0", "false", "off", "none"}:
            v = ""
        if v:
            if v == "socks":
                v = "socks5"
            cfg.proxy_default_scheme = v

    # Free proxy pool controls via env (optional).
    env_use_free = os.getenv("AVITO_USE_FREE_PROXIES") or os.getenv("USE_FREE_PROXIES")
    if env_use_free is not None:
        cfg.use_free_proxies = _to_bool(env_use_free, default=cfg.use_free_proxies)

    env_mix = os.getenv("AVITO_FREE_PROXIES_MIX_WITH_USER_PROXIES") or os.getenv("FREE_PROXIES_MIX_WITH_USER_PROXIES")
    if env_mix is not None:
        cfg.free_proxies_mix_with_user_proxies = _to_bool(env_mix, default=cfg.free_proxies_mix_with_user_proxies)

    def _env_int(name: str, default: int) -> int:
        val = os.getenv(name)
        if val is None:
            return default
        try:
            return int(str(val).strip())
        except Exception:
            return default

    cfg.free_proxies_max_pool = _env_int("AVITO_FREE_PROXIES_MAX_POOL", cfg.free_proxies_max_pool)
    cfg.free_proxies_min_pool = _env_int("AVITO_FREE_PROXIES_MIN_POOL", cfg.free_proxies_min_pool)
    cfg.free_proxies_refresh_minutes = _env_int("AVITO_FREE_PROXIES_REFRESH_MINUTES", cfg.free_proxies_refresh_minutes)
    cfg.free_proxies_max_candidates = _env_int("AVITO_FREE_PROXIES_MAX_CANDIDATES", cfg.free_proxies_max_candidates)
    cfg.free_proxies_validate_concurrency = _env_int(
        "AVITO_FREE_PROXIES_VALIDATE_CONCURRENCY", cfg.free_proxies_validate_concurrency
    )

    env_check_avito = os.getenv("AVITO_FREE_PROXIES_CHECK_AVITO")
    if env_check_avito is not None:
        cfg.free_proxies_check_avito = _to_bool(env_check_avito, default=cfg.free_proxies_check_avito)

    env_cache = os.getenv("AVITO_FREE_PROXIES_CACHE_PATH")
    if env_cache is not None:
        cfg.free_proxies_cache_path = str(env_cache or "").strip() or None

    return cfg


def _parse_searches(avito_section: dict) -> list[SearchQuery]:
    raw_searches = avito_section.get("searches") or []
    parsed: list[SearchQuery] = []
    for entry in raw_searches:
        if not isinstance(entry, dict):
            continue
        text = entry.get("text") or entry.get("query")
        if not text:
            continue
        max_age_seconds = _to_int(entry.get("max_age_seconds"))
        if max_age_seconds is None:
            max_age_days = _to_int(entry.get("max_age_days"))
            if max_age_days is not None:
                max_age_seconds = int(max_age_days) * 24 * 60 * 60
        parsed.append(
            SearchQuery(
                text=str(text),
                region=_normalize_region(entry.get("region")),
                min_price=_to_int(entry.get("min_price")),
                max_price=_to_int(entry.get("max_price")),
                delivery=_normalize_delivery(entry.get("delivery")),
                sort_new=_to_bool(entry.get("sort_new")),
                track_price_changes=_to_bool(entry.get("track_price_changes"), default=True),
                max_age_seconds=max_age_seconds,
            )
        )
    return parsed


def _normalize_region(value) -> RegionPreset:
    if value is None:
        return "all"
    key = str(value).strip().lower()
    return REGION_ALIASES.get(key, "all")


def _normalize_delivery(value) -> DeliveryMode:
    if value is None:
        return "any"
    key = str(value).strip().lower()
    return DELIVERY_ALIASES.get(key, "any")


def _to_int(value):
    if value in (None, ""):
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _to_bool(value, default=None):
    if value is None:
        return default
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        val = value.strip().lower()
        if val in {"1", "true", "yes", "on"}:
            return True
        if val in {"0", "false", "no", "off"}:
            return False
    return bool(value)


def _parse_chat_ids(value: str) -> list[str]:
    parts = re.split(r"[\n,;]+", value)
    cleaned = []
    for part in parts:
        candidate = part.strip()
        if candidate:
            cleaned.append(candidate)
    return cleaned
