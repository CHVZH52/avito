import asyncio
import os
import time
from pathlib import Path
import threading

from loguru import logger

import bot_app
from common_data import HEADERS
from free_proxy_robin import get_free_proxy_pool
from load_config import load_avito_config, _load_dotenv_simple
from scheduler import FiltersScheduler
from user_filters import UserFiltersStorage


def _warm_free_proxy_pool(config) -> None:
    if not bool(getattr(config, "use_free_proxies", False)):
        return
    disable = os.getenv("AVITO_DISABLE_FREE_PROXY_WARMUP")
    if disable is not None and disable.strip().lower() in {"1", "true", "yes", "on"}:
        return
    try:
        # Uses cache if present; otherwise will refresh once in background.
        get_free_proxy_pool(config, force_refresh=False, headers_for_avito=dict(HEADERS))
    except Exception as e:
        logger.debug("free proxy warmup failed: {}", str(e)[:180])


def main():
    # Load .env early so bot_app module-level guards (and scheduler) see env vars.
    _load_dotenv_simple(Path(__file__).resolve().parent)
    config = load_avito_config("config.toml")

    # Warm free-proxy cache in background so first scheduler runs don't stall on scraping/validation.
    threading.Thread(target=_warm_free_proxy_pool, args=(config,), daemon=True, name="free-proxy-warmup").start()

    storage = UserFiltersStorage()
    scheduler = FiltersScheduler(config, storage)
    scheduler.start()
    logger.info("Планировщик готов к работе")

    token = os.getenv("TG_BOT_TOKEN") or os.getenv("TG_TOKEN") or os.getenv("AVITO_TG_TOKEN") or (config.tg_token or "")
    disable_bot = os.getenv("DISABLE_TELEGRAM_BOT")
    if disable_bot is not None and disable_bot.strip().lower() in {"1", "true", "yes", "on"}:
        token = ""
    if not token:
        logger.warning("TG_BOT_TOKEN не задан — запускаю только планировщик (без Telegram-бота)")
        try:
            while True:
                time.sleep(3600)
        finally:
            logger.info("Останавливаю планировщик")
            scheduler.stop()
        return

    logger.info("Бот и планировщик готовы к работе")
    try:
        asyncio.run(bot_app.main())
    finally:
        logger.info("Останавливаю планировщик")
        scheduler.stop()


if __name__ == "__main__":
    main()
