import subprocess
import sys
import os
from pathlib import Path
from loguru import logger


def ensure_playwright_installed(browser: str = "chromium"):
    try:
        if os.name == "nt" or sys.platform.startswith("win"):
            base = Path(os.path.expanduser("~")) / "AppData" / "Local" / "ms-playwright"
        else:
            base = Path(os.path.expanduser("~")) / ".cache" / "ms-playwright"
        os.environ["PLAYWRIGHT_BROWSERS_PATH"] = str(base)

        from playwright._impl._driver import compute_driver_executable

        result = compute_driver_executable()
        if isinstance(result, tuple):
            driver_path, _ = result
        else:
            driver_path = result

        browsers_exist = os.path.exists(driver_path) or base.exists()

        if not browsers_exist:
            logger.info(f"Playwright не найден в {base}. Устанавливаю {browser}…")
            subprocess.run([sys.executable, "-m", "playwright", "install", browser], check=True)
        else:
            logger.debug(f"Playwright уже установлен, путь: {base}")

    except Exception as e:
        logger.warning(f"Ошибка при установке\проверке Playwright: {e}")
