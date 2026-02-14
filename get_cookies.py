import asyncio
import random
import httpx
import os
from loguru import logger
from playwright.async_api import async_playwright
from playwright_stealth import Stealth
from typing import Optional, Dict, List

from dto import Proxy, ProxySplit
from playwright_setup import ensure_playwright_installed
from proxy_utils import env_default_proxy_scheme, parse_proxy

MAX_RETRIES = 3
RETRY_DELAY = 10
BAD_IP_TITLE = "проблема с ip"


class PlaywrightClient:
    def __init__(
            self,
            proxy: Proxy = None,
            headless: bool = True,
            user_agent: Optional[str] = None,
            stop_event=None,
            proxy_default_scheme: str | None = None,
    ):
        self.proxy = proxy
        self.proxy_default_scheme = proxy_default_scheme
        self.proxy_split_obj = self.get_proxy_obj()
        self.headless = headless
        self.user_agent = user_agent or "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/140.0.0.0 Safari/537.36"
        self.context = self.page = self.browser = None
        self.stop_event = stop_event

        self.cookie_provider = (os.getenv("AVITO_COOKIE_PROVIDER") or "").strip().lower()
        self._camoufox_ctx = None

    @staticmethod
    def check_protocol(ip_port: str) -> str:
        if "http://" not in ip_port:
            return f"http://{ip_port}"
        return ip_port

    @staticmethod
    def del_protocol(proxy_string: str):
        if "//" in proxy_string:
            return proxy_string.split("//")[1]
        return proxy_string

    def get_proxy_obj(self) -> ProxySplit | None:
        if not self.proxy:
            return None
        parsed = None
        try:
            scheme = str(self.proxy_default_scheme or "").strip().lower()
            if not scheme:
                scheme = env_default_proxy_scheme()
            parsed = parse_proxy(self.proxy.proxy_string, default_scheme=scheme)
        except Exception:
            parsed = None

        if not parsed:
            logger.critical(
                "Прокси в таком формате не поддерживается. Поддерживаемые форматы: "
                "host:port, user:pass@host:port, host:port:user:pass, user:pass:host:port, "
                "а также scheme://host:port (http/https/socks4/socks5)."
            )
            return None

        scheme = parsed.scheme
        if scheme == "socks5h":
            scheme = "socks5"
        server = f"{scheme}://{parsed.host}:{parsed.port}"
        return ProxySplit(
            ip_port=server,
            login=parsed.username or "",
            password=parsed.password or "",
            change_ip_link=self.proxy.change_ip_link,
        )

    @staticmethod
    def parse_cookie_string(cookie_str: str) -> dict:
        return dict(pair.split("=", 1) for pair in cookie_str.split("; ") if "=" in pair)

    async def launch_browser(self):
        if self.cookie_provider == "camoufox":
            try:
                from camoufox.async_api import AsyncCamoufox  # type: ignore
                proxy_cfg = None
                if self.proxy_split_obj:
                    proxy_cfg = {"server": self.proxy_split_obj.ip_port}
                    if self.proxy_split_obj.login:
                        proxy_cfg["username"] = self.proxy_split_obj.login
                        proxy_cfg["password"] = self.proxy_split_obj.password
                self._camoufox_ctx = AsyncCamoufox(
                    headless=self.headless,
                    proxy=proxy_cfg,
                    geoip=True,
                    block_images=True,
                )
                browser = await self._camoufox_ctx.__aenter__()
                self.browser = browser
                self.page = await browser.new_page()
                self.context = None
                return
            except ImportError:
                logger.warning("Camoufox not installed; falling back to Playwright Chromium")
            except Exception as e:
                logger.warning(f"Camoufox failed; falling back to Playwright Chromium: {e}")

        ensure_playwright_installed("chromium")
        stealth = Stealth()
        self.playwright_context = stealth.use_async(async_playwright())
        playwright = await self.playwright_context.__aenter__()
        self.playwright = playwright

        launch_args = {
            "headless": self.headless,
            "chromium_sandbox": False,
            "args": [
                "--disable-blink-features=AutomationControlled",
                "--no-sandbox",
                "--disable-dev-shm-usage",
                "--start-maximized",
                "--window-size=1920,1080",
            ]
        }

        self.browser = await playwright.chromium.launch(**launch_args)

        context_args = {
            "user_agent": self.user_agent,
            "viewport": {"width": 1920, "height": 1080},
            "screen": {"width": 1920, "height": 1080},
            "device_scale_factor": 1,
            "is_mobile": False,
            "has_touch": False,
        }

        if self.proxy_split_obj:
            context_args["proxy"] = {"server": self.proxy_split_obj.ip_port}
            if self.proxy_split_obj.login:
                context_args["proxy"]["username"] = self.proxy_split_obj.login
                context_args["proxy"]["password"] = self.proxy_split_obj.password

        self.context = await self.browser.new_context(**context_args)
        self.page = await self.context.new_page()
        # block images, not use now
        # await self.page.route("**/*", lambda route, request: asyncio.create_task(self._block_images(route, request)))
        await self._stealth(self.page)

    async def load_page(self, url: str):
        await self.page.goto(url=url,
                             timeout=60_000,
                             wait_until="domcontentloaded")

        for attempt in range(10):
            if self.stop_event and self.stop_event.is_set():
                return {}
            can_continue = await self.check_block(self.page, self.context)
            if not can_continue:
                return {}
            raw_cookie = await self.page.evaluate("() => document.cookie")
            cookie_dict = self.parse_cookie_string(raw_cookie)
            if cookie_dict.get("ft"):
                logger.info("Cookies получены")
                return cookie_dict
            await asyncio.sleep(2)

        logger.warning("Не удалось получить cookies")
        return {}

    async def extract_cookies(self, url: str) -> dict:
        try:
            await self.launch_browser()
            return await self.load_page(url)
        finally:
            if self._camoufox_ctx is not None:
                try:
                    await self._camoufox_ctx.__aexit__(None, None, None)
                except Exception:
                    pass
                return
            if hasattr(self, "browser"):
                if self.browser:
                    await self.browser.close()
            if hasattr(self, "playwright"):
                await self.playwright.stop()
            if hasattr(self, "playwright_context") and self.playwright_context:
                await self.playwright_context.__aexit__(None, None, None)

    async def get_cookies(self, url: str) -> dict:
        return await self.extract_cookies(url)

    async def check_block(self, page, context):
        title = await page.title()
        logger.info(f"Не ошибка, а название страницы: {title}")
        if BAD_IP_TITLE in str(title).lower():
            logger.info("IP заблокирован")
            changed = await self.change_ip()
            if not changed:
                logger.warning("Не удалось сменить IP, прекращаю получение cookies без долгого ожидания")
                return False
            try:
                ctx = context or page.context
                await ctx.clear_cookies()
            except Exception:
                pass
            await page.reload(timeout=60 * 1000)
        return True

    async def change_ip(self, retries: int = MAX_RETRIES):
        if not self.proxy_split_obj:
            logger.info("Смена IP недоступна: прокси не задан")
            return False
        if not self.proxy_split_obj.change_ip_link:
            logger.info("Смена IP недоступна: пустой change_ip_link")
            return False
        for attempt in range(1, retries + 1):
            try:
                response = httpx.get(self.proxy_split_obj.change_ip_link + "&format=json", timeout=20)
                if response.status_code == 200:
                    logger.info(f"IP изменён на {response.json().get('new_ip')}")
                    return True
                else:
                    logger.warning(f"[{attempt}/{retries}] Ошибка смены IP: {response.status_code}")
            except httpx.RequestError as e:
                logger.error(f"[{attempt}/{retries}] Ошибка смены IP: {e}")

            if attempt < retries:
                logger.info(f"Повторная попытка сменить IP через {RETRY_DELAY} секунд...")
                await asyncio.sleep(RETRY_DELAY)
            else:
                logger.error("Превышено количество попыток смены IP")
                return False

    @staticmethod
    async def _stealth(page):
        await page.add_init_script("""
            Object.defineProperty(navigator, 'webdriver', { get: () => undefined });
            Object.defineProperty(navigator, 'platform', { get: () => 'Win32' });
            Object.defineProperty(navigator, 'vendor', { get: () => 'Google Inc.' });
            window.chrome = { runtime: {} };
            Object.defineProperty(navigator, 'plugins', { get: () => [1, 2, 3] });
            Object.defineProperty(navigator, 'languages', { get: () => ['en-US', 'en'] });
        """)

    @staticmethod
    async def _block_images(route, request):
        if request.resource_type == "image":
            await route.abort()
        else:
            await route.continue_()


async def get_cookies(
    proxy: Proxy = None,
    headless: bool = True,
    stop_event=None,
    user_agent: str | None = None,
    proxy_default_scheme: str | None = None,
) -> tuple:
    logger.info("Пытаюсь обновить cookies")
    client = PlaywrightClient(
        proxy=proxy,
        headless=headless,
        user_agent=user_agent,
        stop_event=stop_event,
        proxy_default_scheme=proxy_default_scheme,
    )
    ads_id = str(random.randint(1111111111, 9999999999))
    cookies = await client.get_cookies(f"https://www.avito.ru/{ads_id}")
    return cookies, client.user_agent
