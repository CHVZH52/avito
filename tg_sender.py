import re
import requests
import time

from loguru import logger

from models import Item


class SendAdToTg:
    def __init__(self, bot_token: str, chat_id: list, max_retries: int = 5, retry_delay: int = 5):
        self.bot_token = bot_token
        self.chat_id = chat_id
        self.api_base = f"https://api.telegram.org/bot{self.bot_token}"
        self.max_retries = max_retries
        self.retry_delay = retry_delay

    def __send_to_tg(self, chat_id: str | int, ad: Item = None, msg: str = None):
        if msg:
            payload = {
                "chat_id": chat_id,
                "text": msg,
                "parse_mode": "markdown",
            }
            self._try_send("sendMessage", payload, max_retries=2)
            return

        text, photo = self.format_ad(ad)
        if photo:
            photo_payload = {
                "chat_id": chat_id,
                "photo": photo,
                "caption": text,
                "parse_mode": "markdown",
            }
            if self._try_send("sendPhoto", photo_payload, self.max_retries):
                return

        message_payload = {
            "chat_id": chat_id,
            "text": text,
            "parse_mode": "markdown",
            "disable_web_page_preview": True,
        }
        self._try_send("sendMessage", message_payload, self.max_retries)

    def send_to_tg(self, ad: Item = None, msg: str = None):
        for chat_id in self.chat_id:
            self.__send_to_tg(chat_id=chat_id, ad=ad, msg=msg)

    @staticmethod
    def _escape(text: str) -> str:
        return re.sub(r"([_*`\[\]])", r"\\\1", text)

    @staticmethod
    def _format_price(value: int | None) -> str:
        try:
            integer = int(value)
        except Exception:
            integer = 0
        return f"{integer:,}".replace(",", " ")

    @staticmethod
    def _extract_photo(ad: Item) -> str | None:
        gallery = getattr(ad, "gallery", None)
        if gallery and getattr(gallery, "imageLargeUrl", None):
            return str(gallery.imageLargeUrl)
        images = getattr(ad, "images", None) or []
        for item in images:
            root = getattr(item, "root", {}) or {}
            for url in root.values():
                if url:
                    return str(url)
        return None

    def format_ad(self, ad: Item) -> tuple[str, str | None]:
        title = ad.title or "Без названия"
        escaped_title = self._escape(title)
        url_path = ad.urlPath or ""
        full_url = f"https://www.avito.ru{url_path}" if url_path else f"https://www.avito.ru/{ad.id}"
        short_url = f"https://www.avito.ru/{ad.id}" if ad.id else full_url

        price_value = getattr(getattr(ad, "priceDetailed", None), "value", 0)
        current_price = self._format_price(price_value)
        header: str
        if ad.price_change_from and ad.price_change_from != price_value:
            old_price = self._format_price(ad.price_change_from)
            header = f"💸 Цена изменилась: {old_price} ₽ → *{current_price} ₽*"
        else:
            header = f"💰 Цена: *{current_price} ₽*"

        lines = [
            header,
            f"*{escaped_title}*",
            f"[Открыть объявление]({full_url})",
            # short_url,
        ]

        region = ad.filter_region_label or getattr(getattr(ad, "location", None), "name", None)
        if not region:
            region = getattr(getattr(ad, "geo", None), "formattedAddress", None)
        if region:
            lines.append(f"📍 {self._escape(region)}")

        delivery_available = bool(getattr(getattr(ad, "contacts", None), "delivery", False))
        lines.append(f"🚚 {'Есть доставка' if delivery_available else 'Самовывоз'}")

        if ad.filter_title:
            lines.append(f"🔎 Поиск: {self._escape(ad.filter_title)}")
        photo_url = self._extract_photo(ad)
        return "\n".join(lines), photo_url

    def _try_send(self, method: str, payload: dict, max_retries: int) -> bool:
        url = f"{self.api_base}/{method}"
        for attempt in range(1, max_retries + 1):
            try:
                response = requests.post(url, json=payload, timeout=15)
                if response.status_code == 400:
                    logger.warning("Не удалось отправить сообщение: {}", response.text)
                    return True
                response.raise_for_status()
                logger.debug("Сообщение отправлено ({}, попытка {})", method, attempt)
                return True
            except requests.RequestException as err:
                logger.debug("Ошибка отправки ({}, попытка {}): {}", method, attempt, err)
                if attempt < max_retries:
                    time.sleep(self.retry_delay)
                else:
                    logger.error("Не удалось отправить сообщение после {} попыток", max_retries)
        return False
