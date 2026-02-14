import asyncio
import os
import re
from datetime import datetime, timedelta, timezone
from typing import Optional

from aiogram import Bot, Dispatcher, F
from aiogram.client.default import DefaultBotProperties
from aiogram.client.session.aiohttp import AiohttpSession
from aiogram.enums import ParseMode
from aiogram.filters import CommandStart, Command
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.types import (
    BotCommand,
    CallbackQuery,
    FSInputFile,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    Message,
    ReplyKeyboardRemove,
)

from pathlib import Path

from load_config import _load_dotenv_simple, _parse_chat_ids
from user_filters import (
    MIN_INTERVAL_SECONDS,
    DEFAULT_INTERVAL_SECONDS,
    UserFiltersStorage,
    UserProfile,
)
from paths_helper import user_xlsx_path, user_stats_xlsx_path
from loguru import logger

storage = UserFiltersStorage()
START_STICKER_ID = "CAACAgIAAxkBAAEIB0Zk3u7P5AnbcW2CYwiVdc0GqORdzAACnRcAAnlc4Ub1Z4VHKakiOTQE"
SUCCESS_STICKER_ID = "CAACAgIAAxkBAAEIB0hk3vZPtruzWIEksIB0wbn5omfQUAACUxAAAv1p2Ep-jMxDPZWSQjQE"
HELP_STICKER_ID = "CAACAgIAAxkBAAEIB1Zk4A7WhpnwsnRwJhi3F7S-MUz46QACyB0AAo5buUtxB4ezgmO0hDQE"
EXPORT_HINT = (
    "Готово! Как только я закончу проверять указанные запросы с твоими фильтрами, пришлю свежий XLSX."
    "\nЕсли это первый запуск, дождись окончания цикла — после него появятся данные."
)
HELP_TEXT = (
    "💗 *Как пользоваться ботом*\n"
    "1. Нажми *«Добавить запрос»* и перечисли один или несколько поисковых запросов (например, `Pioneer DDJ FLX4`).\n"
    "2. Затем я предложу настроить фильтр: регион, цены, доставку, интервал проверки в секундах.\n"
    "3. Получай пуши с розовыми сердечками, скачивай XLSX в любой момент.\n"
    "4. Кнопки в меню ведут по шагам, а если что — пишите мне, может чем помогу 💖"
)

ADD_QUERY_LABEL = "Добавить запрос"
MY_QUERIES_LABEL = "Мои запросы"
SETTINGS_LABEL = "Настройки"
XLSX_LABEL = "Скачать XLSX"
HELP_LABEL = "Помощь"

MENU_KB = ReplyKeyboardRemove()

DEFAULT_MAX_AGE_DAYS = 7
MAX_MAX_AGE_DAYS = 3650  # 10 years

def get_allowed_chat_ids() -> set[int]:
    env_value = os.getenv("TG_CHAT_IDS") or os.getenv("TG_CHAT_ID")
    if not env_value:
        return set()
    parsed = _parse_chat_ids(env_value)
    allowed: set[int] = set()
    for item in parsed:
        try:
            allowed.add(int(item))
        except ValueError:
            continue
    return allowed

ALLOWED_CHAT_IDS: set[int] = set()

def _build_main_menu_inline() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="➕ Новый запрос", callback_data="menu:add")],
            [InlineKeyboardButton(text="📋 Мои запросы", callback_data="menu:list")],
            [InlineKeyboardButton(text="⚙️ Настройки фильтров", callback_data="menu:settings")],
            [InlineKeyboardButton(text="📩 Скачать XLSX", callback_data="menu:xlsx")],
            [InlineKeyboardButton(text="📊 Статистика XLSX", callback_data="menu:stats")],
            [InlineKeyboardButton(text="❓ Помощь", callback_data="menu:help")],
        ]
    )


MAIN_MENU_INLINE = _build_main_menu_inline()

REGION_LABELS = {
    "all": "Все регионы",
    "moscow": "Москва",
    "mo": "МО",
    "moscow_mo": "Москва и МО",
}

DELIVERY_LABELS = {
    "any": "Любой способ",
    "delivery_only": "Только доставка",
    "pickup_only": "Без доставки",
}


class FilterForm(StatesGroup):
    query = State()
    region = State()
    min_price = State()
    max_price = State()
    interval = State()
    age = State()
    delivery = State()
    track = State()


def region_keyboard(selected: Optional[str], prefix: str) -> InlineKeyboardMarkup:
    rows = []
    for value, title in REGION_LABELS.items():
        mark = "✅ " if value == selected else ""
        rows.append([InlineKeyboardButton(text=f"{mark}{title}", callback_data=f"{prefix}:{value}")])
    return InlineKeyboardMarkup(inline_keyboard=rows)


def delivery_keyboard(selected: Optional[str], prefix: str) -> InlineKeyboardMarkup:
    rows = []
    for value, title in DELIVERY_LABELS.items():
        mark = "✅ " if value == selected else ""
        rows.append([InlineKeyboardButton(text=f"{mark}{title}", callback_data=f"{prefix}:{value}")])
    return InlineKeyboardMarkup(inline_keyboard=rows)


def track_keyboard(selected: bool) -> InlineKeyboardMarkup:
    on_text = "✅ Вкл" if selected else "Вкл"
    off_text = "Выкл" if selected else "✅ Выкл"
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text=on_text, callback_data="wizard_track:1")],
            [InlineKeyboardButton(text=off_text, callback_data="wizard_track:0")],
        ]
    )


async def send_main_menu(message: Message, text: Optional[str] = None):
    hint = text or "Выбери действие из меню или просто жми на розовые кнопки ниже 💞"
    await message.answer(hint, reply_markup=MAIN_MENU_INLINE)


async def send_help(target_message: Message):
    if HELP_STICKER_ID:
        try:
            await target_message.answer_sticker(HELP_STICKER_ID)
        except Exception:
            pass
    await target_message.answer(HELP_TEXT, parse_mode=ParseMode.MARKDOWN)


async def send_xlsx_file(target_message: Message):
    path = user_xlsx_path(target_message.chat.id)
    if not path.exists():
        logger.info("Запрос XLSX: у пользователя {} ещё нет данных", target_message.chat.id)
        await target_message.answer(
            "Пока нечего выгружать 💞 Добавь поисковый запрос и дождись первой проверки — тогда появится файл."
        )
        return
    try:
        doc = FSInputFile(str(path))
        await target_message.answer_document(
            document=doc,
            caption="Последняя выгрузка по твоим фильтрам",
        )
        logger.info("Отправлен XLSX пользователю {}", target_message.chat.id)
    except Exception as err:
        logger.exception("Ошибка отправки XLSX пользователю {}: {}", target_message.chat.id, err)
        await target_message.answer(f"Не удалось отправить файл: {err}")


async def send_stats_xlsx_file(target_message: Message):
    chat_id = target_message.chat.id
    try:
        # Heavy IO: build stats in a thread to keep the event loop responsive.
        def _build():
            from stats_service import StatsDB

            db = StatsDB()
            today = datetime.now(timezone.utc).date()
            start_7 = today - timedelta(days=6)
            since_7 = start_7.isoformat()

            daily_rows = db.get_daily_rows(str(chat_id), since_date=since_7)
            by_date = {r.date: r for r in daily_rows}

            daily = []
            d = start_7
            while d <= today:
                key = d.isoformat()
                daily.append((key, by_date.get(key)))
                d += timedelta(days=1)

            path = db.export_xlsx(str(chat_id), user_stats_xlsx_path(chat_id), days=30)
            return str(path), daily

        path, daily = await asyncio.to_thread(_build)

        # Pretty 7-day summary message (UTC).
        total_req = total_ok = total_block = total_429 = 0
        header = f"{'Дата (UTC)':<10}  {'Запр':>4}  {'OK':>3}  {'Блок':>4}  {'429':>3}"
        table = [header]
        for date_utc, r in daily:
            req = int(getattr(r, "requests_total", 0) or 0) if r else 0
            ok = int(getattr(r, "ok_total", 0) or 0) if r else 0
            blocked = int(getattr(r, "blocked_total", 0) or 0) if r else 0
            rl = int(getattr(r, "rate_limited_total", 0) or 0) if r else 0
            total_req += req
            total_ok += ok
            total_block += blocked
            total_429 += rl
            table.append(f"{date_utc:<10}  {req:>4}  {ok:>3}  {blocked:>4}  {rl:>3}")

        msg_lines = [
            "📊 Запросы по дням (UTC), последние 7 дней:",
            "```",
            *table,
            "```",
            f"Итого за {len(daily)} дн.: {total_req} (OK {total_ok}, блок {total_block}, 429 {total_429})",
            "XLSX: статистика за последние 30 дней",
        ]
        await target_message.answer("\n".join(msg_lines), disable_notification=True)

        doc = FSInputFile(str(path))
        await target_message.answer_document(
            document=doc,
            caption="Статистика (XLSX) за последние 30 дней",
            disable_notification=True,
        )
        logger.info("Отправлен stats XLSX пользователю {}", chat_id)
    except Exception as err:
        logger.exception("Ошибка отправки stats XLSX пользователю {}: {}", chat_id, err)
        await target_message.answer(f"Не удалось сформировать статистику: {err}")



async def start_cmd(message: Message, state: FSMContext):
    if ALLOWED_CHAT_IDS and message.chat.id not in ALLOWED_CHAT_IDS:
        logger.warning("Доступ к боту отклонён (chat_id={})", message.chat.id)
        await message.answer("👮 Доступ ограничен. Обратитесь к команде ЧВЖ для доступа.")
        return
    await state.clear()
    storage.ensure_user(message.chat.id, message.from_user.username)
    logger.info(
        "Пользователь {} начал работу (username={})",
        message.chat.id,
        message.from_user.username,
    )
    if START_STICKER_ID:
        try:
            await message.answer_sticker(START_STICKER_ID)
        except Exception:
            pass
    await message.answer(
        "💗 Привет! Я помогу настроить поисковые запросы и фильтры Авито. Просто нажимай на кнопки — и всё будет розово‑понятно ✨",
        reply_markup=MENU_KB,
    )
    await send_main_menu(message, "Главное меню:")


async def menu_cmd(message: Message):
    if ALLOWED_CHAT_IDS and message.chat.id not in ALLOWED_CHAT_IDS:
        return
    await send_main_menu(message, "Главное меню:")


async def xlsx_cmd(message: Message):
    if ALLOWED_CHAT_IDS and message.chat.id not in ALLOWED_CHAT_IDS:
        return
    await send_xlsx_file(message)


async def stats_cmd(message: Message):
    if ALLOWED_CHAT_IDS and message.chat.id not in ALLOWED_CHAT_IDS:
        return
    await send_stats_xlsx_file(message)


async def help_cmd(message: Message):
    if ALLOWED_CHAT_IDS and message.chat.id not in ALLOWED_CHAT_IDS:
        return
    await send_help(message)


async def add_filter_cmd(message: Message, state: FSMContext):
    if ALLOWED_CHAT_IDS and message.chat.id not in ALLOWED_CHAT_IDS:
        return
    logger.info("Пользователь {} запускает мастер добавления запроса", message.chat.id)
    await start_filter_wizard(message, state)


async def cancel_cmd(message: Message, state: FSMContext):
    await state.clear()
    await message.answer("Действие отменено", reply_markup=MENU_KB)
    await send_main_menu(message)


async def add_filter_entry(message: Message, state: FSMContext):
    if ALLOWED_CHAT_IDS and message.chat.id not in ALLOWED_CHAT_IDS:
        return
    if message.text != ADD_QUERY_LABEL:
        return
    logger.info("Пользователь {} нажал кнопку {}", message.chat.id, ADD_QUERY_LABEL)
    await start_filter_wizard(message, state)


async def download_xlsx(message: Message):
    if ALLOWED_CHAT_IDS and message.chat.id not in ALLOWED_CHAT_IDS:
        return
    if message.text != XLSX_LABEL:
        return
    await send_xlsx_file(message)


async def help_text(message: Message):
    if ALLOWED_CHAT_IDS and message.chat.id not in ALLOWED_CHAT_IDS:
        return
    if message.text != HELP_LABEL:
        return
    await send_help(message)


async def queries_cmd(message: Message):
    if ALLOWED_CHAT_IDS and message.chat.id not in ALLOWED_CHAT_IDS:
        return
    await render_queries(message)


async def settings_cmd(message: Message):
    if ALLOWED_CHAT_IDS and message.chat.id not in ALLOWED_CHAT_IDS:
        return
    await render_settings(message)


def get_bot_commands() -> list[BotCommand]:
    return [
        BotCommand(command="menu", description="Главное меню"),
        BotCommand(command="add", description="Добавить запрос"),
        BotCommand(command="filters", description="Мои запросы"),
        BotCommand(command="settings", description="Настройки фильтров по умолчанию"),
        BotCommand(command="xlsx", description="Скачать XLSX"),
        BotCommand(command="stats", description="Статистика XLSX"),
        BotCommand(command="help", description="Памятка по боту"),
        BotCommand(command="cancel", description="Отменить текущее действие"),
    ]


async def start_filter_wizard(target: Message, state: FSMContext, profile: Optional[UserProfile] = None):
    profile = profile or storage.get_user_profile(target.chat.id)
    await state.clear()
    await state.set_state(FilterForm.query)
    await state.update_data(mode="create", profile=profile.__dict__, sort_new=profile.default_sort_new)
    await target.answer(
        "🪄 Введи один или несколько поисковых запросов.\n"
        "Можно через запятую или с новой строки (пример: `Pioneer DDJ FLX4, iPhone 16 Pro`).\n"
        "Если нужен точный запрос — оберни его в кавычки: `\"айфон 16\"`.",
        parse_mode=ParseMode.MARKDOWN,
    )


async def menu_callback(callback: CallbackQuery, state: FSMContext):
    await callback.answer()
    await state.clear()
    action = callback.data.split(":", 1)[1]
    if action == "add":
        profile = storage.get_user_profile(callback.from_user.id)
        await start_filter_wizard(callback.message, state, profile=profile)
    elif action == "xlsx":
        await send_xlsx_file(callback.message)
    elif action == "stats":
        await send_stats_xlsx_file(callback.message)
    elif action == "help":
        await send_help(callback.message)
        await send_main_menu(callback.message, "Нужно что-то ещё?")
        return
    else:
        if action == "list":
            await render_queries(callback.message)
        else:
            await render_settings(callback.message)
        await send_main_menu(callback.message, "Нужно что-то ещё?")


async def process_query(message: Message, state: FSMContext):
    data = await state.get_data()
    current_filter = data.get("edit_filter")
    text = message.text.strip()
    queries: list[str]
    if data.get("mode") == "edit" and text == "-":
        queries = [current_filter.get("text")]
    else:
        queries = _extract_queries(text)
    if not queries:
        await message.answer("Нужно указать хотя бы один поисковый запрос")
        return
    await state.update_data(query=queries[0], queries=queries)
    profile = UserProfile(**data["profile"])
    selected = current_filter.get("region") if current_filter else profile.default_region
    await state.set_state(FilterForm.region)
    await message.answer("🌍 Выбери регион поиска", reply_markup=region_keyboard(selected, "wizard_region"))


async def process_region(callback: CallbackQuery, state: FSMContext):
    value = callback.data.split(":", 1)[1]
    await state.update_data(region=value)
    await callback.answer()
    await state.set_state(FilterForm.min_price)
    await callback.message.answer("💰 Минимальная цена или '-' если всё равно")


def _parse_price(text: str, default: Optional[int] = None) -> Optional[int]:
    text = text.strip()
    if text in {"-", ""}:
        return default
    if text.isdigit():
        value = int(text)
        return value
    return None


def _extract_queries(text: str) -> list[str]:
    parts = re.split(r"[,\n]+", text)
    queries: list[str] = []
    for part in parts:
        value = part.strip()
        if value and value not in queries:
            queries.append(value)
    return queries


def _parse_interval(text: str, default: Optional[int]) -> Optional[int]:
    text = text.strip()
    if text in {"-", ""}:
        return default
    if text.isdigit():
        value = int(text)
        if value >= MIN_INTERVAL_SECONDS:
            return value
    return None


def _seconds_to_days(seconds: Optional[int]) -> int:
    try:
        val = int(seconds or 0)
    except Exception:
        return 0
    if val <= 0:
        return 0
    day = 24 * 60 * 60
    return int((val + day - 1) // day)


def _parse_days(text: str, default: int) -> Optional[int]:
    text = text.strip()
    if text in {"-", ""}:
        return default
    if text.isdigit():
        return int(text)
    return None


async def process_min_price(message: Message, state: FSMContext):
    data = await state.get_data()
    current = data.get("edit_filter", {}).get("min_price")
    value = _parse_price(message.text, current)
    if value is None and message.text.strip() not in {"-", ""}:
        await message.answer("Нужно число или '-' для пропуска")
        return
    await state.update_data(min_price=value)
    await state.set_state(FilterForm.max_price)
    await message.answer("💰 Максимальная цена или '-' чтобы пропустить")


async def process_max_price(message: Message, state: FSMContext):
    data = await state.get_data()
    current = data.get("edit_filter", {}).get("max_price")
    value = _parse_price(message.text, current)
    if value is None and message.text.strip() not in {"-", ""}:
        await message.answer("Нужно число или '-' для пропуска")
        return
    await state.update_data(max_price=value)
    current_interval = data.get("edit_filter", {}).get("interval_seconds") or DEFAULT_INTERVAL_SECONDS
    await state.set_state(FilterForm.interval)
    await message.answer(
        f"⏱ Как часто проверять? Напиши число в секундах (минимум {MIN_INTERVAL_SECONDS}) "
        f"или '-' чтобы оставить {current_interval} сек.",
    )


async def process_interval(message: Message, state: FSMContext):
    data = await state.get_data()
    current = data.get("edit_filter", {}).get("interval_seconds") or DEFAULT_INTERVAL_SECONDS
    value = _parse_interval(message.text, current)
    if value is None:
        await message.answer(
            f"Пожалуйста, число в секундах, минимум {MIN_INTERVAL_SECONDS}, или '-' чтобы оставить {current}."
        )
        return
    await state.update_data(interval=value)
    current_seconds = data.get("edit_filter", {}).get("max_age_seconds")
    if current_seconds is None:
        default_days = 0 if data.get("mode") == "edit" else DEFAULT_MAX_AGE_DAYS
    else:
        default_days = _seconds_to_days(current_seconds)
    await state.update_data(max_age_default_days=default_days)
    await state.set_state(FilterForm.age)
    dash_hint = "без ограничения" if default_days <= 0 else f"{default_days} дн."
    await message.answer(
        "⏳ За сколько дней показывать объявления?\n"
        "0 = без ограничения.\n"
        f"Напиши число дней (например 7) или '-' чтобы оставить {dash_hint}."
    )
    return

async def process_age(message: Message, state: FSMContext):
    data = await state.get_data()
    default_days = int(data.get("max_age_default_days") or 0)
    days = _parse_days(message.text, default_days)
    if days is None or days < 0 or days > MAX_MAX_AGE_DAYS:
        await message.answer(f"Нужно число дней от 0 до {MAX_MAX_AGE_DAYS} или '-' чтобы оставить как есть.")
        return
    await state.update_data(max_age_seconds=int(days) * 24 * 60 * 60)
    profile = UserProfile(**data["profile"])
    current_delivery = data.get("edit_filter", {}).get("delivery") or profile.default_delivery
    await state.set_state(FilterForm.delivery)
    await message.answer(
        "🚚 Выбери вариант доставки",
        reply_markup=delivery_keyboard(current_delivery, "wizard_delivery"),
    )


async def process_delivery(callback: CallbackQuery, state: FSMContext):
    value = callback.data.split(":", 1)[1]
    await state.update_data(delivery=value)
    await callback.answer()
    data = await state.get_data()
    current_track = data.get("edit_filter", {}).get("track_price_changes")
    if current_track is None:
        profile = UserProfile(**data["profile"])
        current_track = profile.default_track_price
    await state.set_state(FilterForm.track)
    await callback.message.answer("🔔 Отслеживать изменение цены?", reply_markup=track_keyboard(current_track))


async def process_track(callback: CallbackQuery, state: FSMContext):
    track_value = callback.data.endswith(":1")
    await state.update_data(track_price=track_value)
    await callback.answer()
    data = await state.get_data()
    chat_id = callback.message.chat.id
    interval_value = data.get("interval")
    queries = data.get("queries") or [data.get("query")]
    if data.get("mode") == "edit":
        filt = data["edit_filter"]
        interval_to_use = interval_value if interval_value is not None else filt.get("interval_seconds")
        max_age_seconds = data.get("max_age_seconds")
        if max_age_seconds is None:
            max_age_seconds = filt.get("max_age_seconds")
        storage.update_filter(
            filt["id"],
            chat_id=chat_id,
            text=queries[0] if queries else filt["text"],
            region=data.get("region", filt["region"]),
            min_price=data.get("min_price", filt.get("min_price")),
            max_price=data.get("max_price", filt.get("max_price")),
            delivery=data.get("delivery", filt.get("delivery")),
            sort_new=filt.get("sort_new"),
            track_price_changes=track_value,
            max_age_seconds=max_age_seconds,
            interval_seconds=interval_to_use,
        )
        logger.info(
            "Параметры фильтра #{} обновлены пользователем {}",
            filt["id"],
            chat_id,
        )
        await callback.message.answer("Параметры фильтра обновлены", reply_markup=MENU_KB)
    else:
        interval_to_use = interval_value if interval_value is not None else DEFAULT_INTERVAL_SECONDS
        max_age_seconds = data.get("max_age_seconds")
        if max_age_seconds is None:
            max_age_seconds = DEFAULT_MAX_AGE_DAYS * 24 * 60 * 60
        created = 0
        for query in queries:
            storage.add_filter(
                chat_id=chat_id,
                text=query,
                region=data.get("region", "all"),
                min_price=data.get("min_price"),
                max_price=data.get("max_price"),
                delivery=data.get("delivery", "any"),
                sort_new=data.get("sort_new"),
                track_price_changes=track_value,
                max_age_seconds=max_age_seconds,
                interval_seconds=interval_to_use,
            )
            created += 1
        logger.info(
            "Пользователь {} добавил {} поисковых запросов: {}",
            chat_id,
            created,
            ", ".join(queries),
        )
        await callback.message.answer(
            f"Добавлено поисковых запросов: {created}",
            reply_markup=MENU_KB,
        )
        if SUCCESS_STICKER_ID:
            try:
                await callback.message.answer_sticker(SUCCESS_STICKER_ID)
            except Exception:
                pass
    await callback.message.answer(EXPORT_HINT)
    await send_main_menu(callback.message, "Готово! Что дальше?")
    await state.clear()


def format_filter(row) -> str:
    region = REGION_LABELS.get(row["region"], row["region"])
    delivery = DELIVERY_LABELS.get(row["delivery"], row["delivery"])
    min_price = row["min_price"] if row["min_price"] is not None else "—"
    max_price = row["max_price"] if row["max_price"] is not None else "—"
    track = "Вкл" if row["track_price_changes"] else "Выкл"
    interval = row["interval_seconds"] or DEFAULT_INTERVAL_SECONDS
    try:
        max_age_seconds = row["max_age_seconds"]
    except Exception:
        max_age_seconds = None
    max_age_days = _seconds_to_days(max_age_seconds)
    age_label = "без ограничения" if max_age_days <= 0 else f"не старше {max_age_days} дн."
    parts = [
        f"🔎 Запрос: *{row['text']}*",
        f"📍 Фильтр — регион: {region}",
        f"💰 Фильтр — цена: {min_price} — {max_price}",
        f"🚚 Фильтр — доставка: {delivery}",
        f"🔔 Фильтр — отслеживание цены: {track}",
        f"⏱ Фильтр — проверка каждые {interval} сек",
    ]
    parts.insert(3, f"⏳ Фильтр — объявления: {age_label}")
    return "\n".join(parts)


def filter_keyboard(row) -> InlineKeyboardMarkup:
    delivery_text = "Только доставка: " + ("Выкл" if row["delivery"] == "delivery_only" else "Вкл")
    track_text = "Отслеживание цены: " + ("Выкл" if not row["track_price_changes"] else "Вкл")
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="Изменить фильтр", callback_data=f"filter_edit:{row['id']}")],
            [InlineKeyboardButton(text="Удалить", callback_data=f"filter_delete:{row['id']}")],
            [InlineKeyboardButton(text=delivery_text, callback_data=f"filter_delivery:{row['id']}")],
            [InlineKeyboardButton(text=track_text, callback_data=f"filter_track:{row['id']}")],
        ]
    )


async def show_queries(message: Message):
    if message.text != MY_QUERIES_LABEL:
        return
    await render_queries(message)


async def render_queries(target: Message):
    rows = storage.list_filters(target.chat.id)
    if not rows:
        await target.answer(f"Пока нет поисковых запросов 💗 Нажми «{ADD_QUERY_LABEL}» и мы быстро всё настроим.")
        return
    for row in rows:
        await target.answer(
            format_filter(row),
            reply_markup=filter_keyboard(row),
            parse_mode=ParseMode.MARKDOWN,
        )


async def show_settings(message: Message):
    if message.text != SETTINGS_LABEL:
        return
    await render_settings(message)


async def render_settings(target: Message):
    profile = storage.get_user_profile(target.chat.id)
    text = (
        "Настройки по умолчанию:\n"
        f"• Регион: {REGION_LABELS.get(profile.default_region, profile.default_region)}\n"
        f"• Доставка: {DELIVERY_LABELS.get(profile.default_delivery, profile.default_delivery)}\n"
        f"• Отслеживание цены: {'Вкл' if profile.default_track_price else 'Выкл'}\n"
        f"• Сортировка по новым: {'Вкл' if profile.default_sort_new else 'Выкл'}\n"
        "• Интервал настраивается для каждого добавляемого запроса"
    )
    kb = InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="Изменить регион", callback_data="settings_region")],
            [InlineKeyboardButton(text="Изменить доставку", callback_data="settings_delivery")],
            [InlineKeyboardButton(text="Переключить отслеживание", callback_data="settings_track")],
            [InlineKeyboardButton(text="Переключить сортировку", callback_data="settings_sort")],
        ]
    )
    await target.answer(text, reply_markup=kb)


async def settings_region(callback: CallbackQuery):
    profile = storage.get_user_profile(callback.message.chat.id)
    await callback.answer()
    await callback.message.answer(
        "Выбери регион по умолчанию",
        reply_markup=region_keyboard(profile.default_region, "settings_region_value"),
    )


async def settings_region_value(callback: CallbackQuery):
    value = callback.data.split(":", 1)[1]
    storage.update_user_defaults(callback.message.chat.id, default_region=value)
    await callback.answer("Обновлено")


async def settings_delivery(callback: CallbackQuery):
    profile = storage.get_user_profile(callback.message.chat.id)
    await callback.answer()
    await callback.message.answer(
        "Выбери доставку по умолчанию",
        reply_markup=delivery_keyboard(profile.default_delivery, "settings_delivery_value"),
    )


async def settings_delivery_value(callback: CallbackQuery):
    value = callback.data.split(":", 1)[1]
    storage.update_user_defaults(callback.message.chat.id, default_delivery=value)
    await callback.answer("Обновлено")


async def settings_toggle(callback: CallbackQuery):
    action = callback.data.replace("settings_", "", 1)
    profile = storage.get_user_profile(callback.message.chat.id)
    if action == "track":
        storage.update_user_defaults(callback.message.chat.id, default_track_price=not profile.default_track_price)
    else:
        storage.update_user_defaults(callback.message.chat.id, default_sort_new=not profile.default_sort_new)
    await callback.answer("Обновлено")


async def filter_delete(callback: CallbackQuery):
    filter_id = int(callback.data.split(":", 1)[1])
    storage.delete_filter(filter_id, callback.message.chat.id)
    await callback.answer("Удалено")
    await callback.message.edit_text("Запрос удалён вместе с фильтром")


async def filter_toggle_delivery(callback: CallbackQuery):
    filter_id = int(callback.data.split(":", 1)[1])
    new_value = storage.toggle_delivery_only(filter_id, callback.message.chat.id)
    row = storage.get_filter(filter_id, callback.message.chat.id)
    await callback.answer("Сохранено")
    if row:
        await callback.message.edit_text(
            format_filter(row),
            reply_markup=filter_keyboard(row),
            parse_mode=ParseMode.MARKDOWN,
        )


async def filter_toggle_track(callback: CallbackQuery):
    filter_id = int(callback.data.split(":", 1)[1])
    storage.toggle_price_tracking(filter_id, callback.message.chat.id)
    row = storage.get_filter(filter_id, callback.message.chat.id)
    await callback.answer("Сохранено")
    if row:
        await callback.message.edit_text(
            format_filter(row),
            reply_markup=filter_keyboard(row),
            parse_mode=ParseMode.MARKDOWN,
        )


async def filter_edit(callback: CallbackQuery, state: FSMContext):
    filter_id = int(callback.data.split(":", 1)[1])
    row = storage.get_filter(filter_id, callback.message.chat.id)
    if not row:
        await callback.answer("Не найдено")
        return
    row_dict = dict(row)
    row_dict["track_price_changes"] = bool(row_dict.get("track_price_changes"))
    await state.set_state(FilterForm.query)
    await state.update_data(
        mode="edit",
        edit_filter=row_dict,
        profile=storage.get_user_profile(callback.message.chat.id).__dict__,
        sort_new=row_dict.get("sort_new"),
    )
    await callback.answer()
    await callback.message.answer("Введи новый запрос или '-' чтобы оставить прежний")


async def main():
    _load_dotenv_simple(Path(__file__).resolve().parent)
    global ALLOWED_CHAT_IDS
    ALLOWED_CHAT_IDS = get_allowed_chat_ids()
    token = os.getenv("TG_BOT_TOKEN")
    if not token:
        raise RuntimeError("TG_BOT_TOKEN не задан")
    session = AiohttpSession(timeout=30)
    bot = Bot(token=token, default=DefaultBotProperties(parse_mode=ParseMode.MARKDOWN), session=session)
    dp = Dispatcher()
    dp.message.register(start_cmd, CommandStart())
    dp.message.register(cancel_cmd, Command("cancel"))
    dp.message.register(menu_cmd, Command("menu"))
    dp.message.register(add_filter_cmd, Command("add"))
    dp.message.register(queries_cmd, Command("filters"))
    dp.message.register(settings_cmd, Command("settings"))
    dp.message.register(xlsx_cmd, Command("xlsx"))
    dp.message.register(stats_cmd, Command("stats"))
    dp.message.register(help_cmd, Command("help"))
    dp.message.register(add_filter_entry, F.text == ADD_QUERY_LABEL)
    dp.message.register(show_queries, F.text == MY_QUERIES_LABEL)
    dp.message.register(show_settings, F.text == SETTINGS_LABEL)
    dp.message.register(download_xlsx, F.text == XLSX_LABEL)
    dp.message.register(help_text, F.text == HELP_LABEL)
    dp.message.register(process_query, FilterForm.query)
    dp.message.register(process_interval, FilterForm.interval)
    dp.message.register(process_age, FilterForm.age)
    dp.callback_query.register(process_region, FilterForm.region, F.data.startswith("wizard_region:"))
    dp.message.register(process_min_price, FilterForm.min_price)
    dp.message.register(process_max_price, FilterForm.max_price)
    dp.callback_query.register(process_delivery, FilterForm.delivery, F.data.startswith("wizard_delivery:"))
    dp.callback_query.register(process_track, FilterForm.track, F.data.startswith("wizard_track:"))
    dp.callback_query.register(menu_callback, F.data.startswith("menu:"))
    dp.callback_query.register(settings_region, F.data == "settings_region")
    dp.callback_query.register(settings_delivery, F.data == "settings_delivery")
    dp.callback_query.register(settings_region_value, F.data.startswith("settings_region_value:"))
    dp.callback_query.register(settings_delivery_value, F.data.startswith("settings_delivery_value:"))
    dp.callback_query.register(settings_toggle, F.data.in_({"settings_track", "settings_sort"}))
    dp.callback_query.register(filter_delete, F.data.startswith("filter_delete:"))
    dp.callback_query.register(filter_toggle_delivery, F.data.startswith("filter_delivery:"))
    dp.callback_query.register(filter_toggle_track, F.data.startswith("filter_track:"))
    dp.callback_query.register(filter_edit, F.data.startswith("filter_edit:"))
    await bot.set_my_commands(get_bot_commands())
    await dp.start_polling(bot)


if __name__ == "__main__":
    asyncio.run(main())
