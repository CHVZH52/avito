# Avito бот-парсер в Telegram 🤖

Проект запускает Telegram-бота и планировщик, которые по расписанию проверяют выдачу Авито по вашим фильтрам и присылают новые объявления в Telegram.

Разработано командой **ЧВЖ** (Forked from https://github.com/Duff89/parser_avito).

## Что умеет ✨

- 🧩 Добавление фильтров прямо в боте (регион, цены, доставка, интервал, “объявления не старше N дней”)
- 🔁 Ротация прокси (свои прокси из `.env`)
- 🆓 Пул бесплатных прокси 
- 📩 XLSX выгрузка результатов: `result/users/<chat_id>/monitoring.xlsx`
- 📊 XLSX статистика запросов: `result/users/<chat_id>/stats.xlsx` (кнопка “Статистика XLSX” или команда `/stats`)
  - лист **По дням**: последние 30 дней, колонка “Запросов всего”
  - лист **Товары**: события “достучались до объявления” (последние записи)

## Быстрый старт (Docker) 🚀

1. Создай `.env` (в репозиторий не коммитим):
   - `TG_BOT_TOKEN=...`
   - опционально: `TG_CHAT_IDS=123456789` (если нужно ограничить доступ)
   - опционально: `AVITO_PROXIES=ip:port:user:pass,ip:port:user:pass`
   - опционально: `AVITO_PROXY_DEFAULT_SCHEME=http` (или `socks5`, если прокси без `scheme://`)
   - опционально: `AVITO_USE_FREE_PROXIES=1`

2. Запусти:

```bash
docker compose up -d --build
docker compose logs -f --tail=200 parser
```

3. В Telegram открой бота и добавь фильтр: `➕ Новый запрос`.

## Тестирование

Smoke check:

```bash
docker compose exec -T parser python tools/avito_smoke.py --config config.toml
```

Тесты:

```bash
docker compose exec -T parser python -m unittest discover -s tests -v
```

Остановка:

```bash
docker compose down
```

## Сброс данных (старт “с нуля”) 🧹

Если нужно поднять систему заново для новых пользователей и удалить старые фильтры/историю:

```bash
docker compose down
```

Удалить файлы/папки:
- `user_filters.db` (ваши фильтры)
- `database.db` (история/статистика)
- `cookies/`, `result/`, `logs/`, `free_proxies/` (runtime-артефакты)

После этого снова запусти `docker compose up -d --build`.

## Важно ⚠️

Авито может ограничивать доступ (429/блок-страница). Для стабильной работы используйте качественные прокси и разумные интервалы проверок!
