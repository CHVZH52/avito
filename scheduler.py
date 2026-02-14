import threading
import time
from copy import deepcopy
from dataclasses import dataclass, field
from concurrent.futures import ThreadPoolExecutor
from typing import Dict, Optional

from loguru import logger

from dto import AvitoConfig, SearchQuery
from parser_cls import AvitoParse
from user_filters import (
    DEFAULT_INTERVAL_SECONDS,
    MIN_INTERVAL_SECONDS,
    UserFiltersStorage,
)


def _clamp_interval(value: Optional[int]) -> int:
    try:
        seconds = int(value)
    except (TypeError, ValueError):
        seconds = DEFAULT_INTERVAL_SECONDS
    return max(MIN_INTERVAL_SECONDS, seconds)


def _build_search(row) -> SearchQuery:
    try:
        max_age_seconds = row["max_age_seconds"]
    except Exception:
        max_age_seconds = None
    return SearchQuery(
        text=row["text"],
        region=row["region"] or "all",
        min_price=row["min_price"],
        max_price=row["max_price"],
        delivery=row["delivery"] or "any",
        sort_new=None if row["sort_new"] is None else bool(row["sort_new"]),
        track_price_changes=bool(row["track_price_changes"]),
        max_age_seconds=max_age_seconds,
    )


def _build_config(base_config: AvitoConfig, row, interval: int) -> AvitoConfig:
    cfg = deepcopy(base_config)
    search = _build_search(row)
    cfg.searches = [search]
    cfg.queries = [search.text]
    cfg.urls = []
    cfg.tg_chat_id = [str(row["chat_id"])]
    cfg.chat_owner = f"{row['chat_id']}:{row['id']}"
    cfg.filter_id = row["id"]
    cfg.filter_title = search.text
    cfg.filter_interval_seconds = interval
    cfg.export_user_id = str(row["chat_id"])
    cfg.initial_summary_sent = bool(row["initial_summary_sent"])
    # Scheduler mode: keep single iteration fast and aligned with short intervals.
    # For notifications, first page is usually enough; longer runs cause drift.
    cfg.count = 1
    cfg.pause_between_links = min(int(getattr(cfg, "pause_between_links", 0) or 0), 1)
    cfg.max_count_of_retry = min(int(getattr(cfg, "max_count_of_retry", 3) or 3), 2)
    return cfg


@dataclass
class FilterJob:
    config: AvitoConfig
    interval_seconds: int
    filter_id: int
    chat_id: int
    username: Optional[str]
    next_run_ts: float = field(default_factory=lambda: 0.0)

    def schedule_next(self, *, now: Optional[float] = None) -> None:
        now = now or time.time()
        self.next_run_ts = now + max(1, self.interval_seconds)


class FiltersScheduler:
    def __init__(self, base_config: AvitoConfig, storage: UserFiltersStorage):
        self.base_config = base_config
        self.storage = storage
        self.jobs: Dict[int, FilterJob] = {}
        self.refresh_interval = 60
        self._last_refresh = 0.0
        self.stop_event = threading.Event()
        self._thread: Optional[threading.Thread] = None
        self._executor = ThreadPoolExecutor(max_workers=5, thread_name_prefix="filter-runner")
        self._running: set[int] = set()
        self._lock = threading.Lock()

    def start(self) -> threading.Thread:
        if self._thread and self._thread.is_alive():
            return self._thread
        thread = threading.Thread(target=self.run, name="filters-scheduler", daemon=True)
        thread.start()
        self._thread = thread
        return thread

    def stop(self) -> None:
        self.stop_event.set()
        if self._thread and self._thread.is_alive():
            self._thread.join(timeout=5)
        self._executor.shutdown(wait=False, cancel_futures=True)

    def run(self) -> None:
        logger.info("Планировщик фильтров запущен")
        while not self.stop_event.is_set():
            has_jobs = self._refresh_jobs()
            if not has_jobs:
                # нет фильтров — повторим позже
                self.stop_event.wait(10)
                continue

            job = self._next_job()
            if not job:
                self.stop_event.wait(5)
                continue

            wait_for = job.next_run_ts - time.time()
            if wait_for > 0:
                self.stop_event.wait(min(wait_for, 10))
                continue

            descriptor = f"#{job.filter_id} ({job.config.filter_title})"
            with self._lock:
                already_running = job.filter_id in self._running
                if not already_running:
                    self._running.add(job.filter_id)
            if already_running:
                # Don't queue infinite overlapping runs. Re-schedule quickly.
                job.schedule_next()
                continue

            logger.info(f"Старт фильтра {descriptor} каждые {job.interval_seconds} сек")
            self._executor.submit(self._run_job, job)
            job.schedule_next()
            # обновим список фильтров после итерации
            self._last_refresh = 0

        logger.info("Планировщик фильтров остановлен")

    def _run_job(self, job: FilterJob) -> None:
        descriptor = f"#{job.filter_id} ({job.config.filter_title})"
        try:
            parser = AvitoParse(job.config, stop_event=self.stop_event)
            parser.parse()
        except Exception as err:
            logger.exception("Ошибка при обработке фильтра {}: {}", descriptor, err)
        finally:
            with self._lock:
                self._running.discard(job.filter_id)

    def _refresh_jobs(self) -> bool:
        now = time.time()
        if self.jobs and (now - self._last_refresh) < self.refresh_interval:
            return True

        rows = self.storage.get_filters_for_scheduler()
        self._last_refresh = now
        if not rows:
            self.jobs.clear()
            return False

        existing_ids = set()
        for row in rows:
            filter_id = row["id"]
            interval = _clamp_interval(row["interval_seconds"])
            cfg = _build_config(self.base_config, row, interval)
            if filter_id in self.jobs:
                job = self.jobs[filter_id]
                job.interval_seconds = interval
                job.config = cfg
            else:
                job = FilterJob(
                    config=cfg,
                    interval_seconds=interval,
                    filter_id=filter_id,
                    chat_id=row["chat_id"],
                    username=row["username"],
                    next_run_ts=time.time(),
                )
                self.jobs[filter_id] = job
                logger.info(f"Добавлен фильтр #{filter_id} ({cfg.filter_title}) с интервалом {interval} сек")
            existing_ids.add(filter_id)

        for job_id in list(self.jobs.keys()):
            if job_id not in existing_ids:
                removed = self.jobs.pop(job_id)
                logger.info(f"Удалён фильтр #{job_id} ({removed.config.filter_title}) из расписания")

        return bool(self.jobs)

    def _next_job(self) -> Optional[FilterJob]:
        if not self.jobs:
            return None
        return min(self.jobs.values(), key=lambda job: job.next_run_ts)
