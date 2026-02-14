import sys
from pathlib import Path

# Allow running as `python tools/run_parser_once.py` (project root imports).
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from load_config import load_avito_config
from parser_cls import AvitoParse


def main() -> int:
    cfg = load_avito_config("config.toml")

    # One cycle only, no Telegram, minimal work.
    cfg.one_time_start = True
    cfg.count = 1
    cfg.pause_between_links = 1
    cfg.pause_general = 1
    cfg.save_xlsx = False
    cfg.tg_token = None
    # Use only explicit URLs from config.toml for a simple smoke run.
    cfg.searches = []
    cfg.queries = []
    cfg.max_count_of_retry = 1

    parser = AvitoParse(cfg)
    parser.parse()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
