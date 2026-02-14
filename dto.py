from dataclasses import dataclass, field
from typing import List, Optional, Literal


@dataclass
class Proxy:
    proxy_string: str
    change_ip_link: str


@dataclass
class ProxySplit:
    ip_port: str
    login: str
    password: str
    change_ip_link: str


DeliveryMode = Literal["any", "delivery_only", "pickup_only"]
RegionPreset = Literal["all", "moscow", "moscow_mo", "mo"]


@dataclass
class SearchQuery:
    text: str
    region: RegionPreset = "all"
    min_price: Optional[int] = None
    max_price: Optional[int] = None
    delivery: DeliveryMode = "any"
    sort_new: Optional[bool] = None
    track_price_changes: bool = True
    # Per-query age limit (seconds). None means "use AvitoConfig.max_age".
    # 0 disables the filter for this query.
    max_age_seconds: Optional[int] = None


@dataclass
class AvitoConfig:
    urls: List[str]
    queries: List[str] = field(default_factory=list)
    region_slug: Optional[str] = None
    sort_new: bool = False
    delivery_only: bool = False
    proxy_string: Optional[str] = None
    proxy_change_url: Optional[str] = None
    # Default scheme for proxy strings without explicit scheme:// prefix.
    # Can also be set via env AVITO_PROXY_DEFAULT_SCHEME.
    proxy_default_scheme: str = "http"
    # Free proxies (experimental). Used when proxy_string is empty, unless mixed explicitly.
    use_free_proxies: bool = False
    free_proxies_mix_with_user_proxies: bool = False
    free_proxies_max_pool: int = 25
    free_proxies_min_pool: int = 10
    free_proxies_refresh_minutes: int = 60
    free_proxies_check_avito: bool = True
    free_proxies_max_candidates: int = 400
    free_proxies_validate_concurrency: int = 30
    free_proxies_cache_path: Optional[str] = None
    keys_word_white_list: List[str] = field(default_factory=list)
    keys_word_black_list: List[str] = field(default_factory=list)
    seller_black_list: List[str] = field(default_factory=list)
    count: int = 1
    tg_token: Optional[str] = None
    tg_chat_id: List[str] | None = None
    max_price: int = 999_999_999
    min_price: int = 0
    geo: Optional[str] = None
    max_age: int = 24 * 60 * 60
    debug_mode: int = 0
    pause_general: int = 60
    pause_between_links: int = 5
    max_count_of_retry: int = 5
    ignore_reserv: bool = True
    ignore_promotion: bool = False
    one_time_start: bool = False
    one_file_for_link: bool = False
    parse_views: bool = False
    save_xlsx: bool = True
    searches: List[SearchQuery] = field(default_factory=list)
    chat_owner: Optional[str] = None
    filter_id: Optional[int] = None
    filter_title: Optional[str] = None
    filter_interval_seconds: Optional[int] = None
    export_user_id: Optional[str] = None
    skip_first_notifications: bool = False
    initial_summary_sent: bool = False
