from __future__ import annotations

import json
import random
import threading
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Callable, Iterable
from concurrent.futures import ThreadPoolExecutor, wait, FIRST_COMPLETED

import httpx
from bs4 import BeautifulSoup
from curl_cffi import requests as curl_requests
from loguru import logger

from proxy_utils import ParsedProxy, parse_proxy, proxy_label


DEFAULT_IP_CHECK_URL = "https://api.ipify.org?format=json"
DEFAULT_AVITO_CHECK_URL = "https://www.avito.ru/"


_AVITO_BLOCK_MARKERS = (
    "captcha",
    "доступ ограничен",
    "подозрительная активность",
    "проблема с ip",
    "/blocked",
    "/security",
)


@dataclass(frozen=True)
class ProxyCandidate:
    proxy: ParsedProxy
    source: str

    def url(self) -> str:
        return self.proxy.url(for_playwright=False)


def _http_get_text(url: str, *, timeout: float = 15.0, headers: dict | None = None) -> str:
    headers = headers or {
        "user-agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
        )
    }
    r = httpx.get(url, headers=headers, timeout=timeout, follow_redirects=True)
    r.raise_for_status()
    return str(r.text or "")


def _parse_text_list(text: str, *, default_scheme: str, source: str) -> list[ProxyCandidate]:
    out: list[ProxyCandidate] = []
    for raw_line in (text or "").splitlines():
        line = raw_line.strip()
        if not line:
            continue
        if line.startswith("#"):
            continue
        if " " in line or "\t" in line:
            line = line.split()[0].strip()
        p = parse_proxy(line, default_scheme=default_scheme)
        if not p:
            continue
        out.append(ProxyCandidate(proxy=p, source=source))
    return out


def fetch_proxyscrape(*, timeout: float = 15.0) -> list[ProxyCandidate]:
    client = httpx.Client(timeout=timeout, follow_redirects=True, headers={"user-agent": "curl/8"})
    out: list[ProxyCandidate] = []
    try:
        for proto in ("http", "socks4", "socks5"):
            url = (
                "https://api.proxyscrape.com/v2/?request=getproxies"
                f"&protocol={proto}&timeout=10000&country=all&ssl=all&anonymity=all"
            )
            try:
                r = client.get(url)
                r.raise_for_status()
                out.extend(_parse_text_list(str(r.text or ""), default_scheme=proto, source="ProxyScrape"))
            except Exception:
                continue
    finally:
        try:
            client.close()
        except Exception:
            pass
    return out


def fetch_speedx(*, timeout: float = 15.0) -> list[ProxyCandidate]:
    base = "https://raw.githubusercontent.com/TheSpeedX/PROXY-List/master"
    out: list[ProxyCandidate] = []
    for path, proto in (("http.txt", "http"), ("socks4.txt", "socks4"), ("socks5.txt", "socks5")):
        try:
            text = _http_get_text(f"{base}/{path}", timeout=timeout)
            out.extend(_parse_text_list(text, default_scheme=proto, source="SpeedX"))
        except Exception:
            continue
    return out


def fetch_openproxylist(*, timeout: float = 15.0) -> list[ProxyCandidate]:
    base = "https://raw.githubusercontent.com/roosterkid/openproxylist/main"
    out: list[ProxyCandidate] = []
    for path, proto in (("HTTPS.txt", "http"), ("SOCKS4.txt", "socks4"), ("SOCKS5.txt", "socks5")):
        try:
            text = _http_get_text(f"{base}/{path}", timeout=timeout)
            out.extend(_parse_text_list(text, default_scheme=proto, source="OpenProxyList"))
        except Exception:
            continue
    return out


def fetch_kangproxy(*, timeout: float = 15.0) -> list[ProxyCandidate]:
    base = "https://raw.githubusercontent.com/officialputuid/KangProxy/KangProxy"
    out: list[ProxyCandidate] = []
    for path, proto in (("http/http.txt", "http"), ("socks4/socks4.txt", "socks4"), ("socks5/socks5.txt", "socks5")):
        try:
            text = _http_get_text(f"{base}/{path}", timeout=timeout)
            out.extend(_parse_text_list(text, default_scheme=proto, source="KangProxy"))
        except Exception:
            continue
    return out


def fetch_proxifly(*, timeout: float = 15.0) -> list[ProxyCandidate]:
    url = "https://cdn.jsdelivr.net/gh/proxifly/free-proxy-list@main/proxies/all/data.txt"
    try:
        text = _http_get_text(url, timeout=timeout)
    except Exception:
        return []
    # Proxifly "all" can be mixed; default to http for bare ip:port.
    return _parse_text_list(text, default_scheme="http", source="Proxifly")


def _is_ipv4(value: str) -> bool:
    parts = (value or "").split(".")
    if len(parts) != 4:
        return False
    try:
        return all(0 <= int(p) <= 255 for p in parts)
    except Exception:
        return False


def fetch_freeproxy_world(*, timeout: float = 15.0) -> list[ProxyCandidate]:
    out: list[ProxyCandidate] = []
    for proto in ("http", "socks4", "socks5"):
        url = f"https://www.freeproxy.world/?type={proto}"
        try:
            html = _http_get_text(url, timeout=timeout)
        except Exception:
            continue
        try:
            soup = BeautifulSoup(html, "html.parser")
        except Exception:
            continue

        rows = soup.select("table tbody tr")
        for row in rows:
            tds = [td.get_text(" ", strip=True) for td in row.find_all("td")]
            if not tds:
                continue
            ip = None
            port = None
            for i, cell in enumerate(tds):
                c = (cell or "").strip()
                if ip is None and _is_ipv4(c):
                    ip = c
                    # Port is usually next cell
                    if i + 1 < len(tds) and str(tds[i + 1]).strip().isdigit():
                        port = str(tds[i + 1]).strip()
                    break
            if not ip or not port or not str(port).isdigit():
                continue
            out.append(ProxyCandidate(proxy=ParsedProxy(scheme=proto, host=ip, port=int(port)), source="FreeProxyWorld"))
    return out


DEFAULT_FETCHERS: list[Callable[..., list[ProxyCandidate]]] = [
    fetch_proxyscrape,
    fetch_speedx,
    fetch_openproxylist,
    fetch_kangproxy,
    fetch_proxifly,
    fetch_freeproxy_world,
]


def collect_candidates(*, fetchers: Iterable[Callable[..., list[ProxyCandidate]]] | None = None) -> list[ProxyCandidate]:
    fetchers = list(fetchers or DEFAULT_FETCHERS)
    all_items: list[ProxyCandidate] = []
    for f in fetchers:
        try:
            items = f()
        except Exception as e:
            logger.debug(
                "free_proxy_robin fetcher {} failed: {}",
                getattr(f, "__name__", str(f)),
                str(e)[:180],
            )
            continue
        all_items.extend(items or [])
    # Deduplicate by scheme+host+port.
    uniq: dict[tuple[str, str, int], ProxyCandidate] = {}
    for item in all_items:
        key = (item.proxy.scheme, item.proxy.host, int(item.proxy.port))
        if key not in uniq:
            uniq[key] = item
    return list(uniq.values())


def _parse_ipify(text: str) -> str | None:
    t = (text or "").strip()
    if not t:
        return None
    if t.startswith("{") and t.endswith("}"):
        try:
            data = json.loads(t)
            ip = str(data.get("ip", "") or "").strip()
            return ip or None
        except Exception:
            return None
    return None


@dataclass(frozen=True)
class ProxyCheckResult:
    proxy_url: str
    ok: bool
    dt_s: float | None = None
    exit_ip: str | None = None
    error: str | None = None
    source: str | None = None


def check_proxy_connectivity(
    proxy_url: str,
    *,
    ip_check_url: str = DEFAULT_IP_CHECK_URL,
    timeout: float = 10.0,
) -> ProxyCheckResult:
    t0 = time.time()
    try:
        r = curl_requests.get(
            ip_check_url,
            proxies={"http": proxy_url, "https": proxy_url},
            impersonate="chrome",
            timeout=timeout,
            verify=False,
            allow_redirects=True,
        )
        dt = time.time() - t0
        ip = _parse_ipify(str(getattr(r, "text", "") or ""))
        if r.status_code >= 400 or not ip:
            return ProxyCheckResult(proxy_url=proxy_url, ok=False, dt_s=dt, exit_ip=ip, error=f"status={r.status_code}")
        return ProxyCheckResult(proxy_url=proxy_url, ok=True, dt_s=dt, exit_ip=ip)
    except Exception as e:
        dt = time.time() - t0
        return ProxyCheckResult(proxy_url=proxy_url, ok=False, dt_s=dt, error=str(e)[:200])


def _is_avito_blocked(text: str, *, url: str | None = None, location: str | None = None) -> bool:
    low = (text or "").lower()
    if any(m in low for m in _AVITO_BLOCK_MARKERS):
        return True
    u = (url or "").lower()
    loc = (location or "").lower()
    return ("/blocked" in u) or ("/blocked" in loc) or ("/security" in u) or ("/security" in loc)


def check_avito_access(
    proxy_url: str,
    *,
    avito_url: str = DEFAULT_AVITO_CHECK_URL,
    timeout: float = 12.0,
    headers: dict | None = None,
) -> ProxyCheckResult:
    t0 = time.time()
    try:
        r = curl_requests.get(
            avito_url,
            headers=headers,
            proxies={"http": proxy_url, "https": proxy_url},
            impersonate="chrome",
            timeout=timeout,
            verify=False,
            allow_redirects=False,
        )
        dt = time.time() - t0
        location = ""
        try:
            location = str(getattr(r, "headers", {}).get("location", "") or "")
        except Exception:
            location = ""
        text = str(getattr(r, "text", "") or "")[:8000]
        blocked = _is_avito_blocked(text, url=str(getattr(r, "url", "") or ""), location=location)
        if blocked or int(getattr(r, "status_code", 0) or 0) in {403, 429}:
            return ProxyCheckResult(
                proxy_url=proxy_url,
                ok=False,
                dt_s=dt,
                error=f"blocked status={getattr(r, 'status_code', '')}",
            )
        if int(getattr(r, "status_code", 0) or 0) >= 400:
            return ProxyCheckResult(proxy_url=proxy_url, ok=False, dt_s=dt, error=f"status={r.status_code}")
        return ProxyCheckResult(proxy_url=proxy_url, ok=True, dt_s=dt)
    except Exception as e:
        dt = time.time() - t0
        return ProxyCheckResult(proxy_url=proxy_url, ok=False, dt_s=dt, error=str(e)[:200])


class FreeProxyCache:
    def __init__(self, path: Path):
        self.path = Path(path)

    def load(self, *, max_age_seconds: float) -> list[str]:
        try:
            raw = json.loads(self.path.read_text(encoding="utf-8"))
        except Exception:
            return []
        if not isinstance(raw, dict):
            return []
        ts = raw.get("ts")
        proxies = raw.get("proxies")
        if not isinstance(ts, (int, float)) or not isinstance(proxies, list):
            return []
        age = time.time() - float(ts)
        if age < 0 or age > float(max_age_seconds):
            return []
        cleaned: list[str] = []
        for p in proxies:
            s = str(p or "").strip()
            if s:
                cleaned.append(s)
        return cleaned

    def save(self, proxies: list[str], *, meta: dict | None = None) -> None:
        payload = {"ts": int(time.time()), "proxies": list(proxies or [])}
        if meta:
            payload["meta"] = meta
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.path.write_text(json.dumps(payload, ensure_ascii=True, indent=2), encoding="utf-8")


_REFRESH_LOCK = threading.Lock()


def _config_int(cfg, name: str, default: int) -> int:
    try:
        return int(getattr(cfg, name, default))
    except Exception:
        return int(default)


def _config_bool(cfg, name: str, default: bool) -> bool:
    try:
        v = getattr(cfg, name, default)
    except Exception:
        return bool(default)
    if isinstance(v, bool):
        return v
    if isinstance(v, str):
        return v.strip().lower() in {"1", "true", "yes", "on"}
    return bool(v)


def _cache_path_for(cfg) -> Path:
    raw = str(getattr(cfg, "free_proxies_cache_path", "") or "").strip()
    if raw:
        return Path(raw)
    return Path("free_proxies") / "cache.json"


def get_free_proxy_pool(
    cfg,
    *,
    force_refresh: bool = False,
    fetchers: Iterable[Callable[..., list[ProxyCandidate]]] | None = None,
    headers_for_avito: dict | None = None,
) -> list[str]:
    max_pool = max(1, _config_int(cfg, "free_proxies_max_pool", 25))
    min_pool = max(0, _config_int(cfg, "free_proxies_min_pool", 10))
    refresh_minutes = max(1, _config_int(cfg, "free_proxies_refresh_minutes", 60))
    check_avito = _config_bool(cfg, "free_proxies_check_avito", True)
    max_candidates = max(50, _config_int(cfg, "free_proxies_max_candidates", 400))
    validate_workers = max(1, _config_int(cfg, "free_proxies_validate_concurrency", 30))
    validate_workers = max(1, min(80, validate_workers))

    cache = FreeProxyCache(_cache_path_for(cfg))
    if not force_refresh:
        cached = cache.load(max_age_seconds=refresh_minutes * 60)
        if cached:
            return cached[:max_pool]

    with _REFRESH_LOCK:
        if not force_refresh:
            cached = cache.load(max_age_seconds=refresh_minutes * 60)
            if cached:
                return cached[:max_pool]

        candidates = collect_candidates(fetchers=fetchers)
        random.shuffle(candidates)
        if len(candidates) > max_candidates:
            candidates = candidates[:max_candidates]

        if not candidates:
            return []

        # connectivity + exit IP 
        good: list[tuple[ProxyCandidate, ProxyCheckResult]] = []
        seen_exit_ips: set[str] = set()
        phase1_target = max(60, max_pool * 4)

        it = iter(candidates)
        in_flight_limit = max(20, validate_workers * 2)
        in_flight: set = set()
        fut_to_item: dict = {}

        def _submit_next(ex: ThreadPoolExecutor) -> bool:
            try:
                item = next(it)
            except StopIteration:
                return False
            fut = ex.submit(check_proxy_connectivity, item.url())
            fut_to_item[fut] = item
            in_flight.add(fut)
            return True

        ex = ThreadPoolExecutor(max_workers=validate_workers, thread_name_prefix="free-proxy-check")
        try:
            while len(in_flight) < in_flight_limit and _submit_next(ex):
                pass

            while in_flight:
                done, in_flight = wait(in_flight, return_when=FIRST_COMPLETED, timeout=0.5)
                for fut in done:
                    item = fut_to_item.pop(fut, None)
                    if item is None:
                        continue
                    try:
                        res = fut.result()
                    except Exception:
                        continue
                    if not isinstance(res, ProxyCheckResult) or (not res.ok) or (not res.exit_ip):
                        continue
                    if res.exit_ip in seen_exit_ips:
                        continue
                    seen_exit_ips.add(res.exit_ip)
                    good.append((item, res))
                    if len(good) >= phase1_target:
                        in_flight.clear()
                        break

                while len(in_flight) < in_flight_limit and len(good) < phase1_target and _submit_next(ex):
                    pass
        finally:
            ex.shutdown(wait=False, cancel_futures=True)

        # Sort by protocol preference + latency
        def proto_rank(scheme: str) -> int:
            if scheme == "socks5" or scheme == "socks5h":
                return 0
            if scheme == "socks4":
                return 1
            return 2

        good.sort(key=lambda pair: (proto_rank(pair[0].proxy.scheme), float(pair[1].dt_s or 999.0)))

        # check for top candidates
        selected: list[str] = []
        if check_avito:
            avito_candidates = [item.url() for item, _res in good[: max(1, max_pool * 4)]]
            it2 = iter(avito_candidates)
            in_flight2: set = set()
            in_flight_limit2 = max(10, min(in_flight_limit, max_pool * 2))
            fut_to_url2: dict = {}

            def _submit_next2(ex: ThreadPoolExecutor) -> bool:
                try:
                    u = next(it2)
                except StopIteration:
                    return False
                fut = ex.submit(check_avito_access, u, headers=headers_for_avito)
                fut_to_url2[fut] = u
                in_flight2.add(fut)
                return True

            ex2 = ThreadPoolExecutor(max_workers=min(validate_workers, 30), thread_name_prefix="free-proxy-avito")
            try:
                while len(in_flight2) < in_flight_limit2 and _submit_next2(ex2):
                    pass
                while in_flight2 and len(selected) < max_pool:
                    done2, in_flight2 = wait(in_flight2, return_when=FIRST_COMPLETED, timeout=0.5)
                    for fut in done2:
                        _ = fut_to_url2.pop(fut, None)
                        try:
                            res = fut.result()
                        except Exception:
                            continue
                        if isinstance(res, ProxyCheckResult) and res.ok:
                            selected.append(res.proxy_url)
                            if len(selected) >= max_pool:
                                in_flight2.clear()
                                break
                    while len(in_flight2) < in_flight_limit2 and len(selected) < max_pool and _submit_next2(ex2):
                        pass
            finally:
                ex2.shutdown(wait=False, cancel_futures=True)
        else:
            selected = [item.url() for item, _res in good[:max_pool]]

        if len(selected) < min_pool:
            selected = [item.url() for item, _res in good[:max_pool]]

        cache.save(
            selected,
            meta={
                "candidates_total": len(candidates),
                "phase1_ok": len(good),
                "selected": len(selected),
                "check_avito": bool(check_avito),
            },
        )

        if selected:
            logger.info(
                "free_proxy_robin: refreshed pool size={} (sample={})",
                len(selected),
                proxy_label(selected[0]),
            )
        else:
            logger.warning("free_proxy_robin: refreshed pool is empty")
        return selected
