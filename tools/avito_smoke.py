import os
import sys
import time
import json
import argparse
from pathlib import Path

# Allow running as `python tools/avito_smoke.py` (project root imports).
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from bs4 import BeautifulSoup

from load_config import load_avito_config
from parser_cls import AvitoParse, BLOCK_PAGE_MARKERS
from hide_private_data import mask_sensitive_data


DEFAULT_AVITO_HOME_URL = "https://www.avito.ru/"
DEFAULT_IP_CHECK_URL = "https://api.ipify.org?format=json"


def _is_blocked(html_text: str) -> bool:
    low = (html_text or "").lower()
    return any(m in low for m in BLOCK_PAGE_MARKERS)


def _summarize_response(r) -> dict:
    try:
        status = int(getattr(r, "status_code", 0) or 0)
    except Exception:
        status = 0
    try:
        location = str(getattr(r, "headers", {}).get("location", "") or "")
    except Exception:
        location = ""
    try:
        url = str(getattr(r, "url", "") or "")
    except Exception:
        url = ""
    try:
        text = str(getattr(r, "text", "") or "")
    except Exception:
        text = ""
    soup = BeautifulSoup(text, "html.parser")
    title = (soup.title.string.strip() if soup.title and soup.title.string else "")
    blocked = _is_blocked(text) or ("/blocked" in (url or "").lower()) or ("/blocked" in (location or "").lower())
    return {
        "status": status,
        "final_url": url,
        "location": location,
        "title": title[:160],
        "bytes": len(text.encode("utf-8", errors="ignore")),
        "blocked": blocked,
        "has_mime_invalid": ('type="mime/invalid"' in text) or ("mime/invalid" in text),
    }


def _pick_target_url(parser: AvitoParse) -> str:
    # Prefer a concrete URL from config.toml; fallback to a search-built one.
    url = (parser.config.urls or [None])[0]
    if not url:
        targets = parser._resolve_input_links()
        url = targets[0][1] if targets else DEFAULT_AVITO_HOME_URL
    return url or DEFAULT_AVITO_HOME_URL


def _safe_err(e: Exception) -> str:
    return mask_sensitive_data(str(e))[:240]


def _extract_ip(text: str) -> str | None:
    t = (text or "").strip()
    if not t:
        return None
    # ipify JSON: {"ip":"x.x.x.x"}
    if t.startswith("{") and t.endswith("}"):
        try:
            data = json.loads(t)
            ip = str(data.get("ip", "") or "").strip()
            return ip or None
        except Exception:
            return None
    # ipify plain: x.x.x.x
    if 7 <= len(t) <= 64 and all(ch.isdigit() or ch in ".:abcdefABCDEF" for ch in t):
        return t
    return None


def test_proxies(
    parser: AvitoParse,
    *,
    check_url: str,
    ip_check_url: str | None = DEFAULT_IP_CHECK_URL,
    timeout: float = 20.0,
    limit: int | None = None,
    attempts: int = 1,
    sleep_between: float = 0.2,
    allow_redirects: bool = False,
    cookies: dict | None = None,
) -> list[dict]:
    """
    Tests proxy pool from config:
    - connectivity and outward IP via ip_check_url (optional)
    - Avito access via check_url and blocked markers

    Never prints proxy credentials (uses parser._proxy_label).
    """
    proxies = list(parser.proxy_pool or [])
    if limit is not None:
        try:
            limit = int(limit)
        except Exception:
            limit = None
    if limit is not None and limit > 0:
        proxies = proxies[:limit]

    if not proxies:
        print("proxy_test: no proxies configured (config.proxy_string is empty)")
        return []

    sess = type(parser.session)()
    results: list[dict] = []

    if cookies is None:
        try:
            cookies = parser.session.cookies.get_dict()
        except Exception:
            cookies = {}

    for idx, proxy_string in enumerate(proxies, 1):
        label = parser._proxy_label(proxy_string)
        proxy_data = parser._build_proxy_data_for(proxy_string)

        best: dict | None = None
        last_err: str | None = None
        for attempt in range(1, max(1, int(attempts or 1)) + 1):
            ip = None
            ip_err = None
            ip_dt = None
            if ip_check_url:
                t0 = time.time()
                try:
                    r = sess.get(
                        url=ip_check_url,
                        proxies=proxy_data,
                        timeout=timeout,
                        verify=False,
                        allow_redirects=True,
                        impersonate="chrome",
                    )
                    ip_dt = time.time() - t0
                    ip = _extract_ip(str(getattr(r, "text", "") or ""))
                    if not ip:
                        ip_err = f"ip_parse_failed status={getattr(r, 'status_code', '')}"
                except Exception as e:
                    ip_dt = time.time() - t0
                    ip_err = _safe_err(e)

            t0 = time.time()
            try:
                r = sess.get(
                    url=check_url,
                    headers=parser.headers,
                    proxies=proxy_data,
                    cookies=cookies,
                    impersonate="chrome",
                    timeout=timeout,
                    verify=False,
                    allow_redirects=allow_redirects,
                )
                dt = time.time() - t0
                s = _summarize_response(r)
                best = {
                    "idx": idx,
                    "attempt": attempt,
                    "proxy": label,
                    "ip": ip,
                    "ip_dt_s": None if ip_dt is None else round(float(ip_dt), 3),
                    "ip_error": ip_err,
                    "dt_s": round(float(dt), 3),
                    **s,
                }
                last_err = None
                break
            except Exception as e:
                dt = time.time() - t0
                last_err = _safe_err(e)
                best = {
                    "idx": idx,
                    "attempt": attempt,
                    "proxy": label,
                    "ip": ip,
                    "ip_dt_s": None if ip_dt is None else round(float(ip_dt), 3),
                    "ip_error": ip_err,
                    "dt_s": round(float(dt), 3),
                    "error": last_err,
                }
                if sleep_between:
                    time.sleep(float(sleep_between))

        if best is None:
            best = {"idx": idx, "proxy": label, "error": last_err or "unknown_error"}
        results.append(best)

        status = best.get("status")
        blocked = best.get("blocked")
        err = best.get("error")
        ip = best.get("ip")
        ip_part = f" ip={ip}" if ip else ""
        if err:
            print(f"[{idx}/{len(proxies)}] proxy={label} error={err}{ip_part}")
        else:
            print(
                f"[{idx}/{len(proxies)}] proxy={label} status={status} blocked={'yes' if blocked else 'no'}"
                f" dt={best.get('dt_s')}s{ip_part} title={str(best.get('title', ''))[:80]}"
            )

    return results


def _print_proxy_summary(results: list[dict]) -> None:
    if not results:
        return
    total = len(results)
    ok = sum(1 for r in results if not r.get("error"))
    blocked = sum(1 for r in results if (not r.get("error")) and r.get("blocked"))
    ips = sorted({str(r.get("ip")) for r in results if r.get("ip")})
    print(f"proxy_test_summary total={total} ok={ok} blocked={blocked} unique_ips={len(ips)}")


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description="Avito smoke checks (direct + proxy) and proxy pool testing.")
    p.add_argument("--config", default="config.toml", help="Path to config TOML (default: config.toml)")
    p.add_argument("--url", default=None, help="Target URL to check (default: first config url or home)")
    p.add_argument("--timeout", type=float, default=20.0, help="Request timeout (seconds)")

    p.add_argument("--test-proxies", action="store_true", help="Run proxy pool test instead of single-proxy smoke")
    p.add_argument("--check-url", default=DEFAULT_AVITO_HOME_URL, help="URL used for proxy checks (default: Avito home)")
    p.add_argument("--limit", type=int, default=None, help="Limit number of proxies to test")
    p.add_argument("--attempts", type=int, default=1, help="Attempts per proxy")
    p.add_argument("--sleep-between", type=float, default=0.2, help="Sleep between attempts (seconds)")
    p.add_argument("--ip-check-url", default=DEFAULT_IP_CHECK_URL, help="External IP endpoint (set empty to disable)")
    p.add_argument("--no-ip-check", action="store_true", help="Disable external IP check (overrides --ip-check-url)")
    p.add_argument("--repeat", type=int, default=1, help="Repeat proxy test N times (to see if IP/block changes)")
    p.add_argument("--repeat-delay", type=float, default=60.0, help="Delay between repeats (seconds)")
    p.add_argument("--json-out", default=None, help="Write results JSON to this path")

    args = p.parse_args(argv)

    cfg = load_avito_config(args.config)
    # Smoke test should not send anything to Telegram.
    cfg.tg_token = None

    parser = AvitoParse(cfg)

    url = args.url or _pick_target_url(parser)

    proxy_enabled = bool((cfg.proxy_string or "").strip()) or bool(getattr(cfg, "use_free_proxies", False))
    print(f"target_url={url}")
    print(f"proxy_pool={'yes' if proxy_enabled else 'no'}")
    print(f"cookies_loaded={'yes' if parser.cookies else 'no'}")

    if args.test_proxies:
        ip_check_url = None if args.no_ip_check else ((args.ip_check_url or "").strip() or None)
        all_runs: list[dict] = []
        previous_by_proxy: dict[str, dict] = {}
        runs = max(1, int(args.repeat or 1))
        for run_idx in range(1, runs + 1):
            ts = time.strftime("%Y-%m-%d %H:%M:%S")
            print(f"proxy_test_run={run_idx}/{runs} ts={ts}")
            results = test_proxies(
                parser,
                check_url=str(args.check_url or DEFAULT_AVITO_HOME_URL),
                ip_check_url=ip_check_url,
                timeout=args.timeout,
                limit=args.limit,
                attempts=args.attempts,
                sleep_between=args.sleep_between,
                allow_redirects=False,
            )
            _print_proxy_summary(results)

            # Print deltas (IP or blocked status changes) between runs.
            if run_idx > 1 and results:
                for r in results:
                    key = str(r.get("proxy") or "")
                    if not key:
                        continue
                    prev = previous_by_proxy.get(key)
                    if not prev:
                        continue
                    if r.get("ip") and prev.get("ip") and r.get("ip") != prev.get("ip"):
                        print(f"proxy_delta ip_changed proxy={key} {prev.get('ip')} -> {r.get('ip')}")
                    if (r.get("blocked") is not None) and (prev.get("blocked") is not None) and r.get("blocked") != prev.get("blocked"):
                        print(f"proxy_delta blocked_changed proxy={key} {prev.get('blocked')} -> {r.get('blocked')}")

                previous_by_proxy = {str(r.get("proxy") or ""): r for r in results if r.get("proxy")}
            else:
                previous_by_proxy = {str(r.get("proxy") or ""): r for r in results if r.get("proxy")}

            all_runs.append({"run": run_idx, "ts": ts, "results": results})
            if run_idx < runs:
                time.sleep(max(0.0, float(args.repeat_delay or 0.0)))

        if args.json_out:
            out_path = Path(args.json_out)
            out_path.parent.mkdir(parents=True, exist_ok=True)
            out_path.write_text(json.dumps(all_runs, ensure_ascii=True, indent=2), encoding="utf-8")
            print(f"json_out={str(out_path)}")
        return 0

    # 1) Direct request (no proxy) to show that Avito responds at all.
    try:
        r = parser.session.get(
            url=DEFAULT_AVITO_HOME_URL,
            headers=parser.headers,
            proxies=None,
            cookies=parser.session.cookies.get_dict(),
            impersonate="chrome",
            timeout=20,
            verify=False,
            allow_redirects=False,
        )
        s = _summarize_response(r)
        print(f"direct_home_status={s['status']} blocked={'yes' if s['blocked'] else 'no'} title={s['title']}")
        if s["location"]:
            print(f"direct_home_location={s['location']}")
    except Exception as e:
        print(f"direct_home_error={str(e)[:160]}")

    # 2) Direct request to the configured URL.
    try:
        r = parser.session.get(
            url=url,
            headers=parser.headers,
            proxies=None,
            cookies=parser.session.cookies.get_dict(),
            impersonate="chrome",
            timeout=20,
            verify=False,
            allow_redirects=False,
        )
        s = _summarize_response(r)
        print(f"direct_target_status={s['status']} blocked={'yes' if s['blocked'] else 'no'} title={s['title']}")
        if s["location"]:
            print(f"direct_target_location={s['location']}")
    except Exception as e:
        print(f"direct_target_error={str(e)[:160]}")

    # 3) If proxies are configured (user or free), try a few proxies to see if at least one works.
    if proxy_enabled and parser.proxy_pool:
        max_try = min(5, len(parser.proxy_pool))
        ok = False
        for idx in range(max_try):
            proxy_used = parser.proxy_pool[idx]
            try:
                r = parser.session.get(
                    url="https://www.avito.ru/",
                    headers=parser.headers,
                    proxies=parser._build_proxy_data_for(proxy_used),
                    cookies=parser.session.cookies.get_dict(),
                    impersonate="chrome",
                    timeout=20,
                    verify=False,
                    allow_redirects=False,
                )
                s = _summarize_response(r)
                print(
                    f"proxy_home_status={s['status']} blocked={'yes' if s['blocked'] else 'no'} "
                    f"proxy={parser._proxy_label(proxy_used)} title={s['title']}"
                )
                ok = True
                break
            except Exception as e:
                print(f"proxy_home_error via {parser._proxy_label(proxy_used)}: {str(e)[:160]}")
        if not ok:
            print(f"proxy_home_all_failed tried={max_try}/{len(parser.proxy_pool)}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
