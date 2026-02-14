import tempfile
import unittest
from unittest import mock

from free_proxy_robin import ProxyCandidate, ProxyCheckResult, get_free_proxy_pool
from proxy_utils import ParsedProxy, parse_proxy


class _Cfg:
    pass


class TestFreeProxyRobin(unittest.TestCase):
    def test_cache_is_used(self):
        with tempfile.TemporaryDirectory() as td:
            cfg = _Cfg()
            cfg.use_free_proxies = True
            cfg.free_proxies_cache_path = f"{td}/cache.json"
            cfg.free_proxies_max_pool = 3
            cfg.free_proxies_min_pool = 1
            cfg.free_proxies_refresh_minutes = 999
            cfg.free_proxies_check_avito = True
            cfg.free_proxies_max_candidates = 10
            cfg.free_proxies_validate_concurrency = 4

            candidates = [
                ProxyCandidate(ParsedProxy("socks5", "1.1.1.1", 1080), "t"),
                ProxyCandidate(ParsedProxy("http", "2.2.2.2", 8080), "t"),
                ProxyCandidate(ParsedProxy("http", "3.3.3.3", 8080), "t"),
            ]

            def fetcher():
                return list(candidates)

            def fake_connectivity(proxy_url: str, **_kw):
                p = parse_proxy(proxy_url)
                # Unique exit IP per host for predictable selection.
                return ProxyCheckResult(proxy_url=proxy_url, ok=True, dt_s=0.1, exit_ip=f"99.{p.host}")

            def fake_avito(proxy_url: str, **_kw):
                return ProxyCheckResult(proxy_url=proxy_url, ok=True, dt_s=0.2)

            with mock.patch("free_proxy_robin.check_proxy_connectivity", side_effect=fake_connectivity), mock.patch(
                "free_proxy_robin.check_avito_access", side_effect=fake_avito
            ):
                first = get_free_proxy_pool(cfg, force_refresh=True, fetchers=[fetcher])
                self.assertEqual(len(first), 3)

            # Second call must use cache and not touch network validators.
            with mock.patch("free_proxy_robin.check_proxy_connectivity", side_effect=AssertionError("should_not_call")), mock.patch(
                "free_proxy_robin.check_avito_access", side_effect=AssertionError("should_not_call")
            ):
                second = get_free_proxy_pool(cfg, force_refresh=False, fetchers=[fetcher])
                self.assertEqual(first, second)

    def test_exit_ip_dedup(self):
        with tempfile.TemporaryDirectory() as td:
            cfg = _Cfg()
            cfg.use_free_proxies = True
            cfg.free_proxies_cache_path = f"{td}/cache.json"
            cfg.free_proxies_max_pool = 10
            cfg.free_proxies_min_pool = 0
            cfg.free_proxies_refresh_minutes = 999
            cfg.free_proxies_check_avito = False
            cfg.free_proxies_max_candidates = 10
            cfg.free_proxies_validate_concurrency = 4

            candidates = [
                ProxyCandidate(ParsedProxy("http", "1.1.1.1", 8080), "t"),
                ProxyCandidate(ParsedProxy("http", "1.1.1.2", 8080), "t"),
            ]

            def fetcher():
                return list(candidates)

            # Both proxies map to the same exit IP -> only one should survive phase1.
            def fake_connectivity(proxy_url: str, **_kw):
                _ = parse_proxy(proxy_url)
                return ProxyCheckResult(proxy_url=proxy_url, ok=True, dt_s=0.1, exit_ip="77.77.77.77")

            with mock.patch("free_proxy_robin.check_proxy_connectivity", side_effect=fake_connectivity):
                pool = get_free_proxy_pool(cfg, force_refresh=True, fetchers=[fetcher])
                self.assertEqual(len(pool), 1)

