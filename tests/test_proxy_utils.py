import unittest

from proxy_utils import parse_proxy, proxy_label, proxy_to_playwright_config, proxy_to_url


class TestProxyUtils(unittest.TestCase):
    def test_parse_hostport(self):
        p = parse_proxy("1.2.3.4:8080")
        self.assertIsNotNone(p)
        self.assertEqual(p.scheme, "http")
        self.assertEqual(p.host, "1.2.3.4")
        self.assertEqual(p.port, 8080)
        self.assertEqual(p.username, "")

    def test_parse_userpass_at_hostport(self):
        p = parse_proxy("user:pass@proxy.example:3128")
        self.assertIsNotNone(p)
        self.assertEqual(p.scheme, "http")
        self.assertEqual(p.host, "proxy.example")
        self.assertEqual(p.port, 3128)
        self.assertEqual(p.username, "user")
        self.assertEqual(p.password, "pass")

    def test_parse_hostport_userpass(self):
        p = parse_proxy("1.2.3.4:8080:user:pass")
        self.assertIsNotNone(p)
        self.assertEqual(p.host, "1.2.3.4")
        self.assertEqual(p.port, 8080)
        self.assertEqual(p.username, "user")
        self.assertEqual(p.password, "pass")

    def test_parse_userpass_hostport(self):
        p = parse_proxy("user:pass:1.2.3.4:8080")
        self.assertIsNotNone(p)
        self.assertEqual(p.host, "1.2.3.4")
        self.assertEqual(p.port, 8080)
        self.assertEqual(p.username, "user")
        self.assertEqual(p.password, "pass")

    def test_parse_scheme(self):
        p = parse_proxy("socks5://1.2.3.4:1080")
        self.assertIsNotNone(p)
        self.assertEqual(p.scheme, "socks5")
        self.assertEqual(p.host, "1.2.3.4")
        self.assertEqual(p.port, 1080)

    def test_proxy_to_url_preserves_socks(self):
        self.assertEqual(proxy_to_url("socks5://1.2.3.4:1080"), "socks5://1.2.3.4:1080")
        self.assertEqual(proxy_to_url("socks5h://1.2.3.4:1080"), "socks5h://1.2.3.4:1080")

    def test_proxy_label_hides_creds(self):
        self.assertEqual(proxy_label("http://user:pass@1.2.3.4:8080"), "1.2.3.4:8080")
        self.assertEqual(proxy_label("user:pass@1.2.3.4:8080"), "1.2.3.4:8080")

    def test_playwright_config(self):
        cfg = proxy_to_playwright_config("socks5://user:pass@1.2.3.4:1080")
        self.assertIsInstance(cfg, dict)
        self.assertEqual(cfg["server"], "socks5://1.2.3.4:1080")
        self.assertEqual(cfg["username"], "user")
        self.assertEqual(cfg["password"], "pass")

