import unittest
from unittest import mock

from dto import AvitoConfig
from parser_cls import AvitoParse


class TestParserFreeProxyIntegration(unittest.TestCase):
    def test_free_proxies_are_loaded_when_enabled_and_no_user_proxy(self):
        cfg = AvitoConfig(
            urls=[],
            queries=[],
            proxy_string="",
            proxy_change_url="",
            use_free_proxies=True,
            free_proxies_mix_with_user_proxies=False,
            free_proxies_max_pool=5,
        )

        fake_pool = ["socks5://1.1.1.1:1080", "http://2.2.2.2:8080"]
        with mock.patch("parser_cls.get_free_proxy_pool", return_value=fake_pool) as m:
            p = AvitoParse(cfg)
            self.assertTrue(m.called)
            self.assertEqual(p.proxy_pool[:2], fake_pool)

    def test_free_proxies_not_loaded_when_user_proxy_present_and_mix_false(self):
        cfg = AvitoConfig(
            urls=[],
            queries=[],
            proxy_string="1.2.3.4:8080",
            proxy_change_url="",
            use_free_proxies=True,
            free_proxies_mix_with_user_proxies=False,
        )

        with mock.patch("parser_cls.get_free_proxy_pool", return_value=["http://9.9.9.9:9999"]) as m:
            p = AvitoParse(cfg)
            self.assertFalse(m.called)
            self.assertEqual(p.proxy_pool, ["1.2.3.4:8080"])

