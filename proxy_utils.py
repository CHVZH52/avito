from __future__ import annotations

from dataclasses import dataclass
import os
import re
from typing import Optional
from urllib.parse import urlparse

from hide_private_data import mask_sensitive_data


_KNOWN_SCHEMES = {"http", "https", "socks4", "socks5", "socks5h"}


@dataclass(frozen=True)
class ParsedProxy:
    scheme: str
    host: str
    port: int
    username: str = ""
    password: str = ""

    def hostport(self) -> str:
        return f"{self.host}:{self.port}"

    def url(self, *, for_playwright: bool = False) -> str:
        scheme = self.scheme
        # Playwright does not document socks5h; use socks5 for safety.
        if for_playwright and scheme == "socks5h":
            scheme = "socks5"
        auth = ""
        if self.username:
            # Don't encode, keep behavior consistent with existing proxy formats.
            auth = f"{self.username}:{self.password}@"
        return f"{scheme}://{auth}{self.host}:{self.port}"


def env_default_proxy_scheme() -> str:
    v = (os.getenv("AVITO_PROXY_DEFAULT_SCHEME") or "").strip().lower()
    if not v:
        return "http"
    if v == "socks":
        return "socks5"
    if v in _KNOWN_SCHEMES:
        return v
    return "http"


def _looks_like_hostport(value: str) -> bool:
    v = (value or "").strip()
    if not v or ":" not in v:
        return False
    host, port = v.rsplit(":", 1)
    if not host or not port.isdigit():
        return False
    return True


def _parse_hostport(value: str) -> tuple[str, int] | None:
    v = (value or "").strip()
    if not v:
        return None
    if v.startswith("[") and "]" in v:
        # [ipv6]:port
        try:
            host_part, port_part = v.rsplit("]:", 1)
            host = host_part.lstrip("[").strip()
            if host and port_part.isdigit():
                return host, int(port_part)
        except Exception:
            return None
    if ":" not in v:
        return None
    host, port = v.rsplit(":", 1)
    if not host or not port.isdigit():
        return None
    return host, int(port)


def parse_proxy(raw: str | None, *, default_scheme: str = "http") -> ParsedProxy | None:
    """
    Supports:
    - scheme://user:pass@host:port
    - scheme://host:port
    - user:pass@host:port
    - host:port@user:pass (legacy variant)
    - host:port
    - host:port:user:pass
    - user:pass:host:port
    """
    text = (raw or "").strip()
    if not text:
        return None

    # URL-like form with scheme.
    if "://" in text:
        try:
            u = urlparse(text)
        except Exception:
            u = None
        if u and u.scheme and u.hostname and u.port:
            scheme = u.scheme.strip().lower()
            if scheme not in _KNOWN_SCHEMES:
                scheme = default_scheme
            return ParsedProxy(
                scheme=scheme,
                host=str(u.hostname),
                port=int(u.port),
                username=str(u.username or ""),
                password=str(u.password or ""),
            )

    scheme = default_scheme.strip().lower()
    if scheme not in _KNOWN_SCHEMES:
        scheme = "http"

    # no-scheme forms
    if "@" in text:
        left, right = text.split("@", 1)
        left = left.strip()
        right = right.strip()
        hostport = None
        creds = None

        if _looks_like_hostport(right):
            hostport = right
            creds = left
        elif _looks_like_hostport(left):
            hostport = left
            creds = right
        else:
            # best effort: assume right side is host:port
            hostport = right
            creds = left

        hp = _parse_hostport(hostport)
        if not hp:
            return None
        host, port = hp
        user, pwd = "", ""
        if creds and ":" in creds:
            user, pwd = creds.split(":", 1)
        return ParsedProxy(scheme=scheme, host=host, port=port, username=user, password=pwd)

    # 4-token variant (best-effort; does not support passwords with ':')
    tokens = text.split(":")
    if len(tokens) == 4:
        a, b, c, d = tokens
        if b.isdigit():
            # host:port:user:pass
            return ParsedProxy(scheme=scheme, host=a, port=int(b), username=c, password=d)
        if d.isdigit():
            # user:pass:host:port
            return ParsedProxy(scheme=scheme, host=c, port=int(d), username=a, password=b)

    hp = _parse_hostport(text)
    if not hp:
        return None
    host, port = hp
    return ParsedProxy(scheme=scheme, host=host, port=port)


def proxy_to_url(raw: str | None, *, default_scheme: str | None = None) -> str:
    """
    Converts supported proxy formats to a URL usable by curl_cffi/libcurl.
    If parsing fails, returns the raw string (trimmed) to preserve old behavior.
    """
    text = (raw or "").strip()
    if not text:
        return ""
    if default_scheme is None:
        default_scheme = env_default_proxy_scheme()
    parsed = parse_proxy(text, default_scheme=str(default_scheme or "http"))
    if not parsed:
        return text
    return parsed.url(for_playwright=False)


def proxy_label(raw: str | None) -> str:
    """Never log credentials. Return only host:port when possible."""
    text = (raw or "").strip()
    if not text:
        return ""

    parsed = parse_proxy(text)
    if parsed:
        return parsed.hostport()

    # Fallback: strip obvious schemes and creds, then mask.
    cleaned = re.sub(r"^[a-zA-Z0-9+.-]+://", "", text)
    if "@" in cleaned:
        cleaned = cleaned.split("@", 1)[-1]
    return mask_sensitive_data(cleaned)


def proxy_to_playwright_config(raw: str | None, *, default_scheme: str | None = None) -> dict | None:
    if default_scheme is None:
        default_scheme = env_default_proxy_scheme()
    parsed = parse_proxy(raw, default_scheme=str(default_scheme or "http"))
    if not parsed:
        return None
    scheme = parsed.scheme
    if scheme == "socks5h":
        scheme = "socks5"
    cfg = {"server": f"{scheme}://{parsed.host}:{parsed.port}"}
    if parsed.username:
        cfg["username"] = parsed.username
        cfg["password"] = parsed.password
    return cfg
