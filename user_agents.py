"""
User-Agent rotation helpers.

Avito can correlate repeated requests by identical fingerprints (headers + TLS).
Rotating the User-Agent at least per parser instance helps reduce blocks.

Keep this list realistic (popular desktop browsers). Prefer Chromium UAs, since
the project uses curl_cffi `impersonate` profiles mostly for Chromium-family.
"""

from __future__ import annotations

import random

# NOTE: Avoid exotic or obviously-bot UA strings. These should look like
# real, modern desktop browsers. Versions do not need to be the absolute latest,
# just plausible.
USER_AGENTS: list[str] = [
    # Chrome (Windows)
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/140.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/128.0.0.0 Safari/537.36",
    # Chrome (macOS)
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/140.0.0.0 Safari/537.36",
    # Edge (Windows) - Chromium
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/140.0.0.0 Safari/537.36 Edg/140.0.0.0",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/128.0.0.0 Safari/537.36 Edg/128.0.0.0",
    # Firefox (Windows)
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:118.0) Gecko/20100101 Firefox/118.0",
]


def random_user_agent() -> str:
    return random.choice(USER_AGENTS)

