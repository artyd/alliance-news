"""Security helpers for MacroHarvey.

The Telegram Mini App used to trust ``initDataUnsafe.user.id`` sent straight from
the browser — anyone could forge another user's id (IDOR). Telegram signs the
launch payload (``window.Telegram.WebApp.initData``); the backend must verify that
signature and derive the user id from the *verified* data only.

Docs: https://core.telegram.org/bots/webapps#validating-data-received-via-the-mini-app
"""

from __future__ import annotations

import hashlib
import hmac
import json
import os
import time
from urllib.parse import parse_qsl

# Header the frontend attaches to every /api/webapp call.
INIT_DATA_HEADER = "X-Telegram-Init-Data"

# initData older than this is rejected (replay protection). 24h is generous for a
# session that may sit open in a webview.
_MAX_AGE_SECONDS = 24 * 60 * 60


def _secret_key(bot_token: str) -> bytes:
    return hmac.new(b"WebAppData", bot_token.encode(), hashlib.sha256).digest()


def verify_init_data(init_data: str, bot_token: str, max_age: int = _MAX_AGE_SECONDS) -> dict | None:
    """Return the parsed, verified fields if ``init_data`` is authentic, else None.

    ``init_data`` is the raw query-string from ``Telegram.WebApp.initData``.
    """
    if not init_data or not bot_token:
        return None

    try:
        pairs = dict(parse_qsl(init_data, keep_blank_values=True))
    except Exception:
        return None

    received_hash = pairs.pop("hash", None)
    if not received_hash:
        return None

    # Data-check-string: all fields except hash, sorted, joined by newlines.
    data_check_string = "\n".join(f"{k}={pairs[k]}" for k in sorted(pairs))
    computed = hmac.new(
        _secret_key(bot_token), data_check_string.encode(), hashlib.sha256
    ).hexdigest()

    if not hmac.compare_digest(computed, received_hash):
        return None

    # Reject stale payloads.
    auth_date = pairs.get("auth_date")
    if auth_date and max_age > 0:
        try:
            if time.time() - int(auth_date) > max_age:
                return None
        except ValueError:
            return None

    return pairs


def user_id_from_init_data(init_data: str, bot_token: str) -> int | None:
    """Verify ``init_data`` and return the Telegram user id, or None if invalid."""
    verified = verify_init_data(init_data, bot_token)
    if not verified:
        return None
    try:
        user = json.loads(verified.get("user", "{}"))
        uid = int(user.get("id", 0))
        return uid or None
    except (ValueError, json.JSONDecodeError):
        return None


# When true (default) the /api/webapp endpoints require a valid initData signature.
# Can be disabled for local debugging via TG_AUTH_REQUIRED=0, but should stay on
# in production.
AUTH_REQUIRED = os.getenv("TG_AUTH_REQUIRED", "1").strip().lower() not in ("0", "false", "no")
