"""Unit tests for Telegram initData verification.

Pure functions, no DB or network — run with:  python -m pytest tests/ -q
or directly:  python tests/test_security.py
"""

import hashlib
import hmac
import os
import sys
import time
from urllib.parse import urlencode

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app.security import verify_init_data, user_id_from_init_data

BOT_TOKEN = "123456:TEST-TOKEN-abcDEF"


def _make_init_data(user_id: int, auth_date: int | None = None) -> str:
    """Build a correctly-signed initData string, like Telegram would."""
    fields = {
        "auth_date": str(auth_date if auth_date is not None else int(time.time())),
        "query_id": "AAABBBCCC",
        "user": '{"id":%d,"first_name":"Test"}' % user_id,
    }
    data_check_string = "\n".join(f"{k}={fields[k]}" for k in sorted(fields))
    secret = hmac.new(b"WebAppData", BOT_TOKEN.encode(), hashlib.sha256).digest()
    fields["hash"] = hmac.new(secret, data_check_string.encode(), hashlib.sha256).hexdigest()
    return urlencode(fields)


def test_valid_signature_passes():
    data = _make_init_data(777)
    assert verify_init_data(data, BOT_TOKEN) is not None
    assert user_id_from_init_data(data, BOT_TOKEN) == 777


def test_tampered_user_fails():
    data = _make_init_data(777)
    # Attacker swaps the user id but keeps the original hash.
    tampered = data.replace("%22id%22%3A777", "%22id%22%3A999")
    assert verify_init_data(tampered, BOT_TOKEN) is None
    assert user_id_from_init_data(tampered, BOT_TOKEN) is None


def test_wrong_token_fails():
    data = _make_init_data(777)
    assert verify_init_data(data, "999999:WRONG") is None


def test_missing_hash_fails():
    assert verify_init_data("user=%7B%22id%22%3A1%7D&auth_date=1", BOT_TOKEN) is None


def test_empty_inputs_fail():
    assert verify_init_data("", BOT_TOKEN) is None
    assert verify_init_data("x=1", "") is None


def test_stale_auth_date_fails():
    old = int(time.time()) - 48 * 3600
    data = _make_init_data(777, auth_date=old)
    assert verify_init_data(data, BOT_TOKEN, max_age=24 * 3600) is None


if __name__ == "__main__":
    passed = 0
    for name, fn in sorted(globals().items()):
        if name.startswith("test_") and callable(fn):
            fn()
            print(f"  PASS {name}")
            passed += 1
    print(f"\n{passed} tests passed")
