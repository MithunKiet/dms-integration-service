"""Security utilities: API key hashing, verification, generation, and endpoint checks."""

from __future__ import annotations

import uuid
from typing import List

import bcrypt


def hash_api_key(raw_key: str) -> str:
    """Hash *raw_key* using bcrypt and return the resulting hash string.

    Args:
        raw_key: The plain-text API key to hash.

    Returns:
        A bcrypt hash string suitable for storage.
    """
    encoded = raw_key.encode("utf-8")
    hashed = bcrypt.hashpw(encoded, bcrypt.gensalt())
    return hashed.decode("utf-8")


def verify_api_key(raw_key: str, hashed_key: str) -> bool:
    """Verify that *raw_key* matches *hashed_key* using a constant-time comparison.

    The bcrypt checkpw result is compared with :func:`secrets.compare_digest`
    to prevent timing-based side-channel attacks.

    Args:
        raw_key: The plain-text API key provided by the caller.
        hashed_key: The stored bcrypt hash to verify against.

    Returns:
        ``True`` if the key matches, ``False`` otherwise.
    """
    try:
        encoded_raw = raw_key.encode("utf-8")
        encoded_hash = hashed_key.encode("utf-8")
        return bcrypt.checkpw(encoded_raw, encoded_hash)
    except Exception:
        return False


def generate_api_key(prefix: str = "") -> tuple[str, str]:
    """Generate a new API key and return both the raw and hashed forms.

    The raw key has the structure ``<prefix>.<uuid4_token>`` where the UUID
    token provides 122 bits of entropy.

    Args:
        prefix: Optional short prefix embedded at the start of the key
            (e.g. a client identifier like ``"svc-erp"``).

    Returns:
        A tuple ``(raw_key, hashed_key)`` where *raw_key* should be delivered
        to the caller exactly once and *hashed_key* should be persisted.
    """
    token = str(uuid.uuid4()).replace("-", "")
    raw_key = f"{prefix}.{token}" if prefix else token
    hashed_key = hash_api_key(raw_key)
    return raw_key, hashed_key


def is_endpoint_allowed(allowed_endpoints: List[str], requested_endpoint: str) -> bool:
    """Determine whether *requested_endpoint* is permitted by *allowed_endpoints*.

    The allowlist supports a wildcard entry of ``"*"`` that grants access to
    every endpoint.  Each non-wildcard entry is matched by exact string equality.

    Args:
        allowed_endpoints: List of permitted endpoint path patterns.
        requested_endpoint: The HTTP path of the incoming request.

    Returns:
        ``True`` if the endpoint is allowed, ``False`` otherwise.
    """
    if "*" in allowed_endpoints:
        return True
    return requested_endpoint in allowed_endpoints
