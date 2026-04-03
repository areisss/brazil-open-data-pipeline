"""ETag/hash checker for short-circuiting unchanged data sources.

Used with ShortCircuitOperator to skip the entire pipeline when the remote
source file hasn't changed since the last successful run. Stores ETags in a
local JSON file so state persists across DAG runs.
"""

import hashlib
import json
import os
from pathlib import Path

import requests

ETAG_STORE = "/opt/airflow/data/.etag_cache.json"


def _load_etags() -> dict:
    if os.path.exists(ETAG_STORE):
        return json.loads(Path(ETAG_STORE).read_text())
    return {}


def _save_etags(etags: dict) -> None:
    os.makedirs(os.path.dirname(ETAG_STORE), exist_ok=True)
    Path(ETAG_STORE).write_text(json.dumps(etags, indent=2))


def check_source_changed(source_key: str, url: str, **kwargs) -> bool:
    """Return True if the source has changed (pipeline should run).

    Issues a HEAD request and compares ETag or Content-Length+Last-Modified.
    If the source returns no caching headers, always returns True (run).
    """
    etags = _load_etags()
    previous = etags.get(source_key)

    try:
        resp = requests.head(url, timeout=30, allow_redirects=True)
        resp.raise_for_status()
    except requests.RequestException:
        # Can't reach source — run the pipeline anyway (it will fail at download)
        return True

    # Build a fingerprint from whatever headers the source provides
    etag = resp.headers.get("ETag")
    last_modified = resp.headers.get("Last-Modified")
    content_length = resp.headers.get("Content-Length")

    if etag:
        current = etag
    elif last_modified and content_length:
        current = f"{last_modified}:{content_length}"
    else:
        # No caching headers — always run
        return True

    if current == previous:
        # Source unchanged — short-circuit
        return False

    # Source changed — update cache and continue
    etags[source_key] = current
    _save_etags(etags)
    return True


def hash_local_file(filepath: str) -> str:
    """SHA-256 hash of a local file. Used after download to detect changes."""
    h = hashlib.sha256()
    with open(filepath, "rb") as f:
        for chunk in iter(lambda: f.read(8192), b""):
            h.update(chunk)
    return h.hexdigest()
