"""
culinary_ops_client.py — thin HTTP client for the culinary-ops NestJS backend.

Purpose
-------
This module exists so the betterday-app Flask routes can be migrated off
Google Apps Script one phase at a time without forcing every route to
hand-write its own `requests.get()` + JWT header + retry + fallback block.

The client is dormant by default. It only does anything when the
`CULINARY_OPS_ENABLED` env var is truthy ("1", "true", "yes", "on" — case
insensitive). When disabled, every method short-circuits and returns the
caller-provided fallback (or `None`). This is intentional: the module
ships to production behind a feature flag so we can merge it without
flipping the cutover.

Usage from a Flask route
------------------------
    from culinary_ops_client import get_culinary_client

    @app.route('/manager/dashboard')
    @manager_required
    def manager_dashboard():
        co = get_culinary_client()
        employees = co.get(
            '/corp-manager/employees',
            fallback=lambda: _gas_post({
                'action': 'get_employees',
                'company_id': session['manager_company_id'],
            }),
        )
        ...

When CULINARY_OPS_ENABLED is unset, `co.get(...)` skips the HTTP call
entirely and returns `fallback()`. When it's set, it tries culinary-ops
first; on any failure (timeout, non-2xx, json error) it logs the
discrepancy to the migration diff log and falls back to GAS, so prod
never breaks.

Auth
----
The client picks up a JWT from the Flask session if present
(`session['culinary_ops_jwt']`). Phase 1 routes are read-only and the
culinary-ops dev/staging env can be configured to accept anonymous reads
for the migration window — but the JWT field is wired now so Phase 2
(auth migration) can drop the token in without changing any callsites.

Design notes
------------
- Pure stdlib + `requests` (already in requirements.txt). No new deps.
- All public methods return either parsed JSON, the fallback result, or
  `None` — never raise. Mirrors `_gas_post()`'s graceful-failure shape.
- Discrepancies are logged via the standard `logging` module under the
  `culinary_ops` logger so they show up in Render logs without needing
  a separate file. Phase 1 verification (sub-020) will tee these to
  `data/migration_diff.log` for the dual-read comparison.
- Methods accept `**params` which become query string args on GET, JSON
  body on the others. Keeps callsites short.
- `__init__` takes a `session_getter` so unit tests can inject a fake
  Flask session without spinning up a real app context.
"""

from __future__ import annotations

import logging
import os
from typing import Any, Callable, Optional

import requests

log = logging.getLogger('culinary_ops')


# ─────────────────────────────────────────────────────────────
# CONFIG (read once at import; env-driven so prod is unaffected)
# ─────────────────────────────────────────────────────────────
CULINARY_OPS_BASE_URL = os.environ.get(
    'CULINARY_OPS_BASE_URL',
    'https://culinary-ops-backend-production.up.railway.app',
)

# Default timeout for any single HTTP call to culinary-ops. Kept short so
# a stalled culinary-ops doesn't add latency to the dashboard — the GAS
# fallback fires fast.
CULINARY_OPS_TIMEOUT_S = float(os.environ.get('CULINARY_OPS_TIMEOUT_S', '5'))


def _is_enabled() -> bool:
    """Single source of truth for the feature flag.

    Re-read on every check (not cached) so flipping the env var via Render
    config takes effect on the next request without a redeploy.
    """
    raw = os.environ.get('CULINARY_OPS_ENABLED', '').strip().lower()
    return raw in ('1', 'true', 'yes', 'on')


# ─────────────────────────────────────────────────────────────
# CLIENT
# ─────────────────────────────────────────────────────────────
class CulinaryOpsClient:
    """Thin wrapper over `requests` for talking to the culinary-ops API.

    Construct via `get_culinary_client()` from inside a Flask request so
    the JWT pickup works. For tests, instantiate directly with a custom
    `session_getter` callable that returns a dict.
    """

    def __init__(
        self,
        base_url: str = CULINARY_OPS_BASE_URL,
        timeout: float = CULINARY_OPS_TIMEOUT_S,
        session_getter: Optional[Callable[[], dict]] = None,
        http: Any = None,
    ) -> None:
        # Strip trailing slash so callers can write '/corp-manager/employees'
        # and we always end up with exactly one '/' between base + path.
        self.base_url = base_url.rstrip('/')
        self.timeout = timeout
        self._session_getter = session_getter
        # Injectable HTTP layer so unit tests can pass a Mock.
        self._http = http or requests

    # ── Auth header builder ────────────────────────────────────
    def _auth_headers(self) -> dict:
        headers = {'Accept': 'application/json'}
        jwt = self._jwt()
        if jwt:
            headers['Authorization'] = f'Bearer {jwt}'
        return headers

    def _jwt(self) -> Optional[str]:
        """Pull JWT from injected session_getter or Flask session.

        Returns None if there's no Flask context (e.g. unit tests without
        a session_getter) — caller will fire the request unauthenticated,
        which is fine for read-only Phase 1 routes once culinary-ops dev
        is configured to accept them.
        """
        if self._session_getter is not None:
            try:
                return (self._session_getter() or {}).get('culinary_ops_jwt')
            except Exception:
                return None
        # Lazy import so this module is importable outside a Flask app
        # context (matters for unit tests + scripts).
        try:
            from flask import session as flask_session  # type: ignore
            return flask_session.get('culinary_ops_jwt')
        except Exception:
            return None

    # ── Internal request runner ────────────────────────────────
    def _request(
        self,
        method: str,
        path: str,
        *,
        params: Optional[dict] = None,
        json_body: Optional[dict] = None,
        fallback: Optional[Callable[[], Any]] = None,
    ) -> Any:
        # Short-circuit when disabled. Critical: this means importing
        # the module + constructing the client costs nothing in prod.
        if not _is_enabled():
            return fallback() if fallback else None

        url = f'{self.base_url}{path if path.startswith("/") else "/" + path}'
        try:
            resp = self._http.request(
                method,
                url,
                params=params,
                json=json_body,
                headers=self._auth_headers(),
                timeout=self.timeout,
            )
        except Exception as ex:
            log.warning(
                'culinary-ops %s %s failed (transport): %s', method, path, ex,
            )
            return fallback() if fallback else None

        # Treat anything outside 2xx as a soft failure → fallback.
        status = getattr(resp, 'status_code', 0)
        if not (200 <= status < 300):
            log.warning(
                'culinary-ops %s %s returned %s', method, path, status,
            )
            return fallback() if fallback else None

        try:
            return resp.json()
        except Exception as ex:
            log.warning(
                'culinary-ops %s %s returned non-json: %s', method, path, ex,
            )
            return fallback() if fallback else None

    # ── Public methods ─────────────────────────────────────────
    def get(
        self,
        path: str,
        *,
        fallback: Optional[Callable[[], Any]] = None,
        **params: Any,
    ) -> Any:
        """GET /path?…params. **params become query string args."""
        return self._request(
            'GET', path, params=params or None, fallback=fallback,
        )

    def post(
        self,
        path: str,
        json_body: Optional[dict] = None,
        *,
        fallback: Optional[Callable[[], Any]] = None,
    ) -> Any:
        """POST /path with optional JSON body."""
        return self._request(
            'POST', path, json_body=json_body, fallback=fallback,
        )

    def patch(
        self,
        path: str,
        json_body: Optional[dict] = None,
        *,
        fallback: Optional[Callable[[], Any]] = None,
    ) -> Any:
        """PATCH /path with optional JSON body."""
        return self._request(
            'PATCH', path, json_body=json_body, fallback=fallback,
        )

    def put(
        self,
        path: str,
        json_body: Optional[dict] = None,
        *,
        fallback: Optional[Callable[[], Any]] = None,
    ) -> Any:
        """PUT /path with optional JSON body."""
        return self._request(
            'PUT', path, json_body=json_body, fallback=fallback,
        )

    def delete(
        self,
        path: str,
        *,
        fallback: Optional[Callable[[], Any]] = None,
        **params: Any,
    ) -> Any:
        """DELETE /path?…params."""
        return self._request(
            'DELETE', path, params=params or None, fallback=fallback,
        )


# ─────────────────────────────────────────────────────────────
# FACTORY
# ─────────────────────────────────────────────────────────────
def get_culinary_client() -> CulinaryOpsClient:
    """Default constructor for use inside Flask request handlers.

    Always returns a fresh client so any future per-request state
    (request ID for logging, X-Forwarded-For passthrough, etc.) can be
    threaded in without changing callers.
    """
    return CulinaryOpsClient()
