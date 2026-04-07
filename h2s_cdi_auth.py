"""
h2s_cdi_auth.py — Reusable Hack2skill Central Data Intelligence Level 2 RBAC middleware.

SETUP (copy this file into any new module app, then):

  1. Install dependencies:
       pip install PyJWT python-dotenv requests

  2. Set these env vars in your .env (legacy JARVIS_* names still work):
       H2S_CDI_JWT_SECRET          = <same as H2S_CDI_JWT_SECRET in portal .env>
       H2S_CDI_MODULE_ID           = myapp          (slug registered in the portal)
       H2S_CDI_URL                 = http://h2s.tech (public portal URL)
       H2S_CDI_REGISTRATION_SECRET = <same as MODULE_REGISTRATION_SECRET in portal .env>

  3. In your Flask app.py:
       from werkzeug.middleware.proxy_fix import ProxyFix
       app.wsgi_app = ProxyFix(app.wsgi_app, x_for=1, x_proto=1, x_host=1, x_prefix=1)

  4. Decorate routes:
       @app.route("/dashboard")
       @h2s_cdi_auth_required(page="dashboard")
       def dashboard():
           user = g.user
           ...

  5. Call register_with_portal() at startup (see app.py example).

HOW IT WORKS:
  - The portal issues ONE unified JWT (h2s_cdi_session cookie) at login time.
  - When a user opens a module from the portal, the cookie is refreshed with
    the latest page permissions and the browser is redirected to the app.
  - On every module request the decorator reads the cookie, verifies the signature,
    and checks moduleAccess[MODULE_ID] for page-level access.
  - When the token expires, the user is sent to /auth/refresh on the portal.
"""
import os
from functools import wraps

import jwt
import requests
from dotenv import load_dotenv
from flask import g, make_response, redirect, request

load_dotenv()


def _env_first(*names: str, default: str = "") -> str:
    for n in names:
        v = os.environ.get(n)
        if v is not None and str(v).strip() != "":
            return v
    return default


_JWT_SECRET = _env_first("H2S_CDI_JWT_SECRET", "JARVIS_JWT_SECRET")
_MODULE_ID = _env_first("H2S_CDI_MODULE_ID", "JARVIS_MODULE_ID")
_PORTAL_URL = _env_first("H2S_CDI_URL", "JARVIS_URL", default="http://localhost:5050").rstrip("/")
_internal = _env_first("H2S_CDI_INTERNAL_URL", "JARVIS_INTERNAL_URL")
_INTERNAL_URL = (_internal or _PORTAL_URL).rstrip("/")
_REGISTRATION_SECRET = _env_first(
    "H2S_CDI_REGISTRATION_SECRET",
    "JARVIS_REGISTRATION_SECRET",
)

_COOKIE_NAME = "h2s_cdi_session"


def get_portal_url() -> str:
    """Public portal base URL (for redirects and template links)."""
    return _PORTAL_URL


def _decode_token(token: str) -> dict | None:
    """Decode and verify the unified portal JWT. Returns payload or None."""
    if not _JWT_SECRET:
        return None
    try:
        return jwt.decode(
            token,
            _JWT_SECRET,
            algorithms=["HS256"],
            options={"verify_exp": True},
        )
    except Exception:
        return None


def _module_access_value(payload: dict) -> list[str] | None:
    """
    moduleAccess entry for this module (keys matched case-insensitively).
    None  → JWT null, all pages allowed for this module.
    []    → no pages.
    list  → explicit page ids.
    """
    if payload.get("isAdmin"):
        return None
    module_access = payload.get("moduleAccess") or {}
    if not isinstance(module_access, dict):
        return []
    mid = (_MODULE_ID or "").strip().lower()
    if not mid:
        return []
    for k, v in module_access.items():
        if (k or "").strip().lower() == mid:
            if v is None:
                return None
            if isinstance(v, list):
                return v
            return []
    return []


def _allowed_pages(payload: dict) -> list[str] | None:
    return _module_access_value(payload)


def _refresh_url() -> str:
    return f"{_PORTAL_URL}/auth/refresh?module={_MODULE_ID}"


def _first_allowed_path(pages: list[str] | None) -> str:
    _root = (request.environ.get("SCRIPT_NAME") or "").rstrip("/")
    if pages:
        return f"{_root}/{pages[0]}"
    return f"{_root}/dashboard"


def h2s_cdi_auth_required(f=None, *, page: str | None = None):
    """
    Enforce portal JWT authentication on a Flask route.

    On success, g.user holds the decoded JWT payload.
    """
    def decorator(func):
        @wraps(func)
        def wrapped(*args, **kwargs):
            cookie_token = request.cookies.get(_COOKIE_NAME)
            if not cookie_token:
                return redirect(_refresh_url())

            payload = _decode_token(cookie_token)
            if not payload:
                resp = make_response(redirect(_refresh_url()))
                resp.delete_cookie(_COOKIE_NAME, path="/")
                return resp

            g.user = payload

            if page and not payload.get("isAdmin"):
                pages = _allowed_pages(payload)
                if pages is not None and page not in pages:
                    if pages:
                        return redirect(_first_allowed_path(pages))
                    return redirect(f"{_PORTAL_URL}/dashboard")

            return func(*args, **kwargs)
        return wrapped

    if f is not None:
        return decorator(f)
    return decorator


def get_user() -> dict | None:
    return getattr(g, "user", None)


def get_module_pages(user: dict | None = None) -> list[str] | None:
    if user is None:
        user = get_user()
    if user is None:
        return []
    return _module_access_value(user)


def register_with_portal(
    pages: list[dict],
    module_name: str = "",
    base_url: str = "",
) -> bool:
    """
    Register this module and its pages with the portal on startup.

    pages: list of {"pageId", "label", "path"} dicts.
    """
    if not _REGISTRATION_SECRET:
        print(
            f"[h2s_cdi_auth] WARNING: H2S_CDI_REGISTRATION_SECRET (or JARVIS_REGISTRATION_SECRET) not set. "
            f"Module '{_MODULE_ID}' will not register."
        )
        return False

    if not _MODULE_ID:
        print("[h2s_cdi_auth] WARNING: H2S_CDI_MODULE_ID (or JARVIS_MODULE_ID) not set. Skipping registration.")
        return False

    try:
        resp = requests.post(
            f"{_INTERNAL_URL}/api/modules/register",
            json={
                "moduleId": _MODULE_ID,
                "moduleName": module_name or _MODULE_ID.upper(),
                "baseUrl": base_url,
                "pages": pages,
            },
            headers={"x-module-secret": _REGISTRATION_SECRET},
            timeout=5,
        )
        if resp.status_code == 200:
            data = resp.json()
            new = data.get("pagesNew", 0)
            archived = data.get("pagesArchived", 0)
            print(
                f"[h2s_cdi_auth] Registered with portal. "
                f"Pages: {data.get('pagesIncoming', 0)} total, "
                f"{new} new, {archived} archived."
            )
            return True
        print(f"[h2s_cdi_auth] Registration failed: HTTP {resp.status_code} — {resp.text[:200]}")
        return False
    except requests.exceptions.ConnectionError:
        print(
            f"[h2s_cdi_auth] WARNING: Could not reach portal at {_PORTAL_URL}. "
            f"Registration skipped — app will still start."
        )
        return False
    except Exception as exc:
        print(f"[h2s_cdi_auth] Registration error: {exc}")
        return False
