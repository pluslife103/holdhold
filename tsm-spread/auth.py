"""Password gate for the whole site.

Secrets come only from the environment. If SITE_PASSWORD or SESSION_SECRET
is missing, every request is rejected (fail closed).

Login rate limiting is in-process memory. On Vercel each serverless isolate
has its own memory and can be frozen or replaced at any time, so the limit
is best-effort: it slows a burst against one warm instance, not a global
distributed attack.
"""

import hashlib
import hmac
import os
import threading
import time
from collections import defaultdict, deque
from datetime import timedelta
from urllib.parse import quote

from flask import Flask, jsonify, redirect, render_template, request, session

SESSION_HOURS = 12
LOGIN_WINDOW_SEC = 15 * 60
LOGIN_MAX_FAILURES = 8
MIN_SECRET_LEN = 16
MAX_PASSWORD_LEN = 256
_MAX_TRACKED_IPS = 2000

_lock = threading.Lock()
_failures: dict[str, deque] = defaultdict(deque)


def site_password() -> str:
    # Surrounding whitespace is ignored so a trailing newline in the
    # Vercel UI does not make the password impossible to type.
    return os.environ.get("SITE_PASSWORD", "").strip()


def session_secret() -> str:
    return os.environ.get("SESSION_SECRET", "").strip()


def cookie_secure() -> bool:
    """Secure flag. Default on. run_dev.py sets SESSION_COOKIE_SECURE=0 for http://127.0.0.1."""
    raw = os.environ.get("SESSION_COOKIE_SECURE", "1").strip().lower()
    return raw not in {"0", "false", "no"}


def config_error() -> str | None:
    problems = []
    password = site_password()
    secret = session_secret()
    if not password:
        problems.append("未設定 SITE_PASSWORD")
    if not secret:
        problems.append("未設定 SESSION_SECRET")
    elif len(secret) < MIN_SECRET_LEN:
        problems.append("SESSION_SECRET 至少需要 16 個字元")
    if not problems:
        return None
    return "網站已關閉（fail closed）：" + "；".join(problems) + "。請在部署環境補上環境變數後再試。"


def ct_equal(a: str, b: str) -> bool:
    """Constant-time string compare.

    Hashing both sides first keeps the comparison time independent of the
    input length (hmac.compare_digest on raw strings returns early when
    the lengths differ).
    """
    digest_a = hashlib.sha256(a.encode("utf-8")).digest()
    digest_b = hashlib.sha256(b.encode("utf-8")).digest()
    return hmac.compare_digest(digest_a, digest_b)


def client_ip() -> str:
    # Vercel sets X-Real-IP. X-Forwarded-For is only a fallback because
    # a client can prepend addresses to that header.
    real = (request.headers.get("X-Real-IP") or "").strip()
    if real:
        return real[:80]
    forwarded = (request.headers.get("X-Forwarded-For") or "").split(",")[0].strip()
    if forwarded:
        return forwarded[:80]
    return (request.remote_addr or "unknown")[:80]


def _prune(bucket: deque, now: float) -> None:
    while bucket and now - bucket[0] > LOGIN_WINDOW_SEC:
        bucket.popleft()


def login_retry_after(ip: str) -> int:
    """Seconds until this IP may try again, or 0 if a try is allowed."""
    now = time.time()
    with _lock:
        bucket = _failures.get(ip)
        if not bucket:
            return 0
        _prune(bucket, now)
        if not bucket:
            _failures.pop(ip, None)
            return 0
        if len(bucket) >= LOGIN_MAX_FAILURES:
            return max(1, int(LOGIN_WINDOW_SEC - (now - bucket[0])) + 1)
        return 0


def record_login_failure(ip: str) -> None:
    now = time.time()
    with _lock:
        if len(_failures) >= _MAX_TRACKED_IPS and ip not in _failures:
            for key in list(_failures)[: _MAX_TRACKED_IPS // 2]:
                _failures.pop(key, None)
        bucket = _failures[ip]
        _prune(bucket, now)
        bucket.append(now)


def clear_login_failures(ip: str) -> None:
    with _lock:
        _failures.pop(ip, None)


def is_logged_in() -> bool:
    return session.get("auth") is True


def _wants_json() -> bool:
    return request.path.startswith("/api/")


def safe_next(raw: str | None) -> str:
    """Only same-site absolute paths. Anything else becomes '/'."""
    if not raw:
        return "/"
    value = raw.strip()
    if not value.startswith("/") or value.startswith("//") or value.startswith("/\\"):
        return "/"
    if "\\" in value or "://" in value:
        return "/"
    if any(ord(ch) < 32 or ch in '<>"\'`' for ch in value):
        return "/"
    return value


def _unconfigured_response():
    message = config_error() or "網站未完成設定。"
    if _wants_json():
        return jsonify({"error": message, "configured": False}), 503
    return render_template("unconfigured.html", message=message), 503


def _install_secret(app: Flask) -> None:
    """Bind SESSION_SECRET before Flask opens the session.

    The session cookie is opened when the request context is pushed, which
    is earlier than before_request. A missing or too-short secret clears the
    key so a stale key cannot keep signing cookies.
    """
    secret = session_secret()
    app.secret_key = secret if len(secret) >= MIN_SECRET_LEN else None


def init_auth(app: Flask) -> None:
    _install_secret(app)
    original_wsgi = app.wsgi_app

    def _bind_secret(environ, start_response):
        _install_secret(app)
        return original_wsgi(environ, start_response)

    app.wsgi_app = _bind_secret
    app.config.update(
        SESSION_COOKIE_NAME="tsm_session",
        SESSION_COOKIE_HTTPONLY=True,
        SESSION_COOKIE_SECURE=cookie_secure(),
        SESSION_COOKIE_SAMESITE="Lax",
        PERMANENT_SESSION_LIFETIME=timedelta(hours=SESSION_HOURS),
        SESSION_REFRESH_EACH_REQUEST=True,
    )
    # Readable Chinese in JSON errors. Flask 3 defaults may already be False.
    app.json.ensure_ascii = False

    @app.before_request
    def _require_login():
        if config_error():
            return _unconfigured_response()

        app.secret_key = session_secret()

        path = request.path
        if path == "/login" or path == "/api/bot-reply":
            return None
        if is_logged_in():
            return None
        if _wants_json():
            return jsonify({"error": "需要登入"}), 401

        target = request.full_path if request.query_string else request.path
        if target.endswith("?"):
            target = target[:-1]
        return redirect("/login?next=" + quote(safe_next(target), safe="/?=&"))

    @app.after_request
    def _private_headers(response):
        response.headers["Cache-Control"] = "no-store"
        response.headers["X-Content-Type-Options"] = "nosniff"
        response.headers["X-Frame-Options"] = "DENY"
        response.headers["Referrer-Policy"] = "same-origin"
        response.headers["X-Robots-Tag"] = "noindex, nofollow"
        return response

    @app.route("/login", methods=["GET", "POST"])
    def login():
        if config_error():
            return _unconfigured_response()
        app.secret_key = session_secret()

        if request.method == "GET":
            nxt = safe_next(request.args.get("next"))
            if is_logged_in():
                return redirect(nxt)
            return render_template("login.html", error=None, next=nxt)

        nxt = safe_next(request.form.get("next") or request.args.get("next"))
        ip = client_ip()
        wait = login_retry_after(ip)
        if wait:
            return render_template(
                "login.html",
                error=f"嘗試次數過多，請約 {wait} 秒後再試。",
                next=nxt,
            ), 429

        supplied = request.form.get("password", "")
        if not isinstance(supplied, str):
            supplied = ""
        matched = len(supplied) <= MAX_PASSWORD_LEN and ct_equal(supplied, site_password())
        if matched:
            clear_login_failures(ip)
            session.clear()
            session["auth"] = True
            session.permanent = True
            session.modified = True
            return redirect(nxt)

        record_login_failure(ip)
        return render_template("login.html", error="密碼不正確。", next=nxt), 401

    @app.route("/logout", methods=["POST"])
    def logout():
        if config_error():
            return _unconfigured_response()
        app.secret_key = session_secret()
        session.clear()
        return redirect("/login")
