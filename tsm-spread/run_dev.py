"""Local HTTP server for tsm-spread.

    SITE_PASSWORD=... SESSION_SECRET=... python run_dev.py

SESSION_COOKIE_SECURE is turned off in this process only, so a browser can
keep the session cookie on http://127.0.0.1. Vercel does not run this file.
Leave SESSION_COOKIE_SECURE unset in production so the cookie is Secure.
"""

import os


def main():
    os.environ.setdefault("SESSION_COOKIE_SECURE", "0")
    from app import app

    port = int(os.environ.get("PORT", "5100"))
    print(f"http://127.0.0.1:{port}")
    if not os.environ.get("SITE_PASSWORD", "").strip() or not os.environ.get("SESSION_SECRET", "").strip():
        print("SITE_PASSWORD 或 SESSION_SECRET 未設定，開啟網頁會看到「網站已關閉」。")
    print("本機 cookie 不是 Secure。部署到 Vercel 時不要設定 SESSION_COOKIE_SECURE。")
    app.run(host="127.0.0.1", port=port, debug=True)


if __name__ == "__main__":
    main()
