"""Login gate and chat relay. Redis and the bot webhook are mocked."""

import json
import os
import re
import unittest
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from unittest.mock import patch
from urllib.parse import urlparse

os.environ["SITE_PASSWORD"] = "s3cret-pass"
os.environ["SESSION_SECRET"] = "x" * 32
os.environ["SESSION_COOKIE_SECURE"] = "1"
os.environ["BOT_REPLY_SECRET"] = "reply-secret-value"
os.environ["BOT_WEBHOOK_URL"] = "https://bot.example/hook"
os.environ["BOT_WEBHOOK_KEY"] = "wh-key"
os.environ["KV_REST_API_URL"] = "https://kv.example"
os.environ["KV_REST_API_TOKEN"] = "kv-token"
os.environ.pop("BOT_WEBHOOK_KEY_HEADER", None)
os.environ.pop("BOT_WEBHOOK_KEY_TEMPLATE", None)

from app import app  # noqa: E402
import auth  # noqa: E402
import chat_store  # noqa: E402

BASE = "https://localhost"
PASSWORD = "s3cret-pass"


def _redis_slice(values, start, end):
    n = len(values)
    if n == 0:
        return []
    if start < 0:
        start = max(n + start, 0)
    if end < 0:
        end = n + end
    end = min(end, n - 1)
    if start > end or start >= n:
        return []
    return values[start:end + 1]


class MemoryRedis:
    def __init__(self):
        self.lists = {}
        self.ttls = {}
        self.commands = []

    def pipeline(self, commands):
        self.commands.append(list(commands))
        results = []
        for cmd in commands:
            op = str(cmd[0]).upper()
            key = cmd[1]
            if op == "RPUSH":
                self.lists.setdefault(key, []).append(cmd[2])
                results.append(len(self.lists[key]))
            elif op == "LTRIM":
                self.lists[key] = _redis_slice(self.lists.get(key, []), int(cmd[2]), int(cmd[3]))
                results.append("OK")
            elif op == "EXPIRE":
                self.ttls[key] = int(cmd[2])
                results.append(1)
            elif op == "LRANGE":
                results.append(_redis_slice(self.lists.get(key, []), int(cmd[2]), int(cmd[3])))
            else:
                raise AssertionError(cmd)
        return results


class AuthChatTests(unittest.TestCase):
    def setUp(self):
        self.client = app.test_client()
        auth._failures.clear()
        os.environ["SITE_PASSWORD"] = PASSWORD
        os.environ["SESSION_SECRET"] = "x" * 32
        os.environ["BOT_REPLY_SECRET"] = "reply-secret-value"
        os.environ["BOT_WEBHOOK_URL"] = "https://bot.example/hook"
        os.environ["BOT_WEBHOOK_KEY"] = "wh-key"
        os.environ["KV_REST_API_URL"] = "https://kv.example"
        os.environ["KV_REST_API_TOKEN"] = "kv-token"
        os.environ.pop("BOT_WEBHOOK_KEY_HEADER", None)
        os.environ.pop("BOT_WEBHOOK_KEY_TEMPLATE", None)
        self.redis = MemoryRedis()
        self.webhooks = []
        self.pipeline_patch = patch("chat_store._pipeline", self.redis.pipeline)
        self.webhook_patch = patch("chat_store.deliver_to_bot", self._capture_webhook)
        self.pipeline_patch.start()
        self.webhook_patch.start()

    def tearDown(self):
        self.pipeline_patch.stop()
        self.webhook_patch.stop()

    def _capture_webhook(self, payload):
        self.webhooks.append(payload)

    def _get(self, path, **kwargs):
        kwargs.setdefault("base_url", BASE)
        return self.client.get(path, **kwargs)

    def _post(self, path, **kwargs):
        kwargs.setdefault("base_url", BASE)
        return self.client.post(path, **kwargs)

    def _login(self, password=PASSWORD, next_path="/"):
        return self._post("/login", data={"password": password, "next": next_path})

    def _path(self, location):
        if not location:
            return ""
        if location.startswith("http"):
            parsed = urlparse(location)
            return parsed.path + (("?" + parsed.query) if parsed.query else "")
        return location

    def test_fail_closed_without_password(self):
        os.environ["SITE_PASSWORD"] = "   "
        for path in ("/", "/login", "/chat", "/api/positions", "/api/bot-reply", "/api/current"):
            response = self._get(path)
            self.assertEqual(response.status_code, 503, path)
            body = response.get_data(as_text=True)
            self.assertIn("SITE_PASSWORD", body)
            self.assertIn("fail closed", body)

    def test_fail_closed_without_session_secret(self):
        os.environ["SESSION_SECRET"] = ""
        response = self._get("/")
        self.assertEqual(response.status_code, 503)
        self.assertIn("SESSION_SECRET", response.get_data(as_text=True))

    def test_fail_closed_when_secret_too_short(self):
        os.environ["SESSION_SECRET"] = "too-short"
        response = self._get("/api/positions")
        self.assertEqual(response.status_code, 503)
        self.assertIn("16", response.get_json()["error"])

    def test_unauthenticated_pages_redirect_and_apis_reject(self):
        for path in ("/", "/chat", "/bom", "/ai", "/remote"):
            response = self._get(path)
            self.assertEqual(response.status_code, 302, path)
            self.assertIn("/login", self._path(response.location))
        for path in ("/api/positions", "/api/current", "/api/chat/send", "/api/chat/messages"):
            response = self._get(path) if path != "/api/chat/send" else self._post(
                path, json={"text": "hi"}
            )
            self.assertEqual(response.status_code, 401, path)
            self.assertEqual(response.get_json()["error"], "需要登入")

    def test_wrong_password_rejected_and_sets_no_session(self):
        response = self._login(password="nope", next_path="/chat")
        self.assertEqual(response.status_code, 401)
        self.assertIn("密碼不正確", response.get_data(as_text=True))
        self.assertNotIn("tsm_session=", response.headers.get("Set-Cookie", ""))
        follow = self._get("/chat")
        self.assertEqual(follow.status_code, 302)

    def test_login_rate_limit_blocks_even_the_right_password(self):
        for _ in range(auth.LOGIN_MAX_FAILURES):
            response = self._login(password="nope")
            self.assertEqual(response.status_code, 401)
        blocked = self._login(password=PASSWORD)
        self.assertEqual(blocked.status_code, 429)
        self.assertIn("嘗試次數過多", blocked.get_data(as_text=True))

    def test_login_cookie_flags_and_allows_home_and_chat(self):
        response = self._login(next_path="/chat")
        self.assertEqual(response.status_code, 302)
        self.assertTrue(self._path(response.location).startswith("/chat"))
        cookie = response.headers.get("Set-Cookie", "")
        self.assertIn("tsm_session=", cookie)
        self.assertIn("HttpOnly", cookie)
        self.assertIn("Secure", cookie)
        self.assertIn("SameSite=Lax", cookie)
        self.assertNotIn("Domain=", cookie)
        expires = parsedate_to_datetime(re.search(r"Expires=([^;]+)", cookie).group(1))
        lifetime = (expires - datetime.now(timezone.utc)).total_seconds()
        self.assertAlmostEqual(lifetime, 12 * 60 * 60, delta=120)

        chat = self._get("/chat")
        self.assertEqual(chat.status_code, 200)
        page = chat.get_data(as_text=True)
        self.assertIn("/api/chat/send", page)
        self.assertIn("/api/chat/messages", page)
        self.assertNotIn("positions", page.lower())
        self.assertIn("對方輸入中", page)

        home = self._get("/")
        self.assertEqual(home.status_code, 200)
        home_html = home.get_data(as_text=True)
        self.assertIn('href="/chat"', home_html)
        self.assertIn("/logout", home_html)

        positions = self._get("/api/positions")
        self.assertEqual(positions.status_code, 200)
        self.assertIn("summary", positions.get_json())

    def test_open_redirect_is_dropped(self):
        for bad in ("https://evil.example/phish", "//evil.example", "/\\evil", "/chat?q=<script>"):
            page = self._get("/login?next=" + bad)
            self.assertNotIn("evil.example", page.get_data(as_text=True))
            self.assertNotIn("<script>", page.get_data(as_text=True))
            response = self._login(next_path=bad)
            self.assertNotIn("evil.example", self._path(response.location))
            self.client.delete_cookie("tsm_session")
            auth._failures.clear()

    def test_logout_requires_login_again(self):
        self._login()
        self.assertEqual(self._get("/").status_code, 200)
        logged_out = self._post("/logout")
        self.assertEqual(logged_out.status_code, 302)
        self.assertIn("/login", self._path(logged_out.location))
        self.assertEqual(self._get("/").status_code, 302)
        self.assertEqual(self._get("/api/positions").status_code, 401)

    def test_bot_reply_secret_and_poll_round_trip(self):
        self._login()
        sent = self._post("/api/chat/send", json={"text": "你好"})
        self.assertEqual(sent.status_code, 200)
        body = sent.get_json()
        session_id = body["session_id"]
        message_id = body["message_id"]
        self.assertEqual(len(self.webhooks), 1)
        self.assertEqual(set(self.webhooks[0]), {"session_id", "message_id", "text", "history"})
        self.assertEqual(self.webhooks[0]["text"], "你好")
        self.assertEqual(self.webhooks[0]["history"], [])

        outsider = app.test_client()
        missing = outsider.post(
            "/api/bot-reply",
            json={"session_id": session_id, "message_id": message_id, "text": "嗨"},
            base_url=BASE,
        )
        self.assertEqual(missing.status_code, 401)
        wrong = outsider.post(
            "/api/bot-reply",
            json={"session_id": session_id, "message_id": message_id, "text": "嗨"},
            headers={"X-Bot-Secret": "wrong"},
            base_url=BASE,
        )
        self.assertEqual(wrong.status_code, 401)

        ok = outsider.post(
            "/api/bot-reply",
            json={"session_id": session_id, "message_id": message_id, "text": "嗨，我是 bot"},
            headers={"X-Bot-Secret": "reply-secret-value"},
            base_url=BASE,
        )
        self.assertEqual(ok.status_code, 200)
        again = outsider.post(
            "/api/bot-reply",
            json={"session_id": session_id, "message_id": message_id, "text": "嗨，我是 bot"},
            headers={"X-Bot-Secret": "reply-secret-value"},
            base_url=BASE,
        )
        self.assertEqual(again.status_code, 200)
        self.assertTrue(again.get_json().get("duplicate"))

        polled = self._get(f"/api/chat/messages?session_id={session_id}&after={message_id}")
        self.assertEqual(polled.status_code, 200)
        messages = polled.get_json()["messages"]
        self.assertEqual(len(messages), 1)
        self.assertEqual(messages[0]["role"], "assistant")
        self.assertEqual(messages[0]["text"], "嗨，我是 bot")
        self.assertEqual(messages[0]["reply_to"], message_id)

        full = self._get(f"/api/chat/messages?session_id={session_id}")
        self.assertEqual([m["role"] for m in full.get_json()["messages"]], ["user", "assistant"])

        other = app.test_client()
        other.post("/login", data={"password": PASSWORD, "next": "/"}, base_url=BASE)
        foreign = other.get(f"/api/chat/messages?session_id={session_id}", base_url=BASE)
        self.assertEqual(foreign.status_code, 404)

    def test_history_is_previous_ten_and_ttl_is_seven_days(self):
        self._login()
        first_text = None
        previous_text = None
        for i in range(12):
            text = f"訊息{i}"
            if i == 0:
                first_text = text
            if i == 11:
                previous_text = text
            response = self._post("/api/chat/send", json={"text": text})
            self.assertEqual(response.status_code, 200, response.get_json())
        response = self._post("/api/chat/send", json={"text": "最新"})
        self.assertEqual(response.status_code, 200)
        history = self.webhooks[-1]["history"]
        self.assertEqual(len(history), 10)
        self.assertEqual(history[-1]["text"], previous_text)
        self.assertNotIn(first_text, [item["text"] for item in history])
        self.assertNotIn("最新", [item["text"] for item in history])
        self.assertTrue(all(set(item) == {"message_id", "role", "text"} for item in history))
        expire = [cmd for batch in self.redis.commands for cmd in batch if cmd[0] == "EXPIRE"]
        self.assertTrue(expire)
        self.assertEqual(int(expire[-1][2]), 7 * 24 * 60 * 60)

    def test_send_does_not_touch_positions(self):
        self._login()
        real_open = open

        def guarding_open(file, *args, **kwargs):
            name = file if isinstance(file, (str, os.PathLike)) else ""
            if "positions" in str(name):
                raise AssertionError(f"chat opened {name}")
            return real_open(file, *args, **kwargs)

        with patch("builtins.open", guarding_open):
            response = self._post("/api/chat/send", json={"text": "只是聊天", "positions": [{"code": "2330"}]})
        self.assertEqual(response.status_code, 200)
        self.assertNotIn("positions", self.webhooks[-1])
        joined = json.dumps(self.webhooks[-1], ensure_ascii=False)
        self.assertNotIn("2330", joined)

    def test_missing_redis_or_webhook_does_not_store_or_send(self):
        self._login()
        os.environ["KV_REST_API_URL"] = ""
        response = self._post("/api/chat/send", json={"text": "hi"})
        self.assertEqual(response.status_code, 503)
        self.assertIn("KV_REST_API_URL", response.get_json()["error"])
        self.assertEqual(self.redis.lists, {})
        self.assertEqual(self.webhooks, [])

        os.environ["KV_REST_API_URL"] = "https://kv.example"
        os.environ["BOT_WEBHOOK_URL"] = ""
        response = self._post("/api/chat/send", json={"text": "hi"})
        self.assertEqual(response.status_code, 503)
        self.assertIn("BOT_WEBHOOK_URL", response.get_json()["error"])
        self.assertEqual(self.redis.lists, {})

    def test_webhook_failure_keeps_the_stored_message(self):
        self._login()
        self.webhook_patch.stop()
        try:
            with patch("chat_store.deliver_to_bot", side_effect=RuntimeError("無法連上 BOT_WEBHOOK_URL。")):
                response = self._post("/api/chat/send", json={"text": "還在嗎"})
        finally:
            self.webhook_patch.start()
        self.assertEqual(response.status_code, 502)
        body = response.get_json()
        self.assertTrue(body["stored"])
        listed = self._get(f"/api/chat/messages?session_id={body['session_id']}")
        self.assertEqual(listed.get_json()["messages"][0]["text"], "還在嗎")

    def test_webhook_header_defaults_and_overrides(self):
        captured = {}

        class Resp:
            status_code = 200

        def fake_post(url, headers=None, json=None, timeout=None, allow_redirects=None):
            captured["url"] = url
            captured["headers"] = headers
            captured["json"] = json
            captured["timeout"] = timeout
            captured["allow_redirects"] = allow_redirects
            return Resp()

        payload = {
            "session_id": "a" * 32,
            "message_id": "b" * 32,
            "text": "ping",
            "history": [{"message_id": "c" * 32, "role": "user", "text": "old", "positions": "nope"}],
            "positions": [{"code": "2330"}],
        }
        self.webhook_patch.stop()
        try:
            with patch("chat_store.requests.post", fake_post):
                chat_store.deliver_to_bot(payload)
            self.assertEqual(captured["url"], "https://bot.example/hook")
            self.assertEqual(captured["headers"]["Authorization"], "Bearer wh-key")
            self.assertEqual(captured["headers"]["Content-Type"], "application/json")
            self.assertEqual(captured["timeout"], 8)
            self.assertIs(captured["allow_redirects"], False)
            self.assertEqual(set(captured["json"]), {"session_id", "message_id", "text", "history"})
            self.assertEqual(captured["json"]["history"], [{
                "message_id": "c" * 32,
                "role": "user",
                "text": "old",
            }])

            os.environ["BOT_WEBHOOK_KEY_HEADER"] = "X-Api-Key"
            os.environ["BOT_WEBHOOK_KEY_TEMPLATE"] = "{key}"
            with patch("chat_store.requests.post", fake_post):
                chat_store.deliver_to_bot(payload)
            self.assertEqual(captured["headers"]["X-Api-Key"], "wh-key")
            self.assertNotIn("Authorization", captured["headers"])

            os.environ["BOT_WEBHOOK_KEY_HEADER"] = "Authorization\r\nX-Evil"
            with self.assertRaises(RuntimeError):
                chat_store.deliver_to_bot(payload)
        finally:
            self.webhook_patch.start()

    def test_webhook_rejects_non_http_url_without_calling_out(self):
        os.environ["BOT_WEBHOOK_URL"] = "file:///etc/passwd"
        self.webhook_patch.stop()
        try:
            with patch("chat_store.requests.post") as post:
                with self.assertRaises(RuntimeError):
                    chat_store.deliver_to_bot({"text": "x"})
                post.assert_not_called()
        finally:
            self.webhook_patch.start()

    def test_redis_rest_shape(self):
        seen = {}

        class Resp:
            status_code = 200

            def json(self):
                return [{"result": []}]

        def fake_post(url, headers=None, json=None, timeout=None):
            seen["url"] = url
            seen["headers"] = headers
            seen["json"] = json
            return Resp()

        self.pipeline_patch.stop()
        try:
            with patch("chat_store.requests.post", fake_post):
                self.assertEqual(chat_store.load_messages("d" * 32), [])
        finally:
            self.pipeline_patch.start()
        self.assertEqual(seen["url"], "https://kv.example/pipeline")
        self.assertEqual(seen["headers"]["Authorization"], "Bearer kv-token")
        self.assertNotIn("kv-token", seen["url"])
        self.assertEqual(seen["json"], [["LRANGE", "chat:sess:" + "d" * 32, 0, -1]])

    def test_constant_time_compare_and_secure_default(self):
        self.assertTrue(auth.ct_equal("same", "same"))
        self.assertFalse(auth.ct_equal("same", "same "))
        self.assertFalse(auth.ct_equal("short", "much-longer-value"))
        self.assertFalse(auth.ct_equal("密碼", "密码"))
        with patch.dict(os.environ, {}, clear=False):
            os.environ.pop("SESSION_COOKIE_SECURE", None)
            self.assertTrue(auth.cookie_secure())
            os.environ["SESSION_COOKIE_SECURE"] = "0"
            self.assertFalse(auth.cookie_secure())
            os.environ["SESSION_COOKIE_SECURE"] = "false"
            self.assertFalse(auth.cookie_secure())

    def test_bot_reply_unset_secret_is_closed(self):
        os.environ["BOT_REPLY_SECRET"] = ""
        response = self._post(
            "/api/bot-reply",
            json={"session_id": "a" * 32, "message_id": "b" * 32, "text": "nope"},
            headers={"X-Bot-Secret": ""},
        )
        self.assertEqual(response.status_code, 503)
        self.assertIn("BOT_REPLY_SECRET", response.get_json()["error"])

    def test_chat_sources_do_not_reference_positions(self):
        root = os.path.dirname(__file__)
        for name in ("chat.py", "chat_store.py", "templates/chat.html"):
            with open(os.path.join(root, name), encoding="utf-8") as handle:
                source = handle.read().lower()
            self.assertNotIn("positions.json", source)
            self.assertNotIn("/api/positions", source)


if __name__ == "__main__":
    unittest.main()
