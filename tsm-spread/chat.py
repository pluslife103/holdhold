"""Chat page and relay routes. Login is enforced by auth.py.

Portfolio snapshots are not read here. The webhook body is built in
chat_store.deliver_to_bot and contains only the user's text and chat history.
"""

import time

from flask import Flask, jsonify, render_template, request, session

import chat_store
from auth import ct_equal

MAX_OWNED_SESSIONS = 20


def _owned_sessions() -> list:
    raw = session.get("chat_sessions")
    if not isinstance(raw, list):
        return []
    return [sid for sid in raw if chat_store.valid_id(sid)]


def _remember(session_id: str) -> None:
    owned = [sid for sid in _owned_sessions() if sid != session_id]
    owned.append(session_id)
    session["chat_sessions"] = owned[-MAX_OWNED_SESSIONS:]
    session["chat_current"] = session_id
    session.permanent = True
    session.modified = True


def _owns(session_id: str) -> bool:
    return session_id in _owned_sessions()


def _current() -> str | None:
    current = session.get("chat_current")
    if isinstance(current, str) and current in _owned_sessions():
        return current
    return None


def init_chat(app: Flask) -> None:
    @app.route("/chat")
    def chat_page():
        return render_template("chat.html")

    @app.route("/api/chat/session", methods=["GET"])
    def chat_session():
        return jsonify({"session_id": _current()})

    @app.route("/api/chat/new", methods=["POST"])
    def chat_new():
        session_id = chat_store.new_id()
        _remember(session_id)
        return jsonify({"session_id": session_id})

    @app.route("/api/chat/send", methods=["POST"])
    def chat_send():
        data = request.get_json(silent=True)
        if not isinstance(data, dict):
            return jsonify({"error": "需要 JSON"}), 400
        text = data.get("text")
        if not isinstance(text, str) or not text.strip():
            return jsonify({"error": "請輸入訊息"}), 400
        text = text.strip()
        if len(text) > chat_store.MAX_USER_TEXT:
            return jsonify({"error": f"訊息過長（上限 {chat_store.MAX_USER_TEXT} 字）"}), 400

        raw_sid = data.get("session_id")
        if raw_sid in (None, ""):
            session_id = _current() or chat_store.new_id()
        else:
            if not chat_store.valid_id(raw_sid):
                return jsonify({"error": "session_id 格式不正確"}), 400
            if not _owns(raw_sid):
                return jsonify({"error": "找不到這個對話"}), 404
            session_id = raw_sid

        storage_error = chat_store.redis_config_error()
        if storage_error:
            return jsonify({"error": storage_error}), 503
        webhook_error = chat_store.webhook_config_error()
        if webhook_error:
            return jsonify({"error": webhook_error}), 503

        message_id = chat_store.new_id()
        user_message = {
            "id": message_id,
            "role": "user",
            "text": text,
            "ts": int(time.time()),
        }
        try:
            prior = chat_store.load_messages(session_id)
            history = chat_store.recent_history(prior, chat_store.HISTORY_LIMIT)
            chat_store.append_message(session_id, user_message)
        except RuntimeError as exc:
            return jsonify({"error": f"無法儲存訊息：{exc}"}), 502

        _remember(session_id)
        try:
            chat_store.deliver_to_bot({
                "session_id": session_id,
                "message_id": message_id,
                "text": text,
                "history": history,
            })
        except RuntimeError as exc:
            return jsonify({
                "ok": False,
                "stored": True,
                "session_id": session_id,
                "message_id": message_id,
                "error": f"訊息已儲存，但沒有送到 bot：{exc}",
            }), 502

        return jsonify({
            "ok": True,
            "session_id": session_id,
            "message_id": message_id,
        })

    @app.route("/api/chat/messages", methods=["GET"])
    def chat_messages():
        session_id = request.args.get("session_id", "")
        if not chat_store.valid_id(session_id):
            return jsonify({"error": "session_id 格式不正確"}), 400
        if not _owns(session_id):
            return jsonify({"error": "找不到這個對話"}), 404
        storage_error = chat_store.redis_config_error()
        if storage_error:
            return jsonify({"error": storage_error}), 503
        try:
            messages = chat_store.load_messages(session_id)
        except RuntimeError as exc:
            return jsonify({"error": f"無法讀取對話：{exc}"}), 502

        after = (request.args.get("after") or "").strip()
        if after:
            index = next((i for i, message in enumerate(messages) if message.get("id") == after), None)
            if index is not None:
                messages = messages[index + 1:]
        return jsonify({
            "session_id": session_id,
            "messages": [chat_store.public_message(message) for message in messages],
        })

    @app.route("/api/bot-reply", methods=["POST"])
    def bot_reply():
        # Exempt from the login cookie. Authenticated only by X-Bot-Secret.
        expected = chat_store.reply_secret()
        if not expected:
            return jsonify({"error": "未設定 BOT_REPLY_SECRET"}), 503
        supplied = request.headers.get("X-Bot-Secret", "")
        if not ct_equal(supplied, expected):
            return jsonify({"error": "unauthorized"}), 401

        data = request.get_json(silent=True)
        if not isinstance(data, dict):
            return jsonify({"error": "需要 JSON"}), 400
        session_id = data.get("session_id")
        message_id = data.get("message_id")
        text = data.get("text")
        if not chat_store.valid_id(session_id):
            return jsonify({"error": "session_id 格式不正確"}), 400
        if not chat_store.valid_id(message_id):
            return jsonify({"error": "message_id 格式不正確"}), 400
        if not isinstance(text, str) or not text.strip():
            return jsonify({"error": "缺少 text"}), 400
        text = text.strip()
        if len(text) > chat_store.MAX_REPLY_TEXT:
            return jsonify({"error": "回覆過長"}), 400

        storage_error = chat_store.redis_config_error()
        if storage_error:
            return jsonify({"error": storage_error}), 503
        try:
            existing = chat_store.load_messages(session_id)
        except RuntimeError as exc:
            return jsonify({"error": f"無法儲存回覆：{exc}"}), 502
        for message in existing:
            # A retried webhook should not append a second copy of the same reply.
            if message.get("role") == "assistant" and message.get("reply_to") == message_id and message.get("text") == text:
                return jsonify({"ok": True, "id": message.get("id"), "duplicate": True})
        reply = {
            "id": chat_store.new_id(),
            "role": "assistant",
            "text": text,
            "ts": int(time.time()),
            "reply_to": message_id,
        }
        try:
            chat_store.append_message(session_id, reply)
        except RuntimeError as exc:
            return jsonify({"error": f"無法儲存回覆：{exc}"}), 502
        return jsonify({"ok": True, "id": reply["id"]})
