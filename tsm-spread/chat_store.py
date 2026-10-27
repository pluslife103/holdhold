"""Chat transcripts in Upstash Redis (REST) and the outbound bot webhook.

Portfolio snapshots are not read here. deliver_to_bot() is the only function
that calls the bot webhook, and it forwards session_id, message_id, text,
history, and the selected target (key plus display label).
"""

import json
import os
import re
import unicodedata
import uuid
from urllib.parse import urlparse

import requests

TTL_SECONDS = 7 * 24 * 60 * 60
HISTORY_LIMIT = 10
MAX_STORED_MESSAGES = 200
MAX_USER_TEXT = 4000
MAX_REPLY_TEXT = 20000
MAX_FROM_LEN = 30
KEY_PREFIX = "chat:sess:"
DEFAULT_TARGET = "bot"
# Fixed whitelist. The key is stored and forwarded; the value is display-only.
TARGETS = {
    "bot": "雞雞人",
    "media": "自媒體研究社",
    "industry": "產業研究員",
    "gubyen": "股癌逐字稿",
    "holdings": "股市-持股監督者",
    "advisor": "投顧研究專員",
    "entry": "股市-進場專員",
}
_REDIS_UNSET = (
    "聊天儲存未設定：請設定 KV_REST_API_URL 與 KV_REST_API_TOKEN，"
    "或 UPSTASH_REDIS_REST_URL 與 UPSTASH_REDIS_REST_TOKEN。"
)

_SESSION_RE = re.compile(r"^[0-9a-f]{32}$")
_HEADER_NAME_RE = re.compile(r"^[A-Za-z0-9-]{1,64}$")


def new_id() -> str:
    return uuid.uuid4().hex


def valid_id(value) -> bool:
    return isinstance(value, str) and _SESSION_RE.fullmatch(value) is not None


def _redis_pair(url_name: str, token_name: str):
    url = os.environ.get(url_name, "").strip().rstrip("/")
    token = os.environ.get(token_name, "").strip()
    if url and "://" not in url:
        url = "https://" + url
    if url and token:
        return url, token
    return None


def resolve_target(value):
    """Return (key, label). A missing target uses the default; anything else must be a whitelist key."""
    if value is None:
        return DEFAULT_TARGET, TARGETS[DEFAULT_TARGET]
    if not isinstance(value, str) or value not in TARGETS:
        raise ValueError("target 不正確")
    return value, TARGETS[value]


def clean_from(value):
    """Optional display name on a bot reply. Not used for authentication.

    Control characters are removed, then the text is cut to MAX_FROM_LEN.
    Missing or blank values return None.
    """
    if value is None:
        return None
    if not isinstance(value, str):
        raise ValueError("from 必須是字串")
    cleaned = "".join(ch for ch in value if unicodedata.category(ch) != "Cc").strip()
    if not cleaned:
        return None
    return cleaned[:MAX_FROM_LEN]


def redis_config():
    """REST credentials for chat transcripts.

    KV_REST_API_URL / KV_REST_API_TOKEN are used when both are set.
    If that pair is missing, UPSTASH_REDIS_REST_URL / UPSTASH_REDIS_REST_TOKEN
    are used instead. Chat keys stay under chat:sess: and are separate from
    the AI history key read by app.py.
    """
    return (
        _redis_pair("KV_REST_API_URL", "KV_REST_API_TOKEN")
        or _redis_pair("UPSTASH_REDIS_REST_URL", "UPSTASH_REDIS_REST_TOKEN")
    )


def redis_config_error() -> str | None:
    if redis_config() is None:
        return _REDIS_UNSET
    return None


def _pipeline(commands: list) -> list:
    cfg = redis_config()
    if cfg is None:
        raise RuntimeError(_REDIS_UNSET)
    url, token = cfg
    try:
        resp = requests.post(
            f"{url}/pipeline",
            headers={
                "Authorization": f"Bearer {token}",
                "Content-Type": "application/json",
            },
            json=commands,
            timeout=8,
        )
    except requests.RequestException as exc:
        raise RuntimeError(f"無法連上 Redis（{type(exc).__name__}）。") from exc
    if resp.status_code >= 400:
        raise RuntimeError(f"Redis HTTP {resp.status_code}")
    try:
        data = resp.json()
    except ValueError as exc:
        raise RuntimeError("Redis 回應不是 JSON。") from exc
    if not isinstance(data, list):
        raise RuntimeError("Redis 回應格式不正確。")
    out = []
    for item in data:
        if not isinstance(item, dict):
            raise RuntimeError("Redis 回應格式不正確。")
        if item.get("error"):
            err = str(item.get("error"))[:200]
            if token and token in err:
                err = "redis error"
            raise RuntimeError(err)
        out.append(item.get("result"))
    return out


def _key(session_id: str) -> str:
    if not valid_id(session_id):
        raise ValueError("invalid session_id")
    return KEY_PREFIX + session_id


def _decode_message(raw):
    for _ in range(2):
        if not isinstance(raw, str):
            break
        try:
            raw = json.loads(raw)
        except json.JSONDecodeError:
            return None
    if not isinstance(raw, dict):
        return None
    if not isinstance(raw.get("text"), str) or not isinstance(raw.get("id"), str):
        return None
    if raw.get("role") not in ("user", "assistant"):
        return None
    return raw


def load_messages(session_id: str) -> list:
    result = _pipeline([["LRANGE", _key(session_id), 0, -1]])
    rows = result[0] if result else None
    if rows is None:
        rows = []
    if not isinstance(rows, list):
        raise RuntimeError("Redis LRANGE 回應不是列表。")
    messages = []
    for raw in rows:
        message = _decode_message(raw)
        if message is not None:
            messages.append(message)
    return messages


def append_message(session_id: str, message: dict) -> None:
    payload = json.dumps(message, ensure_ascii=False, separators=(",", ":"))
    _pipeline([
        ["RPUSH", _key(session_id), payload],
        ["LTRIM", _key(session_id), -MAX_STORED_MESSAGES, -1],
        ["EXPIRE", _key(session_id), TTL_SECONDS],
    ])


def recent_history(messages: list, limit: int = HISTORY_LIMIT) -> list:
    """Previous turns only. Each item has message_id, role, and text."""
    cleaned = []
    for message in messages:
        role = message.get("role")
        text = message.get("text")
        if role not in ("user", "assistant") or not isinstance(text, str):
            continue
        cleaned.append({
            "message_id": message.get("message_id") or message.get("id"),
            "role": role,
            "text": text[:MAX_REPLY_TEXT],
        })
    return cleaned[-limit:]


def public_message(message: dict) -> dict:
    item = {
        "id": message.get("id"),
        "role": message.get("role"),
        "text": message.get("text"),
        "ts": message.get("ts"),
    }
    reply_to = message.get("reply_to")
    if isinstance(reply_to, str):
        item["reply_to"] = reply_to
    target = message.get("target")
    if isinstance(target, str) and target in TARGETS:
        item["target"] = target
    sender = message.get("from")
    if isinstance(sender, str) and sender:
        item["from"] = sender
    return item


def _webhook_settings():
    """Resolve the outbound webhook URL and auth header.

    BOT_WEBHOOK_KEY_HEADER
        Header name. Default "Authorization".
    BOT_WEBHOOK_KEY_TEMPLATE
        Header value template. Default "Bearer {key}".
        "{key}" is replaced with BOT_WEBHOOK_KEY.
        If the template has no "{key}", the key is appended after a space,
        so the default shape "Bearer <key>" is what a template of "Bearer" produces too.
    """
    url = os.environ.get("BOT_WEBHOOK_URL", "").strip()
    key = os.environ.get("BOT_WEBHOOK_KEY", "").strip()
    header_name = os.environ.get("BOT_WEBHOOK_KEY_HEADER", "Authorization").strip() or "Authorization"
    template = os.environ.get("BOT_WEBHOOK_KEY_TEMPLATE", "Bearer {key}")

    if not url:
        raise RuntimeError("未設定 BOT_WEBHOOK_URL。")
    parsed = urlparse(url)
    if parsed.scheme not in ("http", "https") or not parsed.netloc:
        raise RuntimeError("BOT_WEBHOOK_URL 必須是 http 或 https 網址。")
    if not key:
        raise RuntimeError("未設定 BOT_WEBHOOK_KEY。")
    if not _HEADER_NAME_RE.fullmatch(header_name):
        raise RuntimeError("BOT_WEBHOOK_KEY_HEADER 只能包含英數與連字號。")
    if any(ch in template for ch in ("\r", "\n")) or any(ch in key for ch in ("\r", "\n")):
        raise RuntimeError("webhook 金鑰或標頭格式含有不合法字元。")

    if "{key}" in template:
        header_value = template.replace("{key}", key)
    elif template.strip() == "":
        header_value = key
    else:
        header_value = f"{template} {key}".strip()
    if any(ch in header_value for ch in ("\r", "\n")):
        raise RuntimeError("webhook 標頭值不合法。")
    return url, {header_name: header_value, "Content-Type": "application/json"}


def webhook_config_error() -> str | None:
    try:
        _webhook_settings()
    except RuntimeError as exc:
        return str(exc)
    return None


def deliver_to_bot(payload: dict) -> None:
    """POST one chat turn to BOT_WEBHOOK_URL.

    This is the only webhook call site. The body is rebuilt so only
    session_id, message_id, text, target, target_label, and the last
    ~10 history turns are sent. target_label always comes from TARGETS.
    """
    url, headers = _webhook_settings()
    text = payload.get("text")
    if not isinstance(text, str) or not text.strip():
        raise RuntimeError("webhook 缺少 text。")
    target, target_label = resolve_target(payload.get("target"))
    # Rebuild history so a caller cannot attach extra fields (for example
    # portfolio data) or more than HISTORY_LIMIT turns.
    normalized = []
    for item in payload.get("history") or []:
        if not isinstance(item, dict):
            continue
        normalized.append({
            "id": item.get("message_id") or item.get("id"),
            "role": item.get("role"),
            "text": item.get("text"),
        })
    body = {
        "session_id": payload.get("session_id"),
        "message_id": payload.get("message_id"),
        "text": text.strip(),
        "history": recent_history(normalized, HISTORY_LIMIT),
        "target": target,
        "target_label": target_label,
    }
    try:
        resp = requests.post(
            url,
            headers=headers,
            json=body,
            timeout=8,
            allow_redirects=False,
        )
    except requests.RequestException as exc:
        raise RuntimeError("無法連上 BOT_WEBHOOK_URL。") from exc
    if resp.status_code >= 400:
        raise RuntimeError(f"bot webhook 回應 HTTP {resp.status_code}")


def reply_secret() -> str:
    return os.environ.get("BOT_REPLY_SECRET", "").strip()
