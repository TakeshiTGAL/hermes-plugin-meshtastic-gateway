"""Allowlist, packet classification, UTF-8 chunks, and send budget.

No radio, no Hermes import. An empty allowlist matches nobody. A channel
packet is not a direct message. Chunk size cannot exceed 200 bytes, which
is under the Meshtastic® DATA_PAYLOAD_LEN of 233.
"""
from __future__ import annotations

import re
import unicodedata
from urllib.parse import urlparse

BROADCAST = 0xFFFFFFFF
CHUNK_BYTES_CEILING = 200
# The approval prefix "Reply /approve or /deny (once only). Run: " is 42 bytes. The floor leaves room for it
# and a short command, so a short approval question can still go out in one chunk.
CHUNK_BYTES_FLOOR = 64
MAX_CHUNKS_CEILING = 8
MIN_GAP_FLOOR_SECONDS = 10
MAX_PER_HOUR_CEILING = 30
HOUR_SECONDS = 3600
CUT_MARK = " [cut]"

DEFAULT_CHUNK_BYTES = 200
DEFAULT_MAX_CHUNKS = 4
DEFAULT_MIN_GAP_SECONDS = 20
DEFAULT_MAX_PER_HOUR = 12


def _clamp_int(value: object, default: int, low: int, high: int) -> int:
    try:
        number = int(value)  # type: ignore[arg-type]
    except (TypeError, ValueError):
        return default
    return max(low, min(high, number))


def chunk_bytes(value: object) -> int:
    return _clamp_int(value, DEFAULT_CHUNK_BYTES, CHUNK_BYTES_FLOOR, CHUNK_BYTES_CEILING)


def max_chunks(value: object) -> int:
    return _clamp_int(value, DEFAULT_MAX_CHUNKS, 1, MAX_CHUNKS_CEILING)


def min_gap_seconds(value: object) -> int:
    return _clamp_int(value, DEFAULT_MIN_GAP_SECONDS, MIN_GAP_FLOOR_SECONDS, HOUR_SECONDS)


def max_per_hour(value: object) -> int:
    return _clamp_int(value, DEFAULT_MAX_PER_HOUR, 1, MAX_PER_HOUR_CEILING)


def node_id(value: object) -> str | None:
    """Canonical '!aabbccdd', or None when the value is empty, broadcast, or not a node id.

    A leading '!' or '0x' is hexadecimal, including ids whose digits are all 0-9.
    A bare number with no a-f digit is decimal.
    A negative value, or a value above 0xffffffff, is refused before any mask.
    '!1aabbccdd' and -5 must not become another node's id.
    """
    if isinstance(value, bool) or value is None:
        return None
    if isinstance(value, int):
        number = value
    else:
        text = str(value).strip().lower()
        if not text or text in {"broadcast", "^all", "all"}:
            return None
        if text.startswith("!"):
            text = text[1:]
            base = 16
        elif text.startswith("0x"):
            base = 16
        elif any(ch in "abcdef" for ch in text):
            base = 16
        else:
            base = 10
        try:
            number = int(text, base)
        except ValueError:
            return None
    if number < 0 or number > 0xFFFFFFFF:
        return None
    if number in {0, BROADCAST}:
        return None
    return f"!{number:08x}"


def _allow_parts(raw: object) -> list[str]:
    if raw is None:
        return []
    if isinstance(raw, (list, tuple, set)):
        return [str(item).strip() for item in raw if str(item).strip()]
    return [part.strip() for part in str(raw).replace("\n", ",").split(",") if part.strip()]


def allowlist_problem(raw: object) -> str | None:
    """None when the list is empty on purpose, or every entry is a node id.

    A blank list answers nobody. A token that is not a node id refuses the
    connection instead of being dropped.
    """
    bad = [part for part in _allow_parts(raw) if node_id(part) is None]
    if not bad:
        return None
    shown = ", ".join(bad[:5])
    return (
        f"MESHTASTIC_ALLOWED_NODES has unreadable ids ({shown}). "
        "Nothing is connected. Use !aabbccdd or a decimal node number, or leave the list empty."
    )


def parse_allowlist(raw: object) -> frozenset[str]:
    if allowlist_problem(raw):
        return frozenset()
    return frozenset(node for node in (node_id(part) for part in _allow_parts(raw)) if node)


def is_allowed(node: object, allowlist: frozenset[str]) -> bool:
    if not allowlist:
        return False
    canonical = node_id(node)
    return canonical is not None and canonical in allowlist


def is_broadcast(value: object) -> bool:
    if isinstance(value, bool):
        return False
    if isinstance(value, int):
        return (value & 0xFFFFFFFF) == BROADCAST
    text = str(value or "").strip().lower()
    return text in {"broadcast", "^all", "all", "!ffffffff", "0xffffffff", "4294967295"}


def _text_from(decoded: dict) -> str | None:
    if "text" in decoded and isinstance(decoded["text"], str):
        text = decoded["text"]
    else:
        payload = decoded.get("payload")
        if isinstance(payload, str):
            text = payload
        elif isinstance(payload, (bytes, bytearray)):
            try:
                text = bytes(payload).decode("utf-8")
            except UnicodeError:
                return None
        else:
            return None
    text = text.replace("\x00", "").strip()
    return text or None


def _packet_field(packet: dict, primary: str, fallback: str):
    """The first field that is present and not None.

    dict.get(primary, packet.get(fallback)) stays None when the key exists and its value is None.
    The library sets fromId to None when that node is missing from its node database, and leaves the number in `from`.
    """
    if primary in packet and packet[primary] is not None:
        return packet[primary]
    if fallback in packet and packet[fallback] is not None:
        return packet[fallback]
    return None


def classify(packet: dict, allowlist: frozenset[str], *, my_node: object = None) -> dict | None:
    """A direct text this gateway may hand to Hermes, or None to drop it.

    Dropped packets are not replies and are not recorded as questions.
    """
    if not isinstance(packet, dict):
        return None
    decoded = packet.get("decoded")
    if not isinstance(decoded, dict):
        return None
    port = decoded.get("portnum")
    if port not in {"TEXT_MESSAGE_APP", 1, "1"}:
        return None
    sender = node_id(_packet_field(packet, "fromId", "from"))
    if sender is None:
        return None
    me = node_id(my_node) if my_node is not None else None
    if me is None or sender == me:
        return None
    destination = _packet_field(packet, "toId", "to")
    # A channel key lets anyone impersonate a sender. Channel packets are never answered.
    if is_broadcast(destination):
        return None
    if node_id(destination) != me:
        return None
    if packet.get("pkiEncrypted") is not True:
        return None
    if not is_allowed(sender, allowlist):
        return None
    text = _text_from(decoded)
    if text is None:
        return None
    packet_id = packet.get("id")
    return {
        "node": sender,
        "text": text,
        "channel": False,
        "packet_id": None if packet_id is None else str(packet_id),
    }


def chunk_text(text: str, size: int, limit: int) -> tuple[list[str], bool]:
    """UTF-8 chunks that each fit in `size` bytes. The bool is True when text was cut."""
    size = chunk_bytes(size)
    limit = max_chunks(limit)
    raw = str(text or "")
    try:
        raw.encode("utf-8")
    except UnicodeError:
        # A lone surrogate has no UTF-8 form. Replacing it keeps a reply from
        # raising out of the send path. An approval line that holds one is
        # refused by radio_text_is_one_line before it reaches this function.
        raw = raw.encode("utf-8", "replace").decode("utf-8")
    chunks: list[str] = []
    buf = ""
    used = 0
    index = 0
    while index < len(raw):
        char = raw[index]
        encoded = char.encode("utf-8")
        if len(encoded) > size:
            break
        if used + len(encoded) > size:
            chunks.append(buf)
            buf = ""
            used = 0
            if len(chunks) >= limit:
                break
            continue
        buf += char
        used += len(encoded)
        index += 1
    else:
        if buf and len(chunks) < limit:
            chunks.append(buf)
        return [part for part in chunks if part], False
    cut = True
    if buf and len(chunks) < limit:
        chunks.append(buf)
    if not chunks:
        return [], True
    mark = CUT_MARK
    mark_len = len(mark.encode("utf-8"))
    if mark_len >= size:
        return chunks[:limit], True
    last = chunks[-1]
    while last and len(last.encode("utf-8")) + mark_len > size:
        last = last[:-1]
    chunks[-1] = (last + mark) if last else mark
    if len(chunks[-1].encode("utf-8")) > size:
        chunks[-1] = chunks[-1][:0]
        return [part for part in chunks if part], True
    return chunks[:limit], cut


class SendBudget:
    """Rolling gap and hourly cap. The caller records a send only after the radio accepts it."""

    def __init__(self, gap_seconds: int, per_hour: int) -> None:
        self.gap = min_gap_seconds(gap_seconds)
        self.per_hour = max_per_hour(per_hour)
        self.sent_at: list[float] = []

    def plan(self, count: int, now: float) -> tuple[str | None, float]:
        """A refusal and a wait. A gap is a wait, not a claim that nothing went out.

        The hourly cap refuses before this call touches the radio. The message
        says earlier sends this hour may already be on the air.
        """
        if count < 1:
            return "Nothing to send.", 0.0
        if count > self.per_hour:
            return (
                f"This reply needs {count} radio sends, above the hourly cap of {self.per_hour}. "
                "This call sends nothing. Earlier sends this hour may already be on the air.",
                0.0,
            )
        fresh = [stamp for stamp in self.sent_at if now - stamp < HOUR_SECONDS]
        self.sent_at = fresh
        if len(fresh) + count > self.per_hour:
            return (
                f"Sending {count} more would pass the hourly cap of {self.per_hour}. "
                "This call sends nothing. Earlier sends this hour may already be on the air.",
                0.0,
            )
        wait = 0.0
        if fresh:
            wait = max(0.0, self.gap - (now - fresh[-1]))
        return None, wait

    def mark(self, now: float) -> None:
        self.sent_at.append(now)


def parse_url(raw: object) -> dict:
    """One device URL. tcp://host:port, serial:///dev/..., or ble://name. No userinfo."""
    text = str(raw or "").strip()
    if not text or any(c.isspace() for c in text):
        raise ValueError("MESHTASTIC_URL must be one tcp://, serial://, or ble:// URL with no spaces.")
    parsed = urlparse(text)
    scheme = (parsed.scheme or "").lower()
    if parsed.username or parsed.password:
        raise ValueError("MESHTASTIC_URL must not contain userinfo.")
    if parsed.query or parsed.fragment:
        raise ValueError("MESHTASTIC_URL must not contain a query or fragment.")
    if scheme == "tcp":
        host = parsed.hostname
        if not host or parsed.path not in {"", "/"}:
            raise ValueError("tcp:// URL must be host and optional port only, for example tcp://radio.example:4403.")
        try:
            port = parsed.port
        except ValueError:
            port = -1
        if port is None:
            port = 4403
        if not 1 <= port <= 65535:
            raise ValueError("tcp port must be a number from 1 to 65535, for example tcp://radio.example:4403.")
        return {"kind": "tcp", "host": host, "port": port}
    if scheme == "serial":
        path = parsed.path or ""
        if not path.startswith("/") or ".." in path.split("/"):
            raise ValueError("serial:// URL must be an absolute device path with no '..'.")
        return {"kind": "serial", "path": path}
    if scheme == "ble":
        name = parsed.netloc
        if not name or parsed.path or "/" in name:
            raise ValueError("ble:// URL must be a single device name or address, for example ble://radio.example.")
        return {"kind": "ble", "name": name}
    raise ValueError("MESHTASTIC_URL must start with tcp://, serial://, or ble://.")


# Hermes keeps these approvals past the one command: "session" for the session, "always" in command_allowlist.
APPROVAL_SCOPE_WORDS = frozenset({"always", "permanent", "permanently", "session", "ses"})
WIDE_APPROVAL_PHRASES = frozenset({
    "always", "session", "remember",
    "approve always", "always approve", "approve session", "session approve",
})
_ATTACHMENT_REFS = re.compile(r"^(?:@(?:image|file|url):[^\n]+\n?)+", re.IGNORECASE)


# One radio chunk at the 64-byte floor. A longer fixed line is split, or dropped when it must fit one chunk.
APPROVAL_SCOPE_REFUSAL = "Once only. always and session are refused."
APPROVAL_WORD_REFUSAL = "Add more words. always and session are refused."
COMMAND_REFUSAL = "That command is refused on the radio."
BUSY_RESET_REFUSAL = "Turn still running. /new and /reset refused. Session not reset."
CONFIRM_CUT_NOTE = "Confirmation does not fit. Command not run."


def slash_confirm_line(title: object, message: object = "") -> str:
    """One radio line for a /new or /undo confirmation.

    /new and its alias /reset discard the conversation. /undo drops one exchange,
    or the count Hermes wrote in the prompt. always is not offered.
    """
    name = " ".join(str(title or "").split())
    if name in {"/new", "/reset"}:
        effect = "discards history"
    elif name == "/undo":
        # The integer that names the turns, not a date elsewhere in the prompt.
        named = re.findall(r"(\d+)\s+(?:user\s+)?turns?\b", str(message or ""), flags=re.IGNORECASE)
        count = int(named[-1]) if named else 1
        if count < 2:
            count = 1
        effect = "drops last exchange" if count == 1 else f"drops {count} turns"
    else:
        effect = "changes this session"
    return f"{name} {effect}. /approve or /cancel. always refused."


def short_unscoped_reply(text: object) -> bool:
    """True when the text is short enough to be one always or session word.

    Used only when Hermes' own word list cannot be read. A longer reply cannot be
    exactly one of those words, so it is not refused for this reason.
    """
    body = " ".join(str(text or "").split())
    return bool(body) and len(body) <= 40


def widens_approval(text: object, extra_phrases: object = ()) -> bool:
    """True when the text would approve for the session or permanently, not just once.

    Covers /approve with session or always (and their aliases), /always and /remember,
    and a message that is only one of Hermes' session or always words.
    """
    body = _ATTACHMENT_REFS.sub("", str(text or "").lstrip())
    body = " ".join(body.lower().split())
    if not body:
        return False
    phrases = set(WIDE_APPROVAL_PHRASES)
    for phrase in extra_phrases or ():
        phrases.add(" ".join(str(phrase).lower().split()))
    if body in phrases or body.lstrip("!/").strip() in phrases:
        return True
    if body[0] not in "/!":
        return False
    words = body[1:].split()
    if not words:
        return False
    head = words[0].split("@", 1)[0]
    if head in {"always", "remember"}:
        return True
    return head == "approve" and any(word in APPROVAL_SCOPE_WORDS for word in words[1:])


# Slash commands a radio node may send. Each exists in Hermes v0.21.4 and main.
# The rest are refused before Hermes sees them: /yolo skips approval for the session,
# /approvals changes approval for the whole profile.
# /cancel is Hermes' own word for leaving a /new, /reset, or /undo confirmation unchanged.
RADIO_COMMANDS = ("approve", "deny", "cancel", "stop", "new", "reset", "help", "status", "whoami", "retry", "undo")


def radio_command_refusal(text: object) -> str | None:
    """None when the text is not a command or is a radio command. Otherwise "approve" or "command".

    A text starting with / or ! is a command. The name is lower-cased and loses any @bot.
    /approve passes with no argument or with once only. A /path with a second / is not a command.
    """
    body = _ATTACHMENT_REFS.sub("", str(text or "").lstrip()).lstrip()
    if not body or body[0] not in "/!":
        return None
    words = body[1:].split()
    if not words:
        return None
    name = words[0].lower().split("@", 1)[0]
    if body[0] == "/" and "/" in name:
        return None
    if name == "approve":
        if all(word.lower() == "once" for word in words[1:]):
            return None
        return "approve"
    if name in RADIO_COMMANDS:
        return None
    return "command"


def session_reset_command(text: object) -> bool:
    """True for /new and /reset, including a ! prefix and an @bot suffix.

    Hermes treats /reset as another name for /new. While a turn is running it
    resets the session without asking, so the adapter refuses both names then.
    """
    body = _ATTACHMENT_REFS.sub("", str(text or "").lstrip()).lstrip()
    if not body or body[0] not in "/!":
        return False
    words = body[1:].split()
    if not words:
        return False
    name = words[0].lower().split("@", 1)[0]
    if body[0] == "/" and "/" in name:
        return False
    return name in {"new", "reset"}


APPROVAL_PREFIX = "Reply /approve or /deny (once only). "
# Cc control, Cf format (includes U+202E), Zl/Zp line and paragraph separators
# (includes U+2028), Cs surrogate, Co private use, Cn unassigned.
_RADIO_BREAK_CATEGORIES = frozenset({"Cc", "Cf", "Zl", "Zp", "Cs", "Co", "Cn"})


def radio_text_is_one_line(text: object) -> bool:
    """True when text is a single visible line the radio can show unchanged.

    `splitlines` catches Unicode line breaks that are not `\\n` or `\\r`.
    A character in Cc, Cf, Zl, or Zp can still sit inside one split line
    (a trailing break, or U+202E) and is refused too. A surrogate (Cs) has no
    UTF-8 form, and a private-use (Co) or unassigned (Cn) code point has no
    agreed glyph, so those are refused as well. A string that cannot be encoded
    as UTF-8 at all is refused last, whatever its categories say. A backtick
    fence is refused.
    """
    if not isinstance(text, str) or text == "":
        return False
    if "```" in text or len(text.splitlines()) != 1:
        return False
    if any(unicodedata.category(ch) in _RADIO_BREAK_CATEGORIES for ch in text):
        return False
    try:
        text.encode("utf-8")
    except UnicodeError:
        return False
    return True


def radio_approval_line(command: object, description: object, chunk: int) -> str | None:
    """One radio line from the structured command and description, or None.

    None means do not transmit. The command inside a returned line is exactly
    `command`. The caller refuses a masked command, and a command that is not
    the one `/approve` runs, before it asks for this line. A line break, a
    format character, a backtick fence, or a line that would have to be cut
    returns None.
    The long prompt text is not read.
    """
    if not isinstance(command, str) or not isinstance(description, str) or command == "":
        return None
    if not radio_text_is_one_line(command):
        return None
    if description and not radio_text_is_one_line(description):
        return None
    line = f"{APPROVAL_PREFIX}Run: {command}"
    if description:
        line = f"{line} — {description}"
    parts, cut = chunk_text(line, chunk, 1)
    if cut or parts != [line]:
        return None
    return line


# Hermes rewrites these plain-text phrases to /restart before it reads the command
# (gateway.platforms.base, v0.21.4 and main). Used when that function cannot be imported.
_PLAINTEXT_RESTART = (
    re.compile(r"^(?:please\s+)?restart\s+(?:the\s+)?gateway[.!?\s]*$", re.IGNORECASE),
    re.compile(r"^(?:please\s+)?restart\s+(?:the\s+)?hermes\s+gateway[.!?\s]*$", re.IGNORECASE),
    re.compile(r"^(?:please\s+)?restart\s+hermes[.!?\s]*$", re.IGNORECASE),
)


def plaintext_restart(text: object) -> bool:
    """True when Hermes would turn this plain text into /restart."""
    body = str(text or "").strip()
    return bool(body) and not body.startswith("/") and any(p.match(body) for p in _PLAINTEXT_RESTART)
