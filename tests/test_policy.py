from __future__ import annotations

import policy
from policy import (
    SendBudget,
    chunk_text,
    classify,
    is_allowed,
    node_id,
    parse_allowlist,
    parse_url,
)


ALLOW = parse_allowlist("!aabbccdd")
SENDER = {
    "fromId": "!aabbccdd",
    "toId": "!11223344",
    "id": 7,
    "pkiEncrypted": True,
    "decoded": {"portnum": "TEXT_MESSAGE_APP", "text": "hello"},
}


def test_empty_allowlist_answers_nobody():
    assert parse_allowlist("") == frozenset()
    assert parse_allowlist(None) == frozenset()
    assert is_allowed("!aabbccdd", frozenset()) is False
    assert classify(SENDER, frozenset()) is None


def test_broadcast_and_stranger_are_dropped():
    channel = dict(SENDER, toId="^all")
    assert classify(channel, ALLOW) is None
    stranger = dict(SENDER, fromId="!01020304")
    assert classify(stranger, ALLOW) is None
    assert classify(SENDER, ALLOW, my_node="!11223344")["node"] == "!aabbccdd"
    assert classify(SENDER, ALLOW, my_node="!11223344")["text"] == "hello"
    assert classify(SENDER, ALLOW) is None
    assert classify(SENDER, ALLOW, my_node="!aabbccdd") is None


def test_channel_packets_are_never_answered():
    channel = dict(SENDER, toId="4294967295")
    assert classify(channel, ALLOW, my_node="!11223344") is None
    unsigned = dict(SENDER)
    unsigned.pop("pkiEncrypted")
    assert classify(unsigned, ALLOW, my_node="!11223344") is None
    assert classify(dict(SENDER, pkiEncrypted=False), ALLOW, my_node="!11223344") is None


def test_node_id_hex_bang_is_not_decimal():
    assert node_id("!00000010") == "!00000010"
    assert node_id(16) == "!00000010"
    assert node_id("!ffffffff") is None
    assert node_id(0) is None
    assert "!ffffffff" not in parse_allowlist("!ffffffff, !aabbccdd")


def test_own_node_and_non_text_are_dropped():
    assert classify(SENDER, ALLOW, my_node="!aabbccdd") is None
    other = dict(SENDER)
    other["decoded"] = {"portnum": "POSITION_APP", "text": "hello"}
    assert classify(other, ALLOW) is None


def test_chunks_are_utf8_bytes_and_cut():
    parts, cut = chunk_text("a" * 200, 64, 2)
    assert cut is True
    assert all(len(part.encode("utf-8")) <= 64 for part in parts)
    assert len(parts) <= 2
    assert parts[-1].endswith(" [cut]")
    snow = "❄"  # 3 bytes
    parts, cut = chunk_text(snow * 30, 64, 8)
    assert cut is False
    assert all(len(part.encode("utf-8")) <= 64 for part in parts)


def test_chunk_size_floor_fits_the_approval_prefix():
    assert policy.chunk_bytes(1) == 64
    assert policy.chunk_bytes("0") == 64
    prefix = (policy.APPROVAL_PREFIX + "Run: ").encode("utf-8")
    assert len(prefix) < policy.CHUNK_BYTES_FLOOR


def test_url_pins_one_device():
    assert parse_url("tcp://radio.example:4403") == {"kind": "tcp", "host": "radio.example", "port": 4403}
    assert parse_url("tcp://radio.example")["port"] == 4403
    try:
        parse_url("tcp://user:pass@radio.example:4403")
    except ValueError as exc:
        assert "userinfo" in str(exc)
    else:
        raise AssertionError("userinfo was accepted")
    try:
        parse_url("http://radio.example:4403")
    except ValueError:
        pass
    else:
        raise AssertionError("http was accepted")
    assert parse_url("serial:///dev/ttyUSB0")["path"] == "/dev/ttyUSB0"
    try:
        parse_url("serial:///tmp/../etc/passwd")
    except ValueError:
        pass
    else:
        raise AssertionError("parent path was accepted")


def test_budget_waits_out_the_gap_and_refuses_over_hour():
    budget = SendBudget(1, 100)
    assert budget.gap == 10
    assert budget.per_hour == 30
    budget.mark(0)
    refusal, wait = budget.plan(1, 5)
    assert refusal is None
    assert wait == 5
    refusal, wait = budget.plan(1, 10)
    assert refusal is None and wait == 0
    tight = SendBudget(10, 2)
    tight.mark(0)
    tight.mark(10)
    refusal, _wait = tight.plan(1, 20)
    assert refusal is not None
    assert "already be on the air" in refusal


def test_unreadable_allowlist_is_not_silently_empty():
    assert policy.allowlist_problem("") is None
    assert policy.allowlist_problem("not-a-node") is not None
    assert policy.allowlist_problem("!aabbccdd, not-a-node") is not None
    assert parse_allowlist("!aabbccdd, not-a-node") == frozenset()


def test_settings_ceilings():
    assert policy.chunk_bytes(10_000) == 200
    assert policy.max_chunks(0) == 1
    assert policy.min_gap_seconds("1") == 10
    assert policy.max_per_hour("nope") == 12


def test_dm_addressed_to_someone_else_is_dropped():
    foreign = dict(SENDER, toId="!01020304")
    assert classify(foreign, ALLOW, my_node="!11223344") is None


def test_remember_replaces_a_temp_file(tmp_path, monkeypatch):
    import json
    import store

    monkeypatch.setattr(store, "data_dir", lambda: tmp_path)
    store.remember("!aabbccdd", "in", 4, 1.0)
    path = tmp_path / "nodes.json"
    assert path.is_file()
    assert not (tmp_path / "nodes.json.tmp").exists()
    assert json.loads(path.read_text(encoding="utf-8"))[0]["node"] == "!aabbccdd"
    path.write_text("{", encoding="utf-8")
    store.remember("!aabbccdd", "out", 1, 2.0)
    assert path.read_text(encoding="utf-8") == "{"


def test_serial_and_ble_are_refused_before_the_library():
    from radio import open_interface

    for spec in ({"kind": "serial", "path": "/dev/ttyUSB0"}, {"kind": "ble", "name": "radio"}):
        try:
            open_interface(spec)
        except ValueError as exc:
            assert "tcp only" in str(exc)
            assert "Nothing is connected" in str(exc)
        else:
            raise AssertionError(spec)


def test_session_and_always_approvals_are_recognized():
    for text in (
        "/approve always", "/approve all always", "/APPROVE  session", "/approve permanent",
        "/approve ses", "/always", "/remember", "!remember", "always", "Session",
        "approve always", "always approve", "session approve", "@file:x\n/approve always",
    ):
        assert policy.widens_approval(text) is True, text
    for text in ("/approve", "/approve all", "/deny", "yes", "always reply in English", ""):
        assert policy.widens_approval(text) is False, text
    assert policy.widens_approval("zawsze", ("Zawsze",)) is True


def test_tcp_port_zero_or_out_of_range_is_refused():
    for url in ("tcp://radio.example:0", "tcp://radio.example:65536", "tcp://radio.example:99999"):
        try:
            parse_url(url)
        except ValueError as exc:
            assert "1 to 65535" in str(exc)
        else:
            raise AssertionError(f"{url} was accepted")
    assert parse_url("tcp://radio.example:1")["port"] == 1
    assert parse_url("tcp://radio.example:65535")["port"] == 65535


def test_radio_commands_are_an_allowlist():
    for text in ("/yolo", "/approvals off", "/APPROVALS OFF", "!yolo", "/yolo@bot", "/restart", "/model x"):
        assert policy.radio_command_refusal(text) == "command", text
    for text in ("/approve always", "/approve session", "/approve all"):
        assert policy.radio_command_refusal(text) == "approve", text
    for text in ("/approve", "/approve once", "/deny", "/cancel", "/stop", "/new", "/reset", "/help", "/STATUS@bot",
                 "/whoami", "/retry", "/undo", "hello", "/home/me/file is broken", ""):
        assert policy.radio_command_refusal(text) is None, text
    assert policy.slash_confirm_line("/new", "discards the current conversation history") == (
        "/new discards history. /approve or /cancel. always refused."
    )
    assert policy.slash_confirm_line("/undo", "the last user/assistant exchange") == (
        "/undo drops last exchange. /approve or /cancel. always refused."
    )
    assert policy.slash_confirm_line("/undo", "the last 3 user turns") == (
        "/undo drops 3 turns. /approve or /cancel. always refused."
    )
    assert policy.slash_confirm_line("/undo", "session 20261008 removes the last 2 turns") == (
        "/undo drops 2 turns. /approve or /cancel. always refused."
    )
    assert policy.session_reset_command("/new") is True
    assert policy.session_reset_command("/reset name") is True
    assert policy.session_reset_command("!new@bot") is True
    assert policy.session_reset_command("/undo") is False
    assert policy.session_reset_command("hello") is False
    assert len(policy.BUSY_RESET_REFUSAL.encode("utf-8")) <= 64
    assert policy.short_unscoped_reply("hello-from-radio") is True
    assert policy.short_unscoped_reply("x" * 41) is False
    assert policy.short_unscoped_reply("") is False


def test_fixed_refusal_and_confirm_lines_fit_one_floor_chunk():
    lines = [
        policy.APPROVAL_SCOPE_REFUSAL,
        policy.APPROVAL_WORD_REFUSAL,
        policy.COMMAND_REFUSAL,
        policy.BUSY_RESET_REFUSAL,
        policy.CONFIRM_CUT_NOTE,
        policy.slash_confirm_line("/new", "discards the current conversation history"),
        policy.slash_confirm_line("/reset", ""),
        policy.slash_confirm_line("/undo", "the last user/assistant exchange"),
        policy.slash_confirm_line("/undo", "the last 3 user turns"),
        policy.slash_confirm_line("/undo", "session 20261008 removes the last 2 turns"),
        policy.slash_confirm_line("/undo", "the last 12 user turns"),
    ]
    assert len(lines) == len(set(lines))
    for line in lines:
        size = len(line.encode("utf-8"))
        assert size <= 64, (size, line)


def test_approval_question_is_made_short():
    question = "Heading\n```\nls -la\n```\nWhy it was flagged: test\n\nReply `/approve` ..."
    assert policy.radio_approval_text(question) == "Reply /approve or /deny (once only). Run: ls -la — test"
    assert policy.radio_approval_text("Approve?") == "Reply /approve or /deny (once only). Approve?"


def test_plain_text_restart_phrases_are_read_as_restart():
    for text in ("restart gateway", "please restart hermes", "Restart the Hermes gateway!"):
        assert policy.plaintext_restart(text) is True, text
    for text in ("restart the router", "can you restart gateway later", "/restart", ""):
        assert policy.plaintext_restart(text) is False, text


def test_node_id_rejects_values_outside_32_bits():
    assert node_id("!1aabbccdd") is None
    assert node_id("!1aabbccdd") != "!aabbccdd"
    assert node_id(0x1AABBCCDD) is None
    assert node_id(0x100000001) is None
    assert node_id(-5) is None
    assert node_id(-5) != "!fffffffb"
    assert node_id("!-5") is None
    assert node_id(-1) is None
    assert node_id(0x100000000) is None
    assert node_id("!aabbccdd") == "!aabbccdd"
    assert node_id("!000000010") == "!00000010"
    assert node_id("!000000001") == "!00000001"
    assert parse_allowlist("!1aabbccdd") == frozenset()
    assert parse_allowlist("!1aabbccdd, !aabbccdd") == frozenset()


def test_missing_from_id_uses_the_number():
    missing = dict(SENDER)
    missing["fromId"] = None
    missing["from"] = 0xAABBCCDD
    got = classify(missing, ALLOW, my_node="!11223344")
    assert got is not None and got["node"] == "!aabbccdd"

    bang = dict(SENDER)
    bang["fromId"] = None
    bang["from"] = "!aabbccdd"
    assert classify(bang, ALLOW, my_node="!11223344")["node"] == "!aabbccdd"

    dropped = dict(SENDER)
    dropped["fromId"] = None
    dropped.pop("from", None)
    assert classify(dropped, ALLOW, my_node="!11223344") is None

    to_number = dict(SENDER)
    to_number["toId"] = None
    to_number["to"] = 0x11223344
    assert classify(to_number, ALLOW, my_node="!11223344")["node"] == "!aabbccdd"

    to_gone = dict(SENDER)
    to_gone["toId"] = None
    to_gone.pop("to", None)
    assert classify(to_gone, ALLOW, my_node="!11223344") is None
