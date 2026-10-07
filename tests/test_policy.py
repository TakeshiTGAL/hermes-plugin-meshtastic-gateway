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
    parts, cut = chunk_text("a" * 10, 4, 2)
    assert cut is True
    assert all(len(part.encode("utf-8")) <= 4 for part in parts)
    assert len(parts) <= 2
    snow = "❄"  # 3 bytes
    parts, cut = chunk_text(snow * 3, 4, 8)
    assert cut is False
    assert all(len(part.encode("utf-8")) <= 4 for part in parts)


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
