"""Construct the real adapter through Hermes and drive one fake-radio reply.

Skipped when this pytest process cannot import Hermes. Preship runs the
plugin tests without the Hermes tree on PYTHONPATH.
"""
from __future__ import annotations

import asyncio
import threading

import pytest

pytest.importorskip("gateway.platform_registry")

from gateway.config import PlatformConfig
from gateway.platform_registry import PlatformEntry, platform_registry

import adapter


def _config() -> PlatformConfig:
    return PlatformConfig(
        enabled=True,
        extra={
            "MESHTASTIC_URL": "tcp://radio.example:4403",
            "MESHTASTIC_ALLOWED_NODES": "!aabbccdd",
            "MESHTASTIC_MIN_GAP_SECONDS": "10",
        },
    )


def _register() -> None:
    platform_registry.register(
        PlatformEntry(
            name="meshtastic-gateway",
            label="Meshtastic",
            adapter_factory=lambda cfg: adapter.MeshtasticAdapter(cfg),
            check_fn=lambda: True,
            validate_config=adapter.validate_config,
            required_env=["MESHTASTIC_URL"],
            source="plugin",
        )
    )


class _Iface:
    def __init__(self):
        self.sent = []

    def sendText(self, text, destinationId, wantAck=False):
        self.sent.append((destinationId, text, wantAck))
        return {"id": len(self.sent)}


def test_create_adapter_then_handle_message_sends_pong():
    _register()
    created = platform_registry.create_adapter("meshtastic-gateway", _config())
    assert created is not None
    assert type(created) is adapter.MeshtasticAdapter

    async def run():
        info = await created.get_chat_info("!aabbccdd")
        assert info["name"] == "!aabbccdd"
        assert info["type"] == "dm"
        iface = _Iface()
        created._iface = iface

        async def handler(_event):
            child_box = {}

            async def child():
                child_box["result"] = await created.send("!aabbccdd", "from-child")

            await asyncio.create_task(child())
            approval = await asyncio.create_task(created.send(
                "!aabbccdd",
                "Approve this?",
                metadata={"is_approval_prompt": True},
            ))
            child_box["approval"] = approval.success
            assert child_box["result"].success is False
            assert "Unsolicited" in (child_box["result"].error or "")
            return "pong"

        created.set_message_handler(handler)
        await created._accept({"node": "!aabbccdd", "text": "hello", "channel": False, "packet_id": "7"})
        pending = [task for task in list(created._background_tasks) if hasattr(task, "__await__")]
        if pending:
            await asyncio.wait_for(asyncio.gather(*pending), timeout=15)
        assert [text for _dest, text, _ack in iface.sent] == ["Approve this?", "pong"]

        refused = await created.send("!aabbccdd", "later")
        assert refused.success is False
        assert "Unsolicited" in (refused.error or "")
        assert [text for _dest, text, _ack in iface.sent] == ["Approve this?", "pong"]

        created.config.extra["MESHTASTIC_CHUNK_BYTES"] = "8"
        created.config.extra["MESHTASTIC_MAX_CHUNKS"] = "1"
        token = adapter._reply_node.set("!aabbccdd")
        created._send_tasks.add(asyncio.current_task())
        try:
            original = adapter.asyncio.sleep

            async def no_wait(_seconds):
                return None

            adapter.asyncio.sleep = no_wait
            try:
                cut = await created.send("!aabbccdd", "abcdefghij")
            finally:
                adapter.asyncio.sleep = original
        finally:
            created._send_tasks.discard(asyncio.current_task())
            adapter._reply_node.reset(token)
        assert cut.success is True
        assert "was cut" in (cut.error or "")
        assert any(text.endswith(" [cut]") for _dest, text, _ack in iface.sent)

    asyncio.run(run())


def test_open_interface_runs_off_the_event_loop(monkeypatch):
    seen = {}

    def fake_open(_spec):
        seen["thread"] = threading.current_thread()
        return object()

    monkeypatch.setattr(adapter, "open_interface", fake_open)
    monkeypatch.setattr(adapter, "subscribe", lambda _iface, _callback: (lambda: None))
    monkeypatch.setattr(adapter, "subscribe_lost", lambda _iface, _callback: (lambda: None))
    monkeypatch.setattr(adapter, "local_node", lambda _iface: "!11223344")
    monkeypatch.setattr(adapter, "close_interface", lambda _iface: None)
    _register()
    created = platform_registry.create_adapter("meshtastic-gateway", _config())

    async def run():
        assert await created.connect() is True

    asyncio.run(run())
    assert seen["thread"] is not threading.current_thread()


def test_stopped_loop_does_not_consume_the_packet_id(monkeypatch):
    monkeypatch.setattr(adapter, "local_node", lambda _iface: "!11223344")
    _register()
    created = platform_registry.create_adapter("meshtastic-gateway", _config())
    created._iface = object()
    created._loop = None
    created._on_packet({
        "fromId": "!aabbccdd",
        "toId": "!11223344",
        "id": 7,
        "pkiEncrypted": True,
        "decoded": {"portnum": "TEXT_MESSAGE_APP", "text": "hello"},
    })
    assert created._seen == []


def test_failed_handoff_forgets_the_packet_id(monkeypatch):
    recorded = []
    monkeypatch.setattr(adapter, "remember", lambda *args: recorded.append(args))
    _register()
    created = platform_registry.create_adapter("meshtastic-gateway", _config())
    created._seen = ["7"]

    def broken_source(**_kwargs):
        raise RuntimeError("cannot build")

    created.build_source = broken_source

    async def run():
        await created._accept({"node": "!aabbccdd", "text": "hello", "channel": False, "packet_id": "7"})

    asyncio.run(run())
    assert created._seen == []
    assert recorded == []


def test_handle_message_without_radio_does_not_record(monkeypatch):
    recorded = []
    monkeypatch.setattr(adapter, "remember", lambda *args: recorded.append(args))
    _register()
    created = platform_registry.create_adapter("meshtastic-gateway", _config())
    source = created.build_source(
        chat_id="!aabbccdd",
        chat_type="dm",
        user_id="!aabbccdd",
        user_name="!aabbccdd",
    )
    from gateway.platforms.event import MessageEvent, MessageType

    event = MessageEvent(text="hello", message_type=MessageType.TEXT, source=source)

    async def run():
        try:
            await created.handle_message(event)
        except Exception:
            return

    asyncio.run(run())
    assert recorded == []


def test_connection_lost_stops_the_adapter_and_asks_hermes_to_reconnect():
    _register()
    created = platform_registry.create_adapter("meshtastic-gateway", _config())
    notes = []

    async def notify():
        notes.append("notified")

    async def run():
        created._loop = asyncio.get_running_loop()
        created._running = True
        created._notify_fatal_error = notify
        created._on_connection_lost()
        await asyncio.sleep(0)
        created._on_connection_lost()

    asyncio.run(run())
    assert created._running is False
    assert notes == ["notified"]


def test_decimal_and_uppercase_allowlist_entries_are_canonical():
    _register()
    cfg = _config()
    cfg.extra["MESHTASTIC_ALLOWED_NODES"] = "2864434397, !AABBCCDD"
    created = platform_registry.create_adapter("meshtastic-gateway", cfg)
    assert created.resolved_allowlist_user_ids() == {"!aabbccdd"}


def test_a_chunk_already_on_the_radio_is_final(monkeypatch):
    calls = {"n": 0}

    def send_text(_iface, _dest, _chunk):
        calls["n"] += 1
        if calls["n"] == 1:
            return "pkt-1"
        raise ConnectionError("down")

    monkeypatch.setattr(adapter, "send_text", send_text)
    monkeypatch.setattr(adapter, "remember", lambda *_args: None)
    _register()
    created = platform_registry.create_adapter("meshtastic-gateway", _config())
    created._iface = object()
    created.config.extra["MESHTASTIC_CHUNK_BYTES"] = "4"
    created.config.extra["MESHTASTIC_MAX_CHUNKS"] = "4"

    async def run():
        token = adapter._reply_node.set("!aabbccdd")
        created._send_tasks.add(asyncio.current_task())
        original = adapter.asyncio.sleep

        async def no_wait(_seconds):
            return None

        adapter.asyncio.sleep = no_wait
        try:
            return await created.send("!aabbccdd", "abcdefghij")
        finally:
            adapter.asyncio.sleep = original
            created._send_tasks.discard(asyncio.current_task())
            adapter._reply_node.reset(token)

    result = asyncio.run(run())
    assert result.success is False
    assert result.raw_response == {"chunks_sent": 1}
    assert created._send_retry_is_final(result) is True
    assert "not sent again" in (result.error or "")


def test_a_reply_that_never_reaches_the_radio_can_be_accepted_again(monkeypatch):
    def send_text(*_args):
        raise ConnectionError("down")

    monkeypatch.setattr(adapter, "send_text", send_text)
    monkeypatch.setattr(adapter, "remember", lambda *_args: None)
    _register()
    created = platform_registry.create_adapter("meshtastic-gateway", _config())
    created._iface = object()
    created._seen = ["9"]

    async def handler(_event):
        return "pong"

    async def run():
        created.set_message_handler(handler)
        await created._accept({"node": "!aabbccdd", "text": "hello", "channel": False, "packet_id": "9"})
        pending = [task for task in list(created._background_tasks) if hasattr(task, "__await__")]
        if pending:
            await asyncio.wait_for(asyncio.gather(*pending), timeout=15)

    asyncio.run(run())
    assert created._seen == []


class _NoWaitBudget:
    gap = 0

    def plan(self, _chunks, _now):
        return None, 0

    def mark(self, _now):
        return None


async def _finish_turns(created):
    for _ in range(50):
        pending = [task for task in list(created._background_tasks) if hasattr(task, "__await__")]
        if not pending:
            return
        await asyncio.wait_for(asyncio.gather(*pending), timeout=15)


def test_two_nodes_talking_at_once_both_get_their_approval_question(monkeypatch):
    monkeypatch.setattr(adapter, "remember", lambda *_args: None)
    _register()
    cfg = _config()
    cfg.extra["MESHTASTIC_ALLOWED_NODES"] = "!aabbccdd,!11223344"
    created = platform_registry.create_adapter("meshtastic-gateway", cfg)
    iface = _Iface()
    created._iface = iface
    created._budget = _NoWaitBudget()
    results = {}

    async def run():
        second_asked = asyncio.Event()

        async def ask(node):
            # A nested task, as Hermes uses for the approval question.
            return await asyncio.create_task(created.send(
                node, f"Approve for {node}?", metadata={"is_approval_prompt": True}))

        async def handler(event):
            node = event.source.chat_id
            if node == "!aabbccdd":
                await asyncio.wait_for(second_asked.wait(), timeout=10)
                results[node] = await ask(node)
            else:
                results[node] = await ask(node)
                second_asked.set()
            return f"final {node}"

        created.set_message_handler(handler)
        await created._accept({"node": "!aabbccdd", "text": "first", "channel": False, "packet_id": "21"})
        await created._accept({"node": "!11223344", "text": "second", "channel": False, "packet_id": "22"})
        await _finish_turns(created)

    asyncio.run(run())
    assert results["!aabbccdd"].success is True
    assert results["!11223344"].success is True
    assert sorted(text for _dest, text, _ack in iface.sent) == sorted([
        "Approve for !aabbccdd?",
        "Approve for !11223344?",
        "final !aabbccdd",
        "final !11223344",
    ])


def test_a_task_made_in_the_turn_cannot_send_after_the_turn(monkeypatch):
    monkeypatch.setattr(adapter, "remember", lambda *_args: None)
    _register()
    created = platform_registry.create_adapter("meshtastic-gateway", _config())
    iface = _Iface()
    created._iface = iface
    created._budget = _NoWaitBudget()
    box = {}

    async def run():
        turn_over = asyncio.Event()

        async def late():
            await turn_over.wait()
            box["late"] = await created.send("!aabbccdd", "late")

        async def handler(_event):
            # Fire and forget. The task copies the turn's context, including the reply node.
            box["task"] = asyncio.get_running_loop().create_task(late())
            return "pong"

        created.set_message_handler(handler)
        await created._accept({"node": "!aabbccdd", "text": "hello", "channel": False, "packet_id": "31"})
        await _finish_turns(created)
        assert created._send_tasks == set()
        turn_over.set()
        await asyncio.wait_for(box["task"], timeout=5)

    asyncio.run(run())
    assert box["late"].success is False
    assert "Unsolicited" in (box["late"].error or "")
    assert [text for _dest, text, _ack in iface.sent] == ["pong"]
