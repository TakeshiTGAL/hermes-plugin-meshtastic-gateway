"""Construct the real adapter through Hermes and drive one fake-radio reply.

Skipped when this pytest process cannot import Hermes. Preship runs the
plugin tests without the Hermes tree on PYTHONPATH.
"""
from __future__ import annotations

import asyncio
import threading
import time

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
        asked = adapter.radio_approval_text("Approve this?")
        assert [text for _dest, text, _ack in iface.sent] == [asked, "pong"]

        refused = await created.send("!aabbccdd", "later")
        assert refused.success is False
        assert "Unsolicited" in (refused.error or "")
        assert [text for _dest, text, _ack in iface.sent] == [asked, "pong"]

        created.config.extra["MESHTASTIC_CHUNK_BYTES"] = "64"
        created.config.extra["MESHTASTIC_MAX_CHUNKS"] = "1"
        token = adapter._reply_node.set("!aabbccdd")
        created._send_tasks.add(asyncio.current_task())
        try:
            original = adapter.asyncio.sleep

            async def no_wait(_seconds):
                return None

            adapter.asyncio.sleep = no_wait
            try:
                cut = await created.send("!aabbccdd", "a" * 100)
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
    created.config.extra["MESHTASTIC_CHUNK_BYTES"] = "64"
    created.config.extra["MESHTASTIC_MAX_CHUNKS"] = "4"

    async def run():
        token = adapter._reply_node.set("!aabbccdd")
        created._send_tasks.add(asyncio.current_task())
        original = adapter.asyncio.sleep

        async def no_wait(_seconds):
            return None

        adapter.asyncio.sleep = no_wait
        try:
            return await created.send("!aabbccdd", "a" * 100)
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
    gap = 10  # only waited between chunks of one reply; these tests send one chunk each

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
        adapter.radio_approval_text("Approve for !aabbccdd?"),
        adapter.radio_approval_text("Approve for !11223344?"),
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


def test_radio_cannot_approve_for_the_session_or_always(monkeypatch):
    monkeypatch.setattr(adapter, "remember", lambda *_args: None)
    _register()
    created = platform_registry.create_adapter("meshtastic-gateway", _config())
    iface = _Iface()
    created._iface = iface
    created._budget = _NoWaitBudget()
    handed = []

    async def handler(event):
        handed.append(event.text)
        return None

    async def run():
        created.set_message_handler(handler)
        for number, text in enumerate(("/approve always", "/approve session", "always", "/always"), start=40):
            await created._accept({"node": "!aabbccdd", "text": text, "channel": False, "packet_id": str(number)})
        await created._accept({"node": "!aabbccdd", "text": "/approve", "channel": False, "packet_id": "49"})
        await _finish_turns(created)

    asyncio.run(run())
    assert [text for _dest, text, _ack in iface.sent] == [
        adapter.APPROVAL_SCOPE_REFUSAL,
        adapter.APPROVAL_SCOPE_REFUSAL,
        adapter.APPROVAL_WORD_REFUSAL,
        adapter.COMMAND_REFUSAL,
    ]
    assert "/approve" in handed[0]
    assert not any(word in " ".join(handed) for word in ("always", "session"))


def test_a_chunk_size_below_the_floor_is_read_as_64(monkeypatch):
    monkeypatch.setattr(adapter, "remember", lambda *_args: None)
    _register()
    created = platform_registry.create_adapter("meshtastic-gateway", _config())
    iface = _Iface()
    created._iface = iface
    created._budget = _NoWaitBudget()
    created.config.extra["MESHTASTIC_CHUNK_BYTES"] = "4"
    created.config.extra["MESHTASTIC_MAX_CHUNKS"] = "1"

    async def run():
        token = adapter._reply_node.set("!aabbccdd")
        created._send_tasks.add(asyncio.current_task())
        try:
            return await created.send("!aabbccdd", "a" * 100)
        finally:
            created._send_tasks.discard(asyncio.current_task())
            adapter._reply_node.reset(token)

    result = asyncio.run(run())
    assert result.success is True
    sent = [text for _dest, text, _ack in iface.sent]
    assert len(sent) == 1 and len(sent[0].encode("utf-8")) == 64
    assert sent[0].endswith(" [cut]")
    assert "ends with [cut]" in (result.error or "")


def test_an_approval_question_is_never_sent_after_hermes_stops_waiting(monkeypatch):
    monkeypatch.setattr(adapter, "remember", lambda *_args: None)
    _register()
    cfg = _config()
    cfg.extra["MESHTASTIC_MIN_GAP_SECONDS"] = "20"
    created = platform_registry.create_adapter("meshtastic-gateway", cfg)
    iface = _Iface()
    created._iface = iface
    box = {}

    async def ask(text):
        started = time.monotonic()
        result = await asyncio.create_task(created.send(
            "!aabbccdd", text, metadata={"is_approval_prompt": True}))
        return result, time.monotonic() - started

    async def handler(_event):
        # A question that does not fit one chunk is not sent at all.
        box["long"] = await ask("x" * 500)
        # A short one goes out at once.
        box["short"] = await ask("Approve this?")
        # The 20 second gap is longer than Hermes waits, so nothing is sent and nothing waits.
        box["blocked"] = await ask("Approve that?")
        return None

    async def run():
        created.set_message_handler(handler)
        await created._accept({"node": "!aabbccdd", "text": "hello", "channel": False, "packet_id": "61"})
        await _finish_turns(created)
        # Long after the refusal, nothing more has gone out.
        await asyncio.sleep(0.2)

    asyncio.run(run())
    long_result, long_seconds = box["long"]
    short_result, short_seconds = box["short"]
    blocked_result, blocked_seconds = box["blocked"]
    assert long_result.success is False and long_seconds < 2
    assert long_result.error == "approval question does not fit one radio chunk"
    assert long_result.retryable is False
    assert short_result.success is True and short_seconds < 2
    assert blocked_result.success is False and blocked_seconds < 2
    assert "Hermes stops waiting" in (blocked_result.error or "")
    assert [text for _dest, text, _ack in iface.sent] == [adapter.radio_approval_text("Approve this?")]


def test_each_node_forgets_only_its_own_packet(monkeypatch):
    monkeypatch.setattr(adapter, "remember", lambda *_args: None)
    _register()
    cfg = _config()
    cfg.extra["MESHTASTIC_ALLOWED_NODES"] = "!aabbccdd,!11223344"
    created = platform_registry.create_adapter("meshtastic-gateway", cfg)
    created._budget = _NoWaitBudget()

    class _HalfIface(_Iface):
        def sendText(self, text, destinationId, wantAck=False):
            if destinationId == "!aabbccdd":
                raise ConnectionError("down")
            return super().sendText(text, destinationId, wantAck)

    created._iface = _HalfIface()
    created._seen = ["71", "72"]

    async def run():
        second_done = asyncio.Event()

        async def handler(event):
            if event.source.chat_id == "!aabbccdd":
                await asyncio.wait_for(second_done.wait(), timeout=10)
            else:
                asyncio.get_running_loop().call_soon(second_done.set)
            return f"final {event.source.chat_id}"

        created.set_message_handler(handler)
        await created._accept({"node": "!aabbccdd", "text": "first", "channel": False, "packet_id": "71"})
        await created._accept({"node": "!11223344", "text": "second", "channel": False, "packet_id": "72"})
        await _finish_turns(created)

    asyncio.run(run())
    # The first node's reply never reached the radio, so only its packet is forgotten.
    assert created._seen == ["72"]
    assert [text for _dest, text, _ack in created._iface.sent] == ["final !11223344"]


def test_radio_commands_outside_the_list_never_reach_hermes(monkeypatch):
    monkeypatch.setattr(adapter, "remember", lambda *_args: None)
    _register()
    created = platform_registry.create_adapter("meshtastic-gateway", _config())
    iface = _Iface()
    created._iface = iface
    created._budget = _NoWaitBudget()
    handed = []

    async def handler(event):
        handed.append(event.text)
        return None

    refused = ["/yolo", "/approvals off", "/APPROVALS OFF", "!yolo", "/yolo@bot"]
    passed = ["/status", "/help@bot", "/deny not now", "/approve once", "hello there"]

    async def run():
        created.set_message_handler(handler)
        for number, text in enumerate(refused + passed, start=80):
            await created._accept({"node": "!aabbccdd", "text": text, "channel": False, "packet_id": str(number)})
            await _finish_turns(created)

    asyncio.run(run())
    assert [text for _dest, text, _ack in iface.sent] == [adapter.COMMAND_REFUSAL] * len(refused)
    assert not any("yolo" in text or "approvals" in text.lower() for text in handed)
    for text in passed:
        assert any(text in seen for seen in handed), text


def test_hermes_text_approval_question_goes_out_only_when_it_fits_one_chunk(monkeypatch, tmp_path):
    monkeypatch.setenv("HERMES_HOME", str(tmp_path))
    monkeypatch.setattr(adapter, "remember", lambda *_args: None)
    from gateway.run import _format_exec_approval_fallback

    question = _format_exec_approval_fallback("rm -rf /tmp/build-cache && make clean", "recursive delete", "/")
    assert len(question.encode("utf-8")) > 200
    long_question = _format_exec_approval_fallback("echo " + "x" * 300, "long command", "/")
    _register()
    created = platform_registry.create_adapter("meshtastic-gateway", _config())
    iface = _Iface()
    created._iface = iface
    created._budget = _NoWaitBudget()
    results = []

    async def handler(_event):
        for text in (question, long_question):
            results.append(await asyncio.create_task(created.send(
                "!aabbccdd", text, metadata={"is_approval_prompt": True})))
        return None

    async def run():
        created.set_message_handler(handler)
        await created._accept({"node": "!aabbccdd", "text": "clean up", "channel": False, "packet_id": "91"})
        await _finish_turns(created)

    asyncio.run(run())
    # The short command fits one chunk and goes out. The long one does not fit, so it is not sent,
    # and Hermes closes that approval itself.
    assert results[0].success is True
    assert results[1].success is False
    assert results[1].error == "approval question does not fit one radio chunk"
    assert [text for _dest, text, _ack in iface.sent] == [
        "Reply /approve or /deny (once only). Run: rm -rf /tmp/build-cache && make clean — recursive delete"
    ]


@pytest.mark.parametrize("own_regex", [True, False])
def test_plain_text_restart_never_reaches_hermes(monkeypatch, own_regex):
    monkeypatch.setattr(adapter, "remember", lambda *_args: None)
    if not own_regex:
        # Only Hermes' own rewrite is left to catch the phrase.
        monkeypatch.setattr(adapter, "plaintext_restart", lambda _text: False)
    _register()
    created = platform_registry.create_adapter("meshtastic-gateway", _config())
    iface = _Iface()
    created._iface = iface
    created._budget = _NoWaitBudget()
    handed = []

    async def handler(event):
        handed.append(event.text)
        return None

    phrases = ["restart gateway", "please restart hermes", "Restart the Hermes gateway!"]

    async def run():
        created.set_message_handler(handler)
        for number, text in enumerate(phrases + ["restart the router"], start=95):
            await created._accept({"node": "!aabbccdd", "text": text, "channel": False, "packet_id": str(number)})
            await _finish_turns(created)

    asyncio.run(run())
    assert [text for _dest, text, _ack in iface.sent] == [adapter.COMMAND_REFUSAL] * len(phrases)
    assert handed == ["restart the router"]


async def _send_in_turn(created, text, **kwargs):
    token = adapter._reply_node.set("!aabbccdd")
    created._send_tasks.add(asyncio.current_task())
    try:
        return await created.send("!aabbccdd", text, **kwargs)
    finally:
        created._send_tasks.discard(asyncio.current_task())
        adapter._reply_node.reset(token)


def test_a_send_after_the_radio_is_lost_fails_at_once(monkeypatch):
    monkeypatch.setattr(adapter, "remember", lambda *_args: None)
    _register()
    created = platform_registry.create_adapter("meshtastic-gateway", _config())

    class _SlowIface(_Iface):
        def sendText(self, text, destinationId, wantAck=False):
            time.sleep(30)  # what the library does while it waits for a lost link
            return super().sendText(text, destinationId, wantAck)

    iface = _SlowIface()
    created._iface = iface
    created._budget = _NoWaitBudget()

    async def notify():
        return None

    created._notify_fatal_error = notify

    async def run():
        created._loop = asyncio.get_running_loop()
        created._running = True
        created._on_connection_lost()
        started = time.monotonic()
        result = await _send_in_turn(created, "hello")
        return result, time.monotonic() - started

    result, seconds = asyncio.run(run())
    assert result.success is False
    assert "not connected" in (result.error or "")
    assert result.retryable is False
    assert seconds < 1
    assert iface.sent == []


def test_a_send_while_the_link_is_down_fails_at_once(monkeypatch):
    monkeypatch.setattr(adapter, "remember", lambda *_args: None)
    _register()
    created = platform_registry.create_adapter("meshtastic-gateway", _config())
    iface = _Iface()
    iface.isConnected = threading.Event()  # the library clears it while the link is down
    created._iface = iface
    created._budget = _NoWaitBudget()

    async def run():
        started = time.monotonic()
        result = await _send_in_turn(created, "hello")
        return result, time.monotonic() - started

    result, seconds = asyncio.run(run())
    assert result.success is False and "not connected" in (result.error or "")
    assert seconds < 1
    assert iface.sent == []
    iface.isConnected.set()
    assert asyncio.run(_send_in_turn(created, "hello")).success is True


def test_a_slow_radio_send_does_not_block_the_event_loop(monkeypatch):
    monkeypatch.setattr(adapter, "remember", lambda *_args: None)
    _register()
    created = platform_registry.create_adapter("meshtastic-gateway", _config())

    class _SlowIface(_Iface):
        def sendText(self, text, destinationId, wantAck=False):
            time.sleep(1.0)
            return super().sendText(text, destinationId, wantAck)

    created._iface = _SlowIface()
    created._budget = _NoWaitBudget()

    async def run():
        ticks = 0
        done = asyncio.Event()

        async def ticker():
            nonlocal ticks
            while not done.is_set():
                ticks += 1
                await asyncio.sleep(0.05)

        tick_task = asyncio.create_task(ticker())
        result = await _send_in_turn(created, "hello")
        done.set()
        await tick_task
        return result, ticks

    result, ticks = asyncio.run(run())
    assert result.success is True
    assert ticks >= 10


def test_an_approval_question_does_not_wait_past_its_limit_for_another_send(monkeypatch):
    monkeypatch.setattr(adapter, "remember", lambda *_args: None)
    monkeypatch.setattr(adapter, "APPROVAL_SEND_SECONDS", 0.3)
    _register()
    created = platform_registry.create_adapter("meshtastic-gateway", _config())

    class _SlowIface(_Iface):
        def sendText(self, text, destinationId, wantAck=False):
            time.sleep(1.0)
            return super().sendText(text, destinationId, wantAck)

    iface = _SlowIface()
    created._iface = iface
    created._budget = _NoWaitBudget()

    async def run():
        reply = asyncio.create_task(_send_in_turn(created, "final reply"))
        await asyncio.sleep(0.1)  # the reply now holds the radio
        created._turn_nodes["!aabbccdd"] = 1
        created._send_tasks.add(reply)
        started = time.monotonic()
        asked = await asyncio.create_task(created.send(
            "!aabbccdd", "Approve this?", metadata={"is_approval_prompt": True}))
        waited = time.monotonic() - started
        await reply
        await asyncio.sleep(0.2)
        return asked, waited

    asked, waited = asyncio.run(run())
    assert asked.success is False and "Hermes stops waiting" in (asked.error or "")
    assert waited < 0.9
    assert [text for _dest, text, _ack in iface.sent] == ["final reply"]


def test_hermes_closes_an_approval_whose_question_did_not_fit(monkeypatch, tmp_path):
    """Hermes' own approval wait, fed through this adapter: the long question is not sent, and the
    command is refused when the approval wait ends."""
    monkeypatch.setenv("HERMES_HOME", str(tmp_path))
    monkeypatch.setattr(adapter, "remember", lambda *_args: None)
    from gateway.run import _format_exec_approval_fallback
    from tools import approval_context
    from tools.approval_gateway_wait import _await_gateway_decision

    monkeypatch.setattr(approval_context, "_get_approval_timeout", lambda: 1)
    _register()
    created = platform_registry.create_adapter("meshtastic-gateway", _config())
    iface = _Iface()
    created._iface = iface
    created._budget = _NoWaitBudget()
    command = "echo " + "x" * 300
    question = _format_exec_approval_fallback(command, "long command", "/")
    seen = {}

    async def run():
        loop = asyncio.get_running_loop()
        turn = asyncio.create_task(asyncio.sleep(10))  # the turn this approval belongs to
        created._send_tasks.add(turn)
        created._turn_nodes["!aabbccdd"] = 1

        def notify(_data):
            # Hermes' text path: send, wait up to 15 seconds, and do not raise on a failed send.
            future = asyncio.run_coroutine_threadsafe(
                created.send("!aabbccdd", question, metadata={"is_approval_prompt": True}), loop)
            seen["send"] = future.result(timeout=15)

        decision = await asyncio.to_thread(
            _await_gateway_decision, "radio-session", notify,
            {"command": command, "description": "long command", "pattern_key": "k", "pattern_keys": ["k"]})
        turn.cancel()
        return decision

    decision = asyncio.run(run())
    assert seen["send"].success is False
    assert seen["send"].error == "approval question does not fit one radio chunk"
    assert iface.sent == []
    assert decision.get("resolved") is False
    assert decision.get("choice") in (None, "deny", "timeout")
