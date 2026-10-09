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
            name="radio-dm-gateway",
            label="Radio DM",
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


def _hold_pending(session_key: str, command: str) -> None:
    """Queue the command /approve would run. The prompt must carry this exact string."""
    from tools import approval

    entry = type("_Pending", (), {})()
    entry.data = {"command": command}
    with approval._lock:
        approval._gateway_queues.setdefault(session_key, []).append(entry)


def _clear_pending(session_key: str) -> None:
    from tools import approval

    with approval._lock:
        approval._gateway_queues.pop(session_key, None)


def test_create_adapter_then_handle_message_sends_pong():
    _register()
    created = platform_registry.create_adapter("radio-dm-gateway", _config())
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
        asked = "Approve this?"
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
    created = platform_registry.create_adapter("radio-dm-gateway", _config())

    async def run():
        assert await created.connect() is True

    asyncio.run(run())
    assert seen["thread"] is not threading.current_thread()


def test_stopped_loop_does_not_consume_the_packet_id(monkeypatch):
    monkeypatch.setattr(adapter, "local_node", lambda _iface: "!11223344")
    _register()
    created = platform_registry.create_adapter("radio-dm-gateway", _config())
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
    created = platform_registry.create_adapter("radio-dm-gateway", _config())
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
    created = platform_registry.create_adapter("radio-dm-gateway", _config())
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
    created = platform_registry.create_adapter("radio-dm-gateway", _config())
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
    created = platform_registry.create_adapter("radio-dm-gateway", cfg)
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
    created = platform_registry.create_adapter("radio-dm-gateway", _config())
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
    assert result.raw_response == {"chunks_sent": 1, "may_have_reached": True}
    assert created._send_retry_is_final(result) is True
    assert "not sent again" in (result.error or "")
    assert "may have reached the radio" in (result.error or "")


def test_a_first_chunk_link_drop_is_final_and_names_no_retry_class(monkeypatch):
    def send_text(*_args):
        raise ConnectionResetError("reset")

    monkeypatch.setattr(adapter, "send_text", send_text)
    monkeypatch.setattr(adapter, "remember", lambda *_args: None)
    _register()
    created = platform_registry.create_adapter("radio-dm-gateway", _config())
    created._iface = object()

    async def run():
        token = adapter._reply_node.set("!aabbccdd")
        created._send_tasks.add(asyncio.current_task())
        try:
            return await created.send("!aabbccdd", "hello")
        finally:
            created._send_tasks.discard(asyncio.current_task())
            adapter._reply_node.reset(token)

    result = asyncio.run(run())
    low = (result.error or "").lower()
    assert result.success is False
    assert result.raw_response == {"chunks_sent": 0, "may_have_reached": True}
    assert created._send_retry_is_final(result) is True
    assert "may have reached the radio" in low
    assert "not sent again" in low
    for mark in (
        "connectionreset",
        "connectionerror",
        "connectionrefused",
        "connecterror",
        "connecttimeout",
        "broken pipe",
        "remotedisconnected",
        "eoferror",
    ):
        assert mark not in low


def test_a_link_drop_keeps_the_packet_id(monkeypatch):
    def send_text(*_args):
        raise ConnectionError("down")

    monkeypatch.setattr(adapter, "send_text", send_text)
    monkeypatch.setattr(adapter, "remember", lambda *_args: None)
    _register()
    created = platform_registry.create_adapter("radio-dm-gateway", _config())
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
    assert created._seen == ["9"]


def test_a_reply_that_never_calls_send_text_can_be_accepted_again(monkeypatch):
    monkeypatch.setattr(adapter, "remember", lambda *_args: None)
    _register()
    created = platform_registry.create_adapter("radio-dm-gateway", _config())
    created._iface = object()
    created._radio_lost = True
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
    created = platform_registry.create_adapter("radio-dm-gateway", cfg)
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
    created = platform_registry.create_adapter("radio-dm-gateway", _config())
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
    created = platform_registry.create_adapter("radio-dm-gateway", _config())
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
    created = platform_registry.create_adapter("radio-dm-gateway", _config())
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
    created = platform_registry.create_adapter("radio-dm-gateway", cfg)
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
    assert [text for _dest, text, _ack in iface.sent] == ["Approve this?"]


def test_one_nodes_link_drop_does_not_drop_the_other_packet(monkeypatch):
    monkeypatch.setattr(adapter, "remember", lambda *_args: None)
    _register()
    cfg = _config()
    cfg.extra["MESHTASTIC_ALLOWED_NODES"] = "!aabbccdd,!11223344"
    created = platform_registry.create_adapter("radio-dm-gateway", cfg)
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
    # The first send may have reached the radio, so its packet id stays. The other node is untouched.
    assert created._seen == ["71", "72"]
    assert [text for _dest, text, _ack in created._iface.sent] == ["final !11223344"]


def test_radio_commands_outside_the_list_never_reach_hermes(monkeypatch):
    monkeypatch.setattr(adapter, "remember", lambda *_args: None)
    _register()
    created = platform_registry.create_adapter("radio-dm-gateway", _config())
    iface = _Iface()
    created._iface = iface
    created._budget = _NoWaitBudget()
    handed = []

    async def handler(event):
        handed.append(event.text)
        return None

    refused = ["/yolo", "/approvals off", "/APPROVALS OFF", "!yolo", "/yolo@bot"]
    passed = ["/status", "/help@bot", "/deny not now", "/approve once", "/cancel", "hello there"]

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
    created = platform_registry.create_adapter("radio-dm-gateway", _config())
    iface = _Iface()
    created._iface = iface
    created._budget = _NoWaitBudget()
    results = []
    command = "rm -rf /tmp/build-cache && make clean"
    from gateway.platforms.base import ExecApprovalPrompt

    async def handler(_event):
        for text in (question, long_question):
            results.append(await asyncio.create_task(created.send(
                "!aabbccdd", text, metadata={"is_approval_prompt": True})))
        _hold_pending("s-fit", command)
        results.append(await created._send_exec_approval_prompt(ExecApprovalPrompt(
            chat_id="!aabbccdd",
            session_key="s-fit",
            text=question,
            actions=[("Allow Once", "once", "primary")],
            command=command,
            description="recursive delete",
            smart_denied=False,
        )))
        _clear_pending("s-fit")
        return None

    async def run():
        created.set_message_handler(handler)
        await created._accept({"node": "!aabbccdd", "text": "clean up", "channel": False, "packet_id": "91"})
        await _finish_turns(created)

    asyncio.run(run())
    # The formatted prompt is fenced text. It is not parsed into a command, and it is not sent.
    assert results[0].success is False
    assert results[1].success is False
    assert results[1].error == "approval question does not fit one radio chunk"
    assert results[2].success is True
    aired = [text for _dest, text, _ack in iface.sent]
    assert aired == [adapter.radio_approval_line(command, "recursive delete", 200)]
    assert aired[0].split("Run: ", 1)[1].split(" — ", 1)[0] == command


@pytest.mark.parametrize("own_regex", [True, False])
def test_plain_text_restart_never_reaches_hermes(monkeypatch, own_regex):
    monkeypatch.setattr(adapter, "remember", lambda *_args: None)
    if not own_regex:
        # Only Hermes' own rewrite is left to catch the phrase.
        monkeypatch.setattr(adapter, "plaintext_restart", lambda _text: False)
    _register()
    created = platform_registry.create_adapter("radio-dm-gateway", _config())
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
    created = platform_registry.create_adapter("radio-dm-gateway", _config())

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
    created = platform_registry.create_adapter("radio-dm-gateway", _config())
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
    created = platform_registry.create_adapter("radio-dm-gateway", _config())

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
    created = platform_registry.create_adapter("radio-dm-gateway", _config())

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
    created = platform_registry.create_adapter("radio-dm-gateway", _config())
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


def test_a_startup_replay_without_the_reply_context_still_sends(monkeypatch):
    monkeypatch.setattr(adapter, "remember", lambda *_args: None)
    _register()
    created = platform_registry.create_adapter("radio-dm-gateway", _config())
    iface = _Iface()
    created._iface = iface
    created._budget = _NoWaitBudget()

    async def handler(_event):
        return "pong-replay"

    async def run():
        created.set_message_handler(handler)
        source = created.build_source(
            chat_id="!aabbccdd",
            chat_type="dm",
            user_id="!aabbccdd",
            user_name="!aabbccdd",
            message_id="replay-1",
        )
        event = adapter.MessageEvent(
            text="hello",
            message_type=adapter.MessageType.TEXT,
            source=source,
            raw_message={"node": "!aabbccdd", "channel": False, "radio_dm_origin": True},
            message_id="replay-1",
        )
        event._radio_dm_origin = True
        assert adapter._reply_node.get() == ""
        await created.handle_message(event)
        await _finish_turns(created)

    asyncio.run(run())
    assert [text for _dest, text, _ack in iface.sent] == ["pong-replay"]


def test_an_unstamped_event_is_still_unsolicited(monkeypatch):
    monkeypatch.setattr(adapter, "remember", lambda *_args: None)
    _register()
    created = platform_registry.create_adapter("radio-dm-gateway", _config())
    iface = _Iface()
    created._iface = iface
    created._budget = _NoWaitBudget()

    async def handler(_event):
        return "pong"

    async def run():
        created.set_message_handler(handler)
        source = created.build_source(
            chat_id="!aabbccdd",
            chat_type="dm",
            user_id="!aabbccdd",
            user_name="!aabbccdd",
            message_id="plain-1",
        )
        event = adapter.MessageEvent(
            text="hello",
            message_type=adapter.MessageType.TEXT,
            source=source,
            raw_message={"node": "!aabbccdd", "channel": False},
            message_id="plain-1",
        )
        await created.handle_message(event)
        await _finish_turns(created)

    asyncio.run(run())
    assert iface.sent == []


def test_interim_text_during_the_turn_is_refused_and_the_final_reply_is_sent(monkeypatch):
    monkeypatch.setattr(adapter, "remember", lambda *_args: None)
    _register()
    created = platform_registry.create_adapter("radio-dm-gateway", _config())
    iface = _Iface()
    created._iface = iface
    created._budget = _NoWaitBudget()
    # _accept does not record the id. _on_packet does. Seed it so a mistaken forget is visible.
    created._seen = ["replay-2"]
    box = {}

    async def handler(_event):
        box["interim"] = await created.send(
            "!aabbccdd",
            "📬 No home channel is set for Radio_Dm_Gateway. Type /sethome",
        )
        box["approval"] = await asyncio.create_task(created.send(
            "!aabbccdd",
            "Approve this?",
            metadata={"is_approval_prompt": True},
        ))
        return "pong"

    async def run():
        created.set_message_handler(handler)
        await created._accept({"node": "!aabbccdd", "text": "hello", "channel": False, "packet_id": "replay-2"})
        await _finish_turns(created)

    asyncio.run(run())
    assert box["interim"].success is False
    assert box["interim"].error == "Interim radio text is refused. Nothing was sent."
    assert box["approval"].success is True
    assert [text for _dest, text, _ack in iface.sent] == [
        "Approve this?",
        "pong",
    ]
    assert "sethome" not in " ".join(text for _dest, text, _ack in iface.sent)
    assert created._seen == ["replay-2"]


def test_new_during_a_running_turn_does_not_reset(monkeypatch):
    monkeypatch.setattr(adapter, "remember", lambda *_args: None)
    _register()
    created = platform_registry.create_adapter("radio-dm-gateway", _config())
    iface = _Iface()
    created._iface = iface
    created._budget = _NoWaitBudget()
    handed = []
    release = asyncio.Event()

    async def handler(event):
        handed.append(event.text)
        if event.text == "go":
            await release.wait()
        return None

    async def run():
        created.set_message_handler(handler)
        first = asyncio.create_task(created._accept({
            "node": "!aabbccdd", "text": "go", "channel": False, "packet_id": "busy-1",
        }))
        for _ in range(50):
            if handed == ["go"]:
                break
            await asyncio.sleep(0)
        await created._accept({
            "node": "!aabbccdd", "text": "/new", "channel": False, "packet_id": "busy-2",
        })
        await created._accept({
            "node": "!aabbccdd", "text": "/reset later", "channel": False, "packet_id": "busy-3",
        })
        await created._accept({
            "node": "!aabbccdd", "text": "/stop", "channel": False, "packet_id": "busy-4",
        })
        release.set()
        await first
        await _finish_turns(created)

    asyncio.run(run())
    assert "/new" not in handed
    assert "/reset later" not in handed
    assert handed == ["go", "/stop"]
    assert [text for _dest, text, _ack in iface.sent].count(adapter.BUSY_RESET_REFUSAL) == 2


def test_hermes_busy_text_during_a_turn_stays_off_the_radio(monkeypatch):
    monkeypatch.setattr(adapter, "remember", lambda *_args: None)
    _register()
    created = platform_registry.create_adapter("radio-dm-gateway", _config())
    iface = _Iface()
    created._iface = iface
    created._budget = _NoWaitBudget()
    release = asyncio.Event()
    handed = []
    ack = (
        "↪ Redirected current run. I'll adjust using your correction.\n\n"
        "💡 First-time tip — I redirected the current run using your message. "
        "Send `/busy queue` to wait for a separate turn."
    )

    async def busy(event, _session_key):
        result = await created.send(event.source.chat_id, ack)
        handed.append(("ack", result.success))
        return True

    async def handler(event):
        handed.append(event.text)
        if event.text == "go":
            await release.wait()
        return "pong"

    async def run():
        created.set_message_handler(handler)
        created._busy_session_handler = busy
        first = asyncio.create_task(created._accept({
            "node": "!aabbccdd", "text": "go", "channel": False, "packet_id": "ack-1",
        }))
        for _ in range(50):
            if handed[:1] == ["go"]:
                break
            await asyncio.sleep(0)
        await created._accept({
            "node": "!aabbccdd", "text": "second", "channel": False, "packet_id": "ack-2",
        })
        release.set()
        await first
        await _finish_turns(created)

    asyncio.run(run())
    aired = [text for _dest, text, _ack in iface.sent]
    joined = "\n".join(aired)
    assert "Redirected" not in joined
    assert "First-time tip" not in joined
    assert "/busy queue" not in joined
    assert ("ack", False) in handed
    assert "second" not in handed
    assert "pong" in aired


def test_an_unsent_exec_approval_is_declined_so_hermes_drops_it(monkeypatch):
    monkeypatch.setattr(adapter, "remember", lambda *_args: None)
    from gateway.platforms.base import ExecApprovalPrompt
    from gateway.relay.egress import declined_send

    _register()
    created = platform_registry.create_adapter("radio-dm-gateway", _config())
    iface = _Iface()
    created._iface = iface
    created._budget = _NoWaitBudget()
    box = {}

    async def handler(_event):
        long_prompt = ExecApprovalPrompt(
            chat_id="!aabbccdd",
            session_key="s-long",
            text="x" * 500,
            actions=[("Allow Once", "once", "primary")],
            command="x" * 400,
            description="long",
            smart_denied=False,
        )
        _hold_pending("s-long", "x" * 400)
        box["long"] = await created._send_exec_approval_prompt(long_prompt)
        _clear_pending("s-long")
        short_prompt = ExecApprovalPrompt(
            chat_id="!aabbccdd",
            session_key="s-short",
            text="echo hi",
            actions=[("Allow Once", "once", "primary")],
            command="echo hi",
            description="list",
            smart_denied=False,
        )
        _hold_pending("s-short", "echo hi")
        box["short"] = await created._send_exec_approval_prompt(short_prompt)
        _clear_pending("s-short")
        return None

    async def run():
        created.set_message_handler(handler)
        await created._accept({"node": "!aabbccdd", "text": "go", "channel": False, "packet_id": "ea1"})
        await _finish_turns(created)

    asyncio.run(run())
    assert box["long"].success is False
    assert declined_send(box["long"]) is True
    assert box["short"].success is True
    assert declined_send(box["short"]) is False
    sent = [text for _dest, text, _ack in iface.sent]
    assert sent == [adapter.radio_approval_line("echo hi", "list", 200)]
    assert sent[0].split("Run: ", 1)[1].split(" — ", 1)[0] == "echo hi"
    assert "x" * 50 not in sent[0]


def test_a_command_that_contains_a_fence_is_not_transmitted(monkeypatch):
    monkeypatch.setattr(adapter, "remember", lambda *_args: None)
    from gateway.platforms.base import ExecApprovalPrompt
    from gateway.relay.egress import declined_send

    _register()
    created = platform_registry.create_adapter("radio-dm-gateway", _config())
    iface = _Iface()
    created._iface = iface
    created._budget = _NoWaitBudget()
    box = {}
    # The fence hides a second command. Parsing the prompt text would air only "echo hi".
    command = "echo hi\n```\nrm -rf /tmp/fence-marker\n```"
    shown = "Heading\n```\necho hi\n```\nWhy it was flagged: recursive delete"

    async def handler(_event):
        _hold_pending("s-fence", command)
        box["fence"] = await created._send_exec_approval_prompt(ExecApprovalPrompt(
            chat_id="!aabbccdd",
            session_key="s-fence",
            text=shown,
            actions=[("Allow Once", "once", "primary")],
            command=command,
            description="recursive delete",
            smart_denied=False,
        ))
        _clear_pending("s-fence")
        newline = "echo a\necho b"
        _hold_pending("s-newline", newline)
        box["newline"] = await created._send_exec_approval_prompt(ExecApprovalPrompt(
            chat_id="!aabbccdd",
            session_key="s-newline",
            text="echo a\necho b",
            actions=[("Allow Once", "once", "primary")],
            command=newline,
            description="two lines",
            smart_denied=False,
        ))
        _clear_pending("s-newline")
        box["bare"] = await created._send_exec_approval_prompt(type("Bare", (), {
            "chat_id": "!aabbccdd",
            "text": shown,
            "metadata": {},
        })())
        return None

    async def run():
        created.set_message_handler(handler)
        await created._accept({"node": "!aabbccdd", "text": "go", "channel": False, "packet_id": "fence1"})
        await _finish_turns(created)

    asyncio.run(run())
    assert box["fence"].success is False and declined_send(box["fence"]) is True
    assert box["newline"].success is False and declined_send(box["newline"]) is True
    assert box["bare"].success is False and declined_send(box["bare"]) is True
    assert iface.sent == []


def test_a_unicode_line_break_or_bidi_override_is_not_transmitted(monkeypatch):
    monkeypatch.setattr(adapter, "remember", lambda *_args: None)
    from gateway.platforms.base import ExecApprovalPrompt
    from gateway.relay.egress import declined_send

    _register()
    created = platform_registry.create_adapter("radio-dm-gateway", _config())
    iface = _Iface()
    created._iface = iface
    created._budget = _NoWaitBudget()
    box = {}
    cases = {
        "line": "echo hi\u2028there",
        "para": "echo hi\u2029there",
        "bidi": "echo hi\u202ethere",
    }

    async def handler(_event):
        for key, command in cases.items():
            _hold_pending(f"s-{key}", command)
            box[key] = await created._send_exec_approval_prompt(ExecApprovalPrompt(
                chat_id="!aabbccdd",
                session_key=f"s-{key}",
                text=command,
                actions=[("Allow Once", "once", "primary")],
                command=command,
                description="list",
                smart_denied=False,
            ))
            _clear_pending(f"s-{key}")
        _hold_pending("s-reason", "echo hi")
        box["reason"] = await created._send_exec_approval_prompt(ExecApprovalPrompt(
            chat_id="!aabbccdd",
            session_key="s-reason",
            text="echo hi",
            actions=[("Allow Once", "once", "primary")],
            command="echo hi",
            description="why\u202ethere",
            smart_denied=False,
        ))
        _clear_pending("s-reason")
        return None

    async def run():
        created.set_message_handler(handler)
        await created._accept({"node": "!aabbccdd", "text": "go", "channel": False, "packet_id": "uni1"})
        await _finish_turns(created)

    asyncio.run(run())
    for key in ("line", "para", "bidi", "reason"):
        assert box[key].success is False and declined_send(box[key]) is True, key
    assert iface.sent == []


def test_a_redacted_command_is_not_transmitted(monkeypatch):
    """Hermes shows a redacted copy. /approve runs the original. Those must not be aired apart."""
    monkeypatch.setattr(adapter, "remember", lambda *_args: None)
    from agent.redact import redact_sensitive_text
    from gateway.platforms.base import ExecApprovalPrompt
    from gateway.relay.egress import declined_send

    original = "rm -r /tmp/radio-redact-missing sk-radioaudit00"
    shown = redact_sensitive_text(original, force=True)
    assert shown != original
    _register()
    created = platform_registry.create_adapter("radio-dm-gateway", _config())
    iface = _Iface()
    created._iface = iface
    created._budget = _NoWaitBudget()
    box = {}

    async def handler(_event):
        _hold_pending("s-redact", original)
        box["redacted"] = await created._send_exec_approval_prompt(ExecApprovalPrompt(
            chat_id="!aabbccdd",
            session_key="s-redact",
            text=shown,
            actions=[("Allow Once", "once", "primary")],
            command=shown,
            description="delete in root path",
            smart_denied=False,
        ))
        _clear_pending("s-redact")
        # Current Hermes stores the masked copy. Matching that copy must still not air.
        _hold_pending("s-mask", shown)
        box["masked_queue"] = await created._send_exec_approval_prompt(ExecApprovalPrompt(
            chat_id="!aabbccdd",
            session_key="s-mask",
            text=shown,
            actions=[("Allow Once", "once", "primary")],
            command=shown,
            description="delete in root path",
            smart_denied=False,
        ))
        _clear_pending("s-mask")
        _hold_pending("s-stars", "echo ***")
        box["stars"] = await created._send_exec_approval_prompt(ExecApprovalPrompt(
            chat_id="!aabbccdd",
            session_key="s-stars",
            text="echo ***",
            actions=[("Allow Once", "once", "primary")],
            command="echo ***",
            description="list",
            smart_denied=False,
        ))
        _clear_pending("s-stars")
        # An older different command is what /approve would run. Do not air the newer one.
        _hold_pending("s-order", "echo first")
        _hold_pending("s-order", "echo second")
        box["newer"] = await created._send_exec_approval_prompt(ExecApprovalPrompt(
            chat_id="!aabbccdd",
            session_key="s-order",
            text="echo second",
            actions=[("Allow Once", "once", "primary")],
            command="echo second",
            description="list",
            smart_denied=False,
        ))
        box["oldest"] = await created._send_exec_approval_prompt(ExecApprovalPrompt(
            chat_id="!aabbccdd",
            session_key="s-order",
            text="echo first",
            actions=[("Allow Once", "once", "primary")],
            command="echo first",
            description="list",
            smart_denied=False,
        ))
        _clear_pending("s-order")
        return None

    async def run():
        created.set_message_handler(handler)
        await created._accept({"node": "!aabbccdd", "text": "go", "channel": False, "packet_id": "red1"})
        await _finish_turns(created)

    asyncio.run(run())
    assert box["redacted"].success is False and declined_send(box["redacted"]) is True
    assert box["masked_queue"].success is False and declined_send(box["masked_queue"]) is True
    assert box["stars"].success is False and declined_send(box["stars"]) is True
    assert box["newer"].success is False and declined_send(box["newer"]) is True
    assert box["oldest"].success is True
    aired = [text for _dest, text, _ack in iface.sent]
    assert len(aired) == 1
    assert aired[0].split("Run: ", 1)[1].split(" — ", 1)[0] == "echo first"
    assert "sk-radioaudit00" not in aired[0]
    assert shown not in "\n".join(aired)


def test_a_missing_approval_queue_is_not_transmitted(monkeypatch):
    monkeypatch.setattr(adapter, "remember", lambda *_args: None)
    from gateway.platforms.base import ExecApprovalPrompt
    from gateway.relay.egress import declined_send
    import tools.approval as approval

    monkeypatch.delattr(approval, "get_pending_gateway_approval")
    _register()
    created = platform_registry.create_adapter("radio-dm-gateway", _config())
    iface = _Iface()
    created._iface = iface
    created._budget = _NoWaitBudget()
    box = {}

    async def handler(_event):
        box["result"] = await created._send_exec_approval_prompt(ExecApprovalPrompt(
            chat_id="!aabbccdd",
            session_key="s-missing",
            text="echo hi",
            actions=[("Allow Once", "once", "primary")],
            command="echo hi",
            description="list",
            smart_denied=False,
        ))
        return None

    async def run():
        created.set_message_handler(handler)
        await created._accept({"node": "!aabbccdd", "text": "go", "channel": False, "packet_id": "miss1"})
        await _finish_turns(created)

    asyncio.run(run())
    assert box["result"].success is False and declined_send(box["result"]) is True
    assert iface.sent == []


def test_a_full_transmit_queue_does_not_call_send_text(monkeypatch):
    import radio
    from gateway.platforms.base import ExecApprovalPrompt
    from gateway.relay.egress import declined_send

    monkeypatch.setattr(radio, "SEND_QUEUE_SECONDS", 0.05)
    monkeypatch.setattr(adapter, "APPROVAL_SEND_SECONDS", 0.05)
    monkeypatch.setattr(adapter, "remember", lambda *_args: None)
    _register()
    created = platform_registry.create_adapter("radio-dm-gateway", _config())
    iface = _Iface()
    iface.queueStatus = type("Queue", (), {"free": 0})()
    created._iface = iface
    created._budget = _NoWaitBudget()
    box = {}

    def _prompt():
        return ExecApprovalPrompt(
            chat_id="!aabbccdd",
            session_key="s-queue",
            text="echo hi",
            actions=[("Allow Once", "once", "primary")],
            command="echo hi",
            description="list",
            smart_denied=False,
        )

    async def handler(_event):
        _hold_pending("s-queue", "echo hi")
        box["full"] = await created._send_exec_approval_prompt(_prompt())
        iface.queueStatus.free = 1
        box["free"] = await created._send_exec_approval_prompt(_prompt())
        _clear_pending("s-queue")
        return None

    async def run():
        created.set_message_handler(handler)
        await created._accept({"node": "!aabbccdd", "text": "go", "channel": False, "packet_id": "q1"})
        await _finish_turns(created)

    asyncio.run(run())
    assert box["full"].success is False and declined_send(box["full"]) is True
    assert box["full"].error == "The radio queue is full. Nothing was sent."
    assert not (isinstance(box["full"].raw_response, dict) and box["full"].raw_response.get("may_have_reached"))
    assert box["free"].success is True
    assert [text for _dest, text, _ack in iface.sent] == [adapter.radio_approval_line("echo hi", "list", 200)]


def test_an_approval_queue_wait_stays_inside_the_gap_budget(monkeypatch):
    """The gap and a full queue share 12 seconds. They must not stack past Hermes's 15."""
    import radio
    from gateway.platforms.base import ExecApprovalPrompt
    from gateway.relay.egress import declined_send

    monkeypatch.setattr(adapter, "APPROVAL_SEND_SECONDS", 0.45)
    # The old bug added this whole wait after the gap and outran Hermes.
    monkeypatch.setattr(radio, "SEND_QUEUE_SECONDS", 5.0)
    monkeypatch.setattr(adapter, "remember", lambda *_args: None)
    _register()
    created = platform_registry.create_adapter("radio-dm-gateway", _config())
    iface = _Iface()
    iface.queueStatus = type("Queue", (), {"free": 0})()
    created._iface = iface

    class _Gap:
        def plan(self, _count, _now):
            return None, 0.25

        def mark(self, _now):
            return None

    created._budget = _Gap()
    box = {}

    async def handler(_event):
        _hold_pending("s-budget", "echo hi")
        started = time.monotonic()
        box["result"] = await created._send_exec_approval_prompt(ExecApprovalPrompt(
            chat_id="!aabbccdd",
            session_key="s-budget",
            text="echo hi",
            actions=[("Allow Once", "once", "primary")],
            command="echo hi",
            description="list",
            smart_denied=False,
        ))
        box["elapsed"] = time.monotonic() - started
        _clear_pending("s-budget")
        return None

    async def run():
        created.set_message_handler(handler)
        await created._accept({"node": "!aabbccdd", "text": "go", "channel": False, "packet_id": "bud1"})
        await _finish_turns(created)

    asyncio.run(run())
    assert box["result"].success is False and declined_send(box["result"]) is True
    assert box["result"].error == "The radio queue is full. Nothing was sent."
    assert not (isinstance(box["result"].raw_response, dict) and box["result"].raw_response.get("may_have_reached"))
    assert box["elapsed"] > 0.25
    assert box["elapsed"] < 0.9
    assert iface.sent == []


def test_a_queue_that_fills_during_send_text_is_not_written(monkeypatch):
    """The library stores the packet, then sleeps while free == 0. Stop that."""
    import radio
    from gateway.platforms.base import ExecApprovalPrompt
    from gateway.relay.egress import declined_send

    monkeypatch.setattr(adapter, "remember", lambda *_args: None)
    _register()
    created = platform_registry.create_adapter("radio-dm-gateway", _config())

    class _Race:
        def __init__(self):
            self.written = []
            self.queue = {7: "already"}
            self.calls = 0
            self.sleeps = 0
            self.block = True
            self._reads = 0
            self.queueStatus = self

        @property
        def free(self):
            self._reads += 1
            if not self.block:
                return 4
            return 4 if self._reads < 2 else 0

        def _queueHasFreeSpace(self):
            return self.free > 0

        def sendText(self, text, destinationId, wantAck=False):
            self.calls += 1
            self.queue[1000 + self.calls] = text
            while self.queue:
                while not self._queueHasFreeSpace():
                    self.sleeps += 1
                    if self.sleeps > 3:
                        raise TimeoutError("library wait was not bounded")
                    time.sleep(0.05)
                key = next(iter(self.queue))
                packet = self.queue.pop(key)
                if isinstance(packet, str) and packet != "already":
                    self.written.append(packet)
            return {"id": self.calls}

    iface = _Race()
    created._iface = iface
    created._budget = _NoWaitBudget()
    box = {}
    line = adapter.radio_approval_line("echo hi", "list", 200)

    def _prompt():
        return ExecApprovalPrompt(
            chat_id="!aabbccdd",
            session_key="s-race",
            text="echo hi",
            actions=[("Allow Once", "once", "primary")],
            command="echo hi",
            description="list",
            smart_denied=False,
        )

    async def handler(_event):
        _hold_pending("s-race", "echo hi")
        try:
            started = time.monotonic()
            box["race"] = await created._send_exec_approval_prompt(_prompt())
            box["elapsed"] = time.monotonic() - started
            box["queue_after_race"] = dict(iface.queue)
            box["method_restored"] = iface._queueHasFreeSpace.__func__ is _Race._queueHasFreeSpace
            iface.block = False
            box["later"] = await created._send_exec_approval_prompt(_prompt())
        finally:
            _clear_pending("s-race")
        return None

    async def run():
        created.set_message_handler(handler)
        await created._accept({"node": "!aabbccdd", "text": "go", "channel": False, "packet_id": "race1"})
        await _finish_turns(created)

    asyncio.run(run())
    assert box["race"].success is False and declined_send(box["race"]) is True
    assert box["race"].error == "The radio queue is full. Nothing was sent."
    assert not (isinstance(box["race"].raw_response, dict) and box["race"].raw_response.get("may_have_reached"))
    assert box["elapsed"] < 0.4
    assert iface.sleeps == 0
    assert iface.calls == 2
    assert 7 in box["queue_after_race"]
    assert line not in box["queue_after_race"].values()
    assert box["method_restored"] is True
    assert iface.written == [line]
    assert box["later"].success is True


def test_slash_confirm_is_rewritten_and_a_cut_one_is_declined(monkeypatch):
    monkeypatch.setattr(adapter, "remember", lambda *_args: None)
    from gateway.relay.egress import declined_send

    _register()
    cfg = _config()
    cfg.extra["MESHTASTIC_CHUNK_BYTES"] = "64"
    created = platform_registry.create_adapter("radio-dm-gateway", cfg)
    iface = _Iface()
    created._iface = iface
    created._budget = _NoWaitBudget()
    box = {}

    async def handler(_event):
        box["fit"] = await created.send_slash_confirm(
            "!aabbccdd", "/new", "⚠️ **Confirm /new**\n\nAlways Approve\n/cancel", "s", "1",
        )
        box["cut"] = await created.send_slash_confirm(
            "!aabbccdd", "/" + ("n" * 80), "long title", "s", "2",
        )
        return None

    async def run():
        created.set_message_handler(handler)
        await created._accept({"node": "!aabbccdd", "text": "go", "channel": False, "packet_id": "sc1"})
        await _finish_turns(created)

    asyncio.run(run())
    assert box["fit"].success is True
    sent = [text for _dest, text, _ack in iface.sent]
    assert sent[0] == "/new discards history. /approve or /cancel. always refused."
    assert len(sent[0].encode("utf-8")) <= 64
    assert "Always Approve" not in " ".join(sent)
    assert box["cut"].success is False
    assert declined_send(box["cut"]) is True
    assert sent[-1] == adapter.CONFIRM_CUT_NOTE


def test_unreadable_approval_words_refuse_a_short_reply(monkeypatch):
    monkeypatch.setattr(adapter, "remember", lambda *_args: None)

    def boom(_key):
        raise RuntimeError("word list down")

    import gateway.run_busy as run_busy
    monkeypatch.setattr(run_busy, "approval_input_words", boom, raising=False)
    _register()
    created = platform_registry.create_adapter("radio-dm-gateway", _config())
    iface = _Iface()
    created._iface = iface
    created._budget = _NoWaitBudget()
    handed = []

    async def handler(event):
        handed.append(event.text)
        return None

    async def run():
        created.set_message_handler(handler)
        await created._accept({"node": "!aabbccdd", "text": "hello", "channel": False, "packet_id": "w1"})
        await _finish_turns(created)
        await created._accept({
            "node": "!aabbccdd",
            "text": "this reply is longer than forty characters easily",
            "channel": False,
            "packet_id": "w2",
        })
        await _finish_turns(created)

    asyncio.run(run())
    assert handed == ["this reply is longer than forty characters easily"]
    assert [text for _dest, text, _ack in iface.sent] == [adapter.APPROVAL_WORD_REFUSAL]


def test_a_surrogate_in_the_command_declines_and_puts_nothing_on_the_radio(monkeypatch):
    """A lone surrogate used to raise out of the approval mouth.

    Hermes then sent the question as plain text, this adapter refused that text,
    Hermes did not read the refusal, and /approve still ran the command.
    """
    from gateway.platforms.base import ExecApprovalPrompt
    from gateway.relay.egress import declined_send

    monkeypatch.setattr(adapter, "remember", lambda *_args: None)
    _register()
    created = platform_registry.create_adapter("radio-dm-gateway", _config())
    iface = _Iface()
    created._iface = iface
    created._budget = _NoWaitBudget()
    command = "rm -r /tmp/rb-sur-missing\udc80x"
    box = {}

    async def handler(_event):
        _hold_pending("s-surrogate", command)
        box["result"] = await created._send_exec_approval_prompt(ExecApprovalPrompt(
            chat_id="!aabbccdd",
            session_key="s-surrogate",
            text=command,
            actions=[("Allow Once", "once", "primary")],
            command=command,
            description="cleanup",
            smart_denied=False,
        ))
        _clear_pending("s-surrogate")
        return None

    async def run():
        created.set_message_handler(handler)
        await created._accept({"node": "!aabbccdd", "text": "go", "channel": False, "packet_id": "sur1"})
        await _finish_turns(created)

    asyncio.run(run())
    assert box["result"].success is False
    assert declined_send(box["result"]) is True
    assert iface.sent == []


def test_a_broken_approval_mouth_is_still_a_decline(monkeypatch):
    """Whatever raises inside the mouth, Hermes must be told the question was not sent."""
    from gateway.platforms.base import ExecApprovalPrompt
    from gateway.relay.egress import declined_send

    monkeypatch.setattr(adapter, "remember", lambda *_args: None)

    def boom(*_args, **_kwargs):
        raise RuntimeError("approval line builder down")

    monkeypatch.setattr(adapter, "radio_approval_line", boom)
    _register()
    created = platform_registry.create_adapter("radio-dm-gateway", _config())
    iface = _Iface()
    created._iface = iface
    created._budget = _NoWaitBudget()
    box = {}

    async def handler(_event):
        _hold_pending("s-boom", "echo hi")
        box["result"] = await created._send_exec_approval_prompt(ExecApprovalPrompt(
            chat_id="!aabbccdd",
            session_key="s-boom",
            text="echo hi",
            actions=[("Allow Once", "once", "primary")],
            command="echo hi",
            description="list",
            smart_denied=False,
        ))
        _clear_pending("s-boom")
        return None

    async def run():
        created.set_message_handler(handler)
        await created._accept({"node": "!aabbccdd", "text": "go", "channel": False, "packet_id": "boom1"})
        await _finish_turns(created)

    asyncio.run(run())
    assert box["result"].success is False
    assert declined_send(box["result"]) is True
    assert iface.sent == []


def test_a_send_text_stuck_inside_the_library_declines_the_approval(monkeypatch):
    """The 12 second budget covers only this plugin's own waits.

    sendText has its own: up to 30 seconds for the link, and a close-sleep-reopen
    on a socket error. A send stuck in there must come back as a decline before
    Hermes stops watching at 15 seconds, and must not leave a second send free to
    call the library at the same time.
    """
    from gateway.platforms.base import ExecApprovalPrompt
    from gateway.relay.egress import declined_send

    monkeypatch.setattr(adapter, "remember", lambda *_args: None)
    monkeypatch.setattr(adapter, "APPROVAL_SEND_SECONDS", 0.3)
    _register()
    created = platform_registry.create_adapter("radio-dm-gateway", _config())

    class _StuckIface:
        def __init__(self):
            self.sent = []
            self.inside = threading.Event()
            self.release = threading.Event()

        def sendText(self, text, destinationId, wantAck=False):
            self.inside.set()
            # The real block is _waitConnected's 30 seconds; the test lets go early.
            self.release.wait(30)
            self.sent.append((destinationId, text, wantAck))
            return {"id": 1}

    iface = _StuckIface()
    created._iface = iface
    created._budget = _NoWaitBudget()
    box = {}

    async def handler(_event):
        _hold_pending("s-stuck", "echo hi")
        started = time.monotonic()
        box["result"] = await created._send_exec_approval_prompt(ExecApprovalPrompt(
            chat_id="!aabbccdd",
            session_key="s-stuck",
            text="echo hi",
            actions=[("Allow Once", "once", "primary")],
            command="echo hi",
            description="list",
            smart_denied=False,
        ))
        box["elapsed"] = time.monotonic() - started
        box["still_inside"] = iface.inside.is_set() and not iface.release.is_set()
        box["slot_held"] = created._send_lock.locked()
        box["orphan"] = created._orphan_send is not None
        iface.release.set()
        orphan = created._orphan_send
        if orphan is not None:
            await asyncio.wait_for(asyncio.shield(orphan), timeout=10)
        for _ in range(50):
            if not created._send_lock.locked():
                break
            await asyncio.sleep(0.05)
        box["slot_free_after"] = not created._send_lock.locked()
        _clear_pending("s-stuck")
        return None

    async def run():
        created.set_message_handler(handler)
        await created._accept({"node": "!aabbccdd", "text": "go", "channel": False, "packet_id": "stuck1"})
        await _finish_turns(created)

    asyncio.run(run())
    assert box["result"].success is False
    assert declined_send(box["result"]) is True
    assert box["elapsed"] < 5
    assert box["still_inside"] is True
    assert box["slot_held"] is True
    assert box["orphan"] is True
    assert box["slot_free_after"] is True


def test_a_nodes_file_that_cannot_be_read_still_answers_the_message(monkeypatch, tmp_path):
    """Bytes that are not UTF-8, and JSON nested too deep, stop recording only."""
    import store

    monkeypatch.setattr(adapter, "remember", store.remember)
    monkeypatch.setattr(store, "data_dir", lambda: tmp_path)
    monkeypatch.setattr(store, "_corrupt_warned", False)
    _register()
    created = platform_registry.create_adapter("radio-dm-gateway", _config())
    iface = _Iface()
    created._iface = iface
    created._budget = _NoWaitBudget()
    path = tmp_path / "nodes.json"
    handed = []

    async def handler(event):
        handed.append(event.text)
        return "answered"

    async def run():
        created.set_message_handler(handler)
        await created._accept({"node": "!aabbccdd", "text": "hello", "channel": False, "packet_id": "bad1"})
        await _finish_turns(created)

    # The depth that overflows differs per interpreter: 1000 raises on 3.9, 20000 on 3.12,
    # 100000 on 3.14. 200000 raises on all three.
    for raw in (b'[{"node":"\xff"}]', (b"[" * 200000) + (b"]" * 200000)):
        handed.clear()
        iface.sent.clear()
        store._corrupt_warned = False
        path.write_bytes(raw)
        asyncio.run(run())
        assert handed == ["hello"], raw[:16]
        assert [text for _dest, text, _ack in iface.sent] == ["answered"], raw[:16]
        assert path.read_bytes() == raw
        created._seen = []
