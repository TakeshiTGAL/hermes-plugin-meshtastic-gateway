"""Hermes gateway adapter. Imported from register(), not from the policy tests."""
from __future__ import annotations

import asyncio
import contextvars
import time
from typing import Any

from gateway.config import Platform, PlatformConfig
from gateway.platforms.base import BasePlatformAdapter, SendResult
from gateway.platforms.event import MessageEvent, MessageType

if __package__:
    from .policy import (
        SendBudget,
        allowlist_problem,
        chunk_bytes,
        chunk_text,
        classify,
        is_allowed,
        max_chunks,
        max_per_hour,
        min_gap_seconds,
        node_id,
        parse_allowlist,
        parse_url,
    )
    from .radio import (
        close_interface,
        local_node,
        open_interface,
        send_text,
        subscribe,
        subscribe_lost,
        unsubscribe,
    )
    from .store import remember
else:
    from policy import (
        SendBudget,
        allowlist_problem,
        chunk_bytes,
        chunk_text,
        classify,
        is_allowed,
        max_chunks,
        max_per_hour,
        min_gap_seconds,
        node_id,
        parse_allowlist,
        parse_url,
    )
    from radio import (
        close_interface,
        local_node,
        open_interface,
        send_text,
        subscribe,
        subscribe_lost,
        unsubscribe,
    )
    from store import remember

_reply_node: contextvars.ContextVar[str] = contextvars.ContextVar("meshtastic_reply_node", default="")
_from_radio: contextvars.ContextVar[bool] = contextvars.ContextVar("meshtastic_from_radio", default=False)
PLATFORM = "meshtastic-gateway"


def _env(config: PlatformConfig, name: str) -> str:
    extra = getattr(config, "extra", None) or {}
    if isinstance(extra, dict) and extra.get(name):
        return str(extra[name])
    import os
    return os.environ.get(name, "")


class MeshtasticAdapter(BasePlatformAdapter):
    """One radio. Empty allowlist answers nobody. Channel packets are never answered."""

    _dm_policy = "allowlist"
    _group_policy = "disabled"
    MAX_MESSAGE_LENGTH = 800

    def __init__(self, config: PlatformConfig, platform: Platform | None = None):
        super().__init__(config, platform or Platform(PLATFORM))
        self._iface = None
        self._seen: list[str] = []
        self._budget: SendBudget | None = None
        self._loop: asyncio.AbstractEventLoop | None = None
        self._listener = None
        self._lost = None
        self._send_tasks: set[asyncio.Task] = set()
        self._turn_task: asyncio.Task | None = None
        self._turn_nodes: dict[str, int] = {}
        self._turn_packet_id: str | None = None

    @property
    def enforces_own_access_policy(self) -> bool:
        return True

    def _is_dm_allowed(self, user_id: str) -> bool:
        return is_allowed(user_id, self._allowlist())

    def _allowlist(self):
        return parse_allowlist(_env(self.config, "MESHTASTIC_ALLOWED_NODES"))

    def resolved_allowlist_user_ids(self) -> set[str]:
        """Canonical '!aabbccdd' ids. Hermes compares the env text as written, so decimal and '!AABBCCDD' are added here."""
        return set(self._allowlist())

    def _send_retry_is_final(self, result: SendResult) -> bool:
        """A chunk already on the radio must not be sent again. Hermes retries when the error names ConnectionError, and otherwise sends the whole reply as plain text."""
        raw = getattr(result, "raw_response", None)
        sent = raw.get("chunks_sent") if isinstance(raw, dict) else None
        return isinstance(sent, int) and not isinstance(sent, bool) and sent > 0

    def _budget_for(self) -> SendBudget:
        if self._budget is None:
            self._budget = SendBudget(
                min_gap_seconds(_env(self.config, "MESHTASTIC_MIN_GAP_SECONDS") or 20),
                max_per_hour(_env(self.config, "MESHTASTIC_MAX_PER_HOUR") or 12),
            )
        return self._budget

    def set_message_handler(self, handler) -> None:
        """Remember the task that runs the turn. Nested tasks must not transmit."""

        async def wrapped(event):
            task = asyncio.current_task()
            if task is not None and task not in self._send_tasks:
                node = _reply_node.get()
                self._send_tasks.add(task)
                self._turn_task = task
                self._hold_turn_node(node)
                task.add_done_callback(lambda done, node=node: self._end_turn_task(done, node))
            return await handler(event)

        super().set_message_handler(wrapped)

    def _hold_turn_node(self, node: str) -> None:
        if node:
            self._turn_nodes[node] = self._turn_nodes.get(node, 0) + 1

    def _release_turn_node(self, node: str) -> None:
        count = self._turn_nodes.get(node, 0) - 1
        if count > 0:
            self._turn_nodes[node] = count
        else:
            self._turn_nodes.pop(node, None)

    def _end_turn_task(self, task: asyncio.Task, node: str = "") -> None:
        self._send_tasks.discard(task)
        self._release_turn_node(node)
        if self._turn_task is task:
            self._turn_task = None
            self._turn_packet_id = None

    def _may_send(self, dest: str, metadata: dict | None) -> bool:
        """The turn task may send the final reply. An approval question for that same node may also be sent. A nested task may not."""
        task = asyncio.current_task()
        if self._send_tasks:
            if task in self._send_tasks and _reply_node.get() == dest:
                return True
            meta = metadata or {}
            return meta.get("is_approval_prompt") is True and dest in self._turn_nodes
        return False

    def _forget_failed_reply(self, result: SendResult) -> SendResult:
        """A reply that never reached the radio is not treated as already seen."""
        if asyncio.current_task() is not self._turn_task or result.success:
            return result
        raw = getattr(result, "raw_response", None)
        sent = raw.get("chunks_sent") if isinstance(raw, dict) else 0
        if isinstance(sent, int) and not isinstance(sent, bool) and sent > 0:
            return result
        self._forget_seen(self._turn_packet_id)
        return result

    async def get_chat_info(self, chat_id: str) -> dict[str, Any]:
        return {"name": str(chat_id), "type": "dm"}

    async def connect(self, *, is_reconnect: bool = False) -> bool:
        spec = parse_url(_env(self.config, "MESHTASTIC_URL"))
        problem = allowlist_problem(_env(self.config, "MESHTASTIC_ALLOWED_NODES"))
        if problem:
            raise ValueError(problem)
        self._loop = asyncio.get_running_loop()
        if self._iface is not None or self._listener is not None or self._lost is not None:
            await self.disconnect()
        try:
            self._iface = await asyncio.to_thread(open_interface, spec)
        except Exception as exc:
            self._iface = None
            detail = str(exc).strip() or exc.__class__.__name__
            if "Nothing is connected" not in detail:
                detail = f"{detail} Nothing is connected."
            raise ValueError(detail) from exc
        try:
            self._listener = subscribe(self._iface, self._on_packet)
            self._lost = subscribe_lost(self._iface, self._on_connection_lost)
        except Exception as exc:
            await self.disconnect()
            raise ValueError(
                f"The radio opened but text could not be subscribed ({exc.__class__.__name__}). Nothing is connected."
            ) from exc
        if local_node(self._iface) is None:
            await self.disconnect()
            raise ValueError(
                "The radio opened, but its own node number could not be read. "
                "This plugin will not answer. Nothing stays connected. "
                "Check that the radio reported its node number."
            )
        self._running = True
        return True

    async def disconnect(self) -> None:
        self._running = False
        try:
            unsubscribe(self._listener)
            unsubscribe(self._lost, "meshtastic.connection.lost")
        finally:
            self._listener = None
            self._lost = None
            iface = self._iface
            self._iface = None
            if iface is not None:
                close_interface(iface)

    def _on_connection_lost(self) -> None:
        if not self._running:
            return
        self._set_fatal_error(
            "radio_lost",
            "The radio stopped. Hermes can connect again.",
            retryable=True,
        )
        loop = self._loop
        if loop is None or not loop.is_running():
            return
        try:
            asyncio.run_coroutine_threadsafe(self._notify_fatal_error(), loop)
        except RuntimeError:
            return

    def _on_packet(self, packet: dict) -> None:
        item = classify(
            packet,
            self._allowlist(),
            my_node=local_node(self._iface) if self._iface is not None else None,
        )
        if item is None:
            return
        packet_id = item.get("packet_id")
        if packet_id and packet_id in self._seen:
            return
        loop = self._loop
        if loop is None or not loop.is_running():
            return
        if packet_id:
            self._seen.append(packet_id)
            del self._seen[:-100]
        try:
            asyncio.run_coroutine_threadsafe(self._accept(item), loop)
        except RuntimeError:
            self._forget_seen(packet_id)

    def _forget_seen(self, packet_id: str | None) -> None:
        if packet_id:
            self._seen = [seen for seen in self._seen if seen != packet_id]

    async def handle_message(self, event: MessageEvent) -> None:
        if _from_radio.get():
            source = getattr(event, "source", None)
            node = getattr(source, "chat_id", None)
            if node:
                text = getattr(event, "text", "") or ""
                remember(str(node), "in", len(str(text).encode("utf-8")), time.time())
        await super().handle_message(event)

    async def _accept(self, item: dict) -> None:
        packet_id = item.get("packet_id")
        try:
            source = self.build_source(
                chat_id=item["node"],
                chat_type="dm",
                user_id=item["node"],
                user_name=item["node"],
                message_id=packet_id,
            )
            event = MessageEvent(
                text=item["text"],
                message_type=MessageType.TEXT,
                source=source,
                raw_message={"node": item["node"], "channel": item["channel"]},
                message_id=packet_id,
            )
            token = _reply_node.set(item["node"])
        except Exception:
            self._forget_seen(packet_id)
            return
        task = asyncio.current_task()
        if task is not None:
            self._send_tasks.add(task)
        starting = not self._turn_nodes
        if starting:
            self._turn_packet_id = packet_id if isinstance(packet_id, str) else None
        self._hold_turn_node(item["node"])
        radio = _from_radio.set(True)
        try:
            await self.handle_message(event)
        finally:
            _from_radio.reset(radio)
            _reply_node.reset(token)
            if task is not None:
                self._send_tasks.discard(task)
            self._release_turn_node(item["node"])
            if starting and self._turn_task is None and not self._background_tasks:
                self._turn_packet_id = None

    async def send(
        self,
        chat_id: str,
        content: str,
        reply_to: str | None = None,
        metadata: dict | None = None,
    ) -> SendResult:
        dest = node_id(chat_id)
        if dest is None or not is_allowed(dest, self._allowlist()):
            return SendResult(success=False, error="This node is not on the allowlist. Nothing was sent.", retryable=False)
        if self._iface is None:
            return self._forget_failed_reply(
                SendResult(success=False, error="The radio is not connected. Nothing was sent.", retryable=False)
            )
        if not self._may_send(dest, metadata):
            return SendResult(
                success=False,
                error="Unsolicited radio sends are refused. Nothing was sent.",
                retryable=False,
            )
        size = chunk_bytes(_env(self.config, "MESHTASTIC_CHUNK_BYTES") or 200)
        limit = max_chunks(_env(self.config, "MESHTASTIC_MAX_CHUNKS") or 4)
        chunks, cut = chunk_text(content or "", size, limit)
        if not chunks:
            return self._forget_failed_reply(
                SendResult(success=False, error="The reply is empty. Nothing was sent.", retryable=False)
            )
        budget = self._budget_for()
        now = time.time()
        refusal, wait = budget.plan(len(chunks), now)
        if refusal:
            return self._forget_failed_reply(SendResult(success=False, error=refusal, retryable=False))
        if wait:
            await asyncio.sleep(wait)
        sent: list[str] = []
        for index, chunk in enumerate(chunks):
            if index:
                await asyncio.sleep(budget.gap)
            try:
                packet_id = send_text(self._iface, dest, chunk)
            except Exception as exc:
                return self._forget_failed_reply(SendResult(
                    success=False,
                    error=(
                        f"Sent {len(sent)} of {len(chunks)}. The radio stopped the rest: {exc.__class__.__name__}. "
                        "Check the radio link. The part already sent is not sent again."
                    ),
                    message_id=sent[-1] if sent else None,
                    retryable=False,
                    raw_response={"chunks_sent": len(sent)},
                ))
            budget.mark(time.time())
            sent.append(packet_id)
            remember(dest, "out", len(chunk.encode("utf-8")), time.time())
        note = None
        if cut:
            note = "The reply was cut. The chunks that fit were sent, and the last one ends with [cut]."
        return SendResult(success=True, message_id=sent[-1] if sent else "", error=note, retryable=False)


def check_requirements() -> bool:
    try:
        import meshtastic.tcp_interface  # noqa: F401
    except Exception:
        return False
    return True


def validate_config(config: PlatformConfig) -> bool:
    parse_url(_env(config, "MESHTASTIC_URL"))
    problem = allowlist_problem(_env(config, "MESHTASTIC_ALLOWED_NODES"))
    if problem:
        raise ValueError(problem)
    return True


def register(ctx) -> None:
    ctx.register_platform(
        name=PLATFORM,
        label="Meshtastic",
        adapter_factory=lambda cfg: MeshtasticAdapter(cfg),
        check_fn=check_requirements,
        validate_config=validate_config,
        required_env=["MESHTASTIC_URL"],
        install_hint="Install meshtastic into the Hermes virtualenv (GPL-3.0-only). This plugin does not vendor it.",
        allowed_users_env="MESHTASTIC_ALLOWED_NODES",
        max_message_length=800,
        emoji="📻",
        allow_update_command=False,
        platform_hint=(
            "You are answering over a Meshtastic radio. Keep replies short. "
            "An empty node allowlist means you answer nobody. Do not invent a node id."
        ),
    )
