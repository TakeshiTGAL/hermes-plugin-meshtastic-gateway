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
        CUT_MARK,
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
        plaintext_restart,
        radio_approval_text,
        radio_command_refusal,
        widens_approval,
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
        CUT_MARK,
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
        plaintext_restart,
        radio_approval_text,
        radio_command_refusal,
        widens_approval,
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
PLATFORM = "radio-dm-gateway"
# Hermes waits 15 seconds for an approval question to be sent. This send must end before that.
APPROVAL_SEND_SECONDS = 12
APPROVAL_SCOPE_REFUSAL = (
    "Over the radio, approval is one time only. always and session are refused. "
    "Send /approve to allow this once, or /deny."
)
APPROVAL_WORD_REFUSAL = APPROVAL_SCOPE_REFUSAL + " To send the word itself, add more words."
COMMAND_REFUSAL = (
    "Over the radio, only these commands are accepted: /approve (once only), /deny, /stop, "
    "/new, /reset, /help, /status, /whoami, /retry, /undo."
)


def _text_as_hermes_reads_it(event: MessageEvent, text: str) -> str:
    """The text after Hermes' own plain-text rewrite (for example "restart gateway" becomes /restart).

    The allowlist must see what Hermes will see. If the rewrite cannot be loaded, the same
    restart phrases are read as /restart here, so the check stays closed.
    """
    if plaintext_restart(text):
        return "/restart"
    try:
        from gateway.platforms.base import coerce_plaintext_gateway_command
    except Exception:
        return text
    coerce_plaintext_gateway_command(event)
    return getattr(event, "text", None) or text


def _approval_scope_phrases() -> tuple[str, ...]:
    """Hermes' own words for session and always approval, in the active language, when Hermes exposes them."""
    try:
        from gateway.run_busy import approval_input_words
    except Exception:
        return ()
    words: list[str] = []
    for key in ("always", "session", "confirm_always"):
        try:
            words.extend(approval_input_words(key))
        except Exception:
            continue
    return tuple(words)


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
        self._turn_tasks: dict[asyncio.Task, str | None] = {}
        self._turn_nodes: dict[str, int] = {}
        # The handler task only, and only until the handler returns. Hermes sends the final reply after that.
        self._in_handler: set[asyncio.Task] = set()
        # One send at a time, so chunks of two replies never interleave and the gap holds.
        self._send_lock = asyncio.Lock()
        # Set by the library's connection-lost event. A send then fails at once instead of
        # waiting up to 30 seconds inside the library for a link that is gone.
        self._radio_lost = False

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
        """Stop Hermes from sending this reply again inside the turn.

        A chunk already counted as sent is final. A library error during sendText is also final:
        the class name would otherwise match Hermes's retry list (ConnectionResetError and the rest)
        or fall through to a plain-text resend of the whole reply. Either path can put the text on
        the radio twice. The retries about 30 seconds and about 2.5 minutes later are a different path;
        this adapter refuses those because they are outside the turn.
        """
        raw = getattr(result, "raw_response", None)
        if not isinstance(raw, dict):
            return False
        if raw.get("may_have_reached") is True:
            return True
        sent = raw.get("chunks_sent")
        return isinstance(sent, int) and not isinstance(sent, bool) and sent > 0

    def _budget_for(self) -> SendBudget:
        if self._budget is None:
            self._budget = SendBudget(
                min_gap_seconds(_env(self.config, "MESHTASTIC_MIN_GAP_SECONDS") or 20),
                max_per_hour(_env(self.config, "MESHTASTIC_MAX_PER_HOUR") or 12),
            )
        return self._budget

    def set_message_handler(self, handler) -> None:
        """Remember the task that runs the turn. Nested tasks must not transmit.

        Hermes queues a message that arrives during startup and later calls handle_message again
        from a task that did not copy this adapter's reply context. The event still names the node.
        """

        async def wrapped(event):
            task = asyncio.current_task()
            if task is not None and task not in self._send_tasks:
                node = _reply_node.get()
                if not node and self._event_from_this_radio(event):
                    source = getattr(event, "source", None)
                    recovered = node_id(getattr(source, "chat_id", None))
                    if recovered:
                        _reply_node.set(recovered)
                        node = recovered
                packet_id = getattr(event, "message_id", None)
                self._send_tasks.add(task)
                self._turn_tasks[task] = packet_id if isinstance(packet_id, str) else None
                self._hold_turn_node(node)
                task.add_done_callback(lambda done, node=node: self._end_turn_task(done, node))
            if task is not None:
                self._in_handler.add(task)
            try:
                return await handler(event)
            finally:
                if task is not None:
                    self._in_handler.discard(task)

        super().set_message_handler(wrapped)

    def _event_from_this_radio(self, event) -> bool:
        """True only for an event this adapter built. An unstamped event is not a radio reply."""
        raw = getattr(event, "raw_message", None)
        if isinstance(raw, dict) and raw.get("radio_dm_origin") is True:
            return True
        return getattr(event, "_radio_dm_origin", False) is True

    def _interim_on_this_task(self, metadata: dict | None) -> bool:
        """Text sent before the handler returns. An approval question is not interim text."""
        task = asyncio.current_task()
        if task is None or task not in self._in_handler:
            return False
        return (metadata or {}).get("is_approval_prompt") is not True

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
        self._turn_tasks.pop(task, None)
        self._release_turn_node(node)

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
        """A reply that never called sendText is not treated as already seen. A send that may have reached the radio keeps its packet id."""
        task = asyncio.current_task()
        if task not in self._turn_tasks or result.success:
            return result
        raw = getattr(result, "raw_response", None)
        if isinstance(raw, dict) and raw.get("may_have_reached") is True:
            return result
        sent = raw.get("chunks_sent") if isinstance(raw, dict) else 0
        if isinstance(sent, int) and not isinstance(sent, bool) and sent > 0:
            return result
        self._forget_seen(self._turn_tasks.get(task))
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
        self._radio_lost = False
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
        self._radio_lost = True
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
                raw_message={"node": item["node"], "channel": item["channel"], "radio_dm_origin": True},
                message_id=packet_id,
            )
            # Survives the startup queue: Hermes replays this same event from another task.
            event._radio_dm_origin = True
            token = _reply_node.set(item["node"])
        except Exception:
            self._forget_seen(packet_id)
            return
        task = asyncio.current_task()
        if task is not None:
            self._send_tasks.add(task)
        self._hold_turn_node(item["node"])
        radio = _from_radio.set(True)
        try:
            refusal = None
            kind = radio_command_refusal(_text_as_hermes_reads_it(event, item["text"]))
            if kind == "approve":
                # Hermes would keep a session or always approval. A radio node may approve once only.
                refusal = APPROVAL_SCOPE_REFUSAL
            elif kind == "command":
                # /yolo, /approvals and the rest can change approval for the session or the whole profile.
                refusal = COMMAND_REFUSAL
            elif widens_approval(item["text"], _approval_scope_phrases()):
                refusal = APPROVAL_WORD_REFUSAL
            if refusal is not None:
                await self.send(item["node"], refusal)
                return
            await self.handle_message(event)
        finally:
            _from_radio.reset(radio)
            _reply_node.reset(token)
            if task is not None:
                self._send_tasks.discard(task)
            self._release_turn_node(item["node"])

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
        if self._interim_on_this_task(metadata):
            # Do not forget the packet id, and do not spend the send budget. The final reply is still owed.
            return SendResult(
                success=False,
                error="Interim radio text is refused. Nothing was sent.",
                retryable=False,
            )
        if self._iface is None or not self._radio_up():
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
        approval = (metadata or {}).get("is_approval_prompt") is True
        if approval:
            # One short chunk: the long Hermes question would need several gaps, and only once is accepted here.
            chunks, cut = chunk_text(radio_approval_text(content), size, 1)
            if cut:
                # Nobody may approve a command they could not read in full. Hermes then fails closed
                # (notify_failed or the approval timeout).
                return SendResult(
                    success=False,
                    retryable=False,
                    error="approval question does not fit one radio chunk",
                )
        else:
            chunks, cut = chunk_text(content or "", size, limit)
        if not chunks:
            return self._forget_failed_reply(
                SendResult(success=False, error="The reply is empty. Nothing was sent.", retryable=False)
            )
        started = time.monotonic()
        if approval:
            # Hermes stops waiting for an approval send after 15 seconds and does not cancel it.
            # A send it counted as failed must never go out later, so the wait for the lock is capped.
            try:
                await asyncio.wait_for(self._send_lock.acquire(), timeout=APPROVAL_SEND_SECONDS)
            except asyncio.TimeoutError:
                return SendResult(
                    success=False,
                    error=(
                        "The approval question was not sent. Another radio send is still going out, "
                        "and Hermes stops waiting for this send after 15 seconds."
                    ),
                    retryable=False,
                )
        else:
            await self._send_lock.acquire()
        try:
            return await self._send_locked(dest, chunks, cut, approval, started)
        finally:
            self._send_lock.release()

    def _radio_up(self) -> bool:
        """False after the library reported the link lost, or while its isConnected flag is down."""
        if self._radio_lost:
            return False
        flag = getattr(self._iface, "isConnected", None)
        if flag is None:
            return True
        is_set = getattr(flag, "is_set", None)
        if callable(is_set):
            return bool(is_set())
        return bool(flag)

    async def _send_locked(
        self, dest: str, chunks: list[str], cut: bool, approval: bool, started: float
    ) -> SendResult:
        if self._iface is None or not self._radio_up():
            return self._forget_failed_reply(
                SendResult(success=False, error="The radio is not connected. Nothing was sent.", retryable=False)
            )
        budget = self._budget_for()
        now = time.time()
        refusal, wait = budget.plan(len(chunks), now)
        if refusal:
            return self._forget_failed_reply(SendResult(success=False, error=refusal, retryable=False))
        if approval and (time.monotonic() - started) + wait > APPROVAL_SEND_SECONDS:
            # Not started: it could not go out before Hermes stops waiting.
            return SendResult(
                success=False,
                error=(
                    f"The approval question was not sent. The radio gap needs {int(wait + 0.999)} more seconds, "
                    "and Hermes stops waiting for this send after 15 seconds."
                ),
                retryable=False,
            )
        if wait:
            await asyncio.sleep(wait)
        sent: list[str] = []
        for index, chunk in enumerate(chunks):
            if index:
                await asyncio.sleep(budget.gap)
            iface = self._iface
            if iface is None or not self._radio_up():
                return self._forget_failed_reply(SendResult(
                    success=False,
                    error=(
                        f"Sent {len(sent)} of {len(chunks)}. The radio is not connected. "
                        "The part already sent is not sent again."
                    ),
                    message_id=sent[-1] if sent else None,
                    retryable=False,
                    raw_response={"chunks_sent": len(sent)},
                ))
            try:
                # Off the event loop: the library can block while the link is down.
                packet_id = await asyncio.to_thread(send_text, iface, dest, chunk)
            except Exception:
                # The class name is omitted on purpose. Hermes retries when the error text contains
                # connectionreset, connectionerror, and the other names in _RETRYABLE_ERROR_PATTERNS,
                # and otherwise sends the whole reply again as plain text.
                return self._forget_failed_reply(SendResult(
                    success=False,
                    error=(
                        f"Sent {len(sent)} of {len(chunks)}. The library did not finish this chunk, "
                        "so this attempt may have reached the radio. It is not sent again."
                    ),
                    message_id=sent[-1] if sent else None,
                    retryable=False,
                    raw_response={"chunks_sent": len(sent), "may_have_reached": True},
                ))
            budget.mark(time.time())
            sent.append(packet_id)
            remember(dest, "out", len(chunk.encode("utf-8")), time.time())
        note = None
        if cut and sent and chunks[-1].endswith(CUT_MARK):
            note = "The reply was cut. The chunks that fit were sent, and the last one ends with [cut]."
        elif cut:
            note = "The reply was cut. The chunk size is too small to carry [cut], so the last chunk has no mark."
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
        label="Radio DM",
        adapter_factory=lambda cfg: MeshtasticAdapter(cfg),
        check_fn=check_requirements,
        validate_config=validate_config,
        required_env=["MESHTASTIC_URL"],
        install_hint=(
            "meshtastic>=2.7.9,<3 (GPL-3.0-only) is declared in python_dependencies, so Hermes installs it "
            "with the plugin. It is not vendored."
        ),
        allowed_users_env="MESHTASTIC_ALLOWED_NODES",
        max_message_length=800,
        emoji="📻",
        allow_update_command=False,
        platform_hint=(
            "You are answering over a Meshtastic® radio. Keep replies short. "
            "An empty node allowlist means you answer nobody. Do not invent a node id."
        ),
    )
