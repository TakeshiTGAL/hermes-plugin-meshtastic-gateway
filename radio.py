"""Open one Meshtastic® interface and send already-checked text chunks.

The meshtastic package is imported only when a tcp connection is opened.
This plugin opens tcp only. Serial and BLE cannot be stopped once started,
so those URLs are refused before the library is called. The first socket
waits at most 20 seconds to connect, then the timeout is cleared so a quiet
radio does not stop the reader. This plugin does not replace
socket.create_connection, and it does not limit the library's later reconnects.
"""
from __future__ import annotations

import socket
import time
from typing import Any, Callable

OPEN_SECONDS = 20
# The library waits forever while the radio's transmit queue reports free == 0.
# Stop before sendText, so a full queue is "nothing was sent", not a maybe.
SEND_QUEUE_SECONDS = 12


class RadioNotSent(Exception):
    """sendText was not called. The radio queue had no free slot in time."""


def _open_tcp(spec: dict) -> Any:
    from meshtastic.stream_interface import StreamInterface
    from meshtastic.tcp_interface import TCPInterface

    sock = socket.create_connection((spec["host"], int(spec["port"])), timeout=OPEN_SECONDS)
    try:
        sock.settimeout(None)
        iface = TCPInterface(
            hostname=spec["host"],
            portNumber=int(spec["port"]),
            connectNow=False,
        )
        iface.socket = sock
        # TCPInterface.connect() would open a second socket with no timeout.
        StreamInterface.connect(iface)
    except Exception:
        try:
            sock.close()
        except Exception:
            pass
        raise
    if getattr(iface, "socket", None) is not None:
        iface.socket.settimeout(None)
    return iface


def open_interface(spec: dict) -> Any:
    kind = spec.get("kind")
    if kind in {"serial", "ble"}:
        raise ValueError(
            "This plugin opens tcp only. A serial or BLE open cannot be stopped, so that URL is refused. "
            "Use tcp://. Nothing is connected."
        )
    if kind == "tcp":
        return _open_tcp(spec)
    raise ValueError("MESHTASTIC_URL kind is not tcp, serial, or ble. Nothing is connected.")


def close_interface(iface: Any) -> None:
    closer = getattr(iface, "close", None)
    if callable(closer):
        closer()


def local_node(iface: Any) -> str | None:
    info = getattr(iface, "myInfo", None)
    number = getattr(info, "my_node_num", None)
    if number is None and isinstance(info, dict):
        number = info.get("myNodeNum") or info.get("my_node_num")
    if __package__:
        from .policy import node_id
    else:
        from policy import node_id

    return node_id(number)


def _tx_slot_is_free(iface: Any) -> bool | None:
    """True when sendText will not wait on the queue. False when it would. None if unreadable."""
    status = getattr(iface, "queueStatus", None)
    if status is None:
        return True
    free = getattr(status, "free", None)
    # bool is an int. A flag is not a free-slot count.
    if isinstance(free, bool) or not isinstance(free, int):
        return None
    return free > 0


def send_text(iface: Any, node: str, text: str) -> str:
    """One unreliable text. wantAck stays false so this call does not ask for retries.

    A full transmit queue raises RadioNotSent before sendText. The socket was not written.
    """
    deadline = time.monotonic() + SEND_QUEUE_SECONDS
    while True:
        slot = _tx_slot_is_free(iface)
        if slot is True:
            break
        if slot is None or time.monotonic() >= deadline:
            raise RadioNotSent("The radio queue is full. Nothing was sent.")
        time.sleep(min(0.5, max(0.0, deadline - time.monotonic())))
    packet = iface.sendText(text, destinationId=node, wantAck=False)
    packet_id = getattr(packet, "id", None)
    if packet_id is None and isinstance(packet, dict):
        packet_id = packet.get("id")
    return "" if packet_id is None else str(packet_id)


def subscribe(iface: Any, callback: Callable[[dict], None]) -> Callable[..., None]:
    from pubsub import pub

    def _recv(packet, interface=None, **_kwargs):
        if interface is not None and interface is not iface:
            return
        if isinstance(packet, dict):
            callback(packet)

    # One topic. Subscribing to the parent and the text child delivers the same packet twice.
    pub.subscribe(_recv, "meshtastic.receive.text")
    return _recv


def subscribe_lost(iface: Any, callback: Callable[[], None]) -> Callable[..., None]:
    from pubsub import pub

    def _lost(interface=None, **_kwargs):
        if interface is not None and interface is not iface:
            return
        callback()

    pub.subscribe(_lost, "meshtastic.connection.lost")
    return _lost


def unsubscribe(listener: Callable[..., None] | None, topic: str = "meshtastic.receive.text") -> None:
    if listener is None:
        return
    try:
        from pubsub import pub

        pub.unsubscribe(listener, topic)
    except Exception:
        return
