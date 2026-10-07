from __future__ import annotations

import socket
import sys
import types

import radio


def test_tcp_open_clears_the_socket_timeout_and_does_not_replace_create_connection(monkeypatch):
    sock = types.SimpleNamespace(timeout="unset", closed=False)

    def settimeout(value):
        sock.timeout = value

    def close():
        sock.closed = True

    sock.settimeout = settimeout
    sock.close = close
    seen = {}

    def wrapped(address, timeout=socket._GLOBAL_DEFAULT_TIMEOUT, *args, **kwargs):
        seen["during"] = socket.create_connection
        seen["address"] = address
        seen["timeout"] = timeout
        return sock

    monkeypatch.setattr(socket, "create_connection", wrapped)

    class FakeTCP:
        def __init__(self, hostname, portNumber, connectNow):
            self.socket = None
            self.connectNow = connectNow
            self.hostname = hostname
            self.portNumber = portNumber

    class FakeStream:
        @staticmethod
        def connect(iface):
            seen["reader_socket"] = iface.socket
            seen["timeout_before_reader"] = sock.timeout

    tcp_mod = types.ModuleType("meshtastic.tcp_interface")
    tcp_mod.TCPInterface = FakeTCP
    stream_mod = types.ModuleType("meshtastic.stream_interface")
    stream_mod.StreamInterface = FakeStream
    monkeypatch.setitem(sys.modules, "meshtastic", types.ModuleType("meshtastic"))
    monkeypatch.setitem(sys.modules, "meshtastic.tcp_interface", tcp_mod)
    monkeypatch.setitem(sys.modules, "meshtastic.stream_interface", stream_mod)

    iface = radio._open_tcp({"kind": "tcp", "host": "radio.example", "port": 4403})
    assert seen["during"] is wrapped
    assert seen["address"] == ("radio.example", 4403)
    assert seen["timeout"] == 20
    assert seen["reader_socket"] is sock
    assert seen["timeout_before_reader"] is None
    assert iface.socket is sock
    assert sock.timeout is None
    assert socket.create_connection is wrapped
    assert sock.closed is False
