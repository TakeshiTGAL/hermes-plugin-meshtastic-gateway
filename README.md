# meshtastic-gateway

Talk to Hermes over a Meshtastic radio, built on the Meshtastic Python API.

A direct message that is addressed to this node, from a node id on the allowlist, is handed to Hermes only when the packet's `pkiEncrypted` flag is true. The reply is sent back to that node as one or more text packets. An empty allowlist answers nobody. There is no allow-all switch. Channel packets are never answered, because a channel key can impersonate any sender. Checked with a fake radio and the real Hermes adapter (`platform_registry.create_adapter`, then `handle_message`, then `send`) on Hermes v0.21.4 and on current main. One meshtasticd 2.7.26 simulator accepted TCP. After 25 seconds with no traffic, the socket timeout was still cleared and the stream reader was still alive, and the simulator log shows the client downloaded a packet. A direct-message decrypt was not completed. Not tried on a physical radio.

This plugin does not flash firmware, does not publish MQTT, and does not send a message to `^all`.

## What was tested

Checked with a fake radio and the real Hermes adapter: `platform_registry.create_adapter`, then `handle_message`, then `send`, on Hermes v0.21.4 and on current main. That fake radio received the reply. One meshtasticd 2.7.26 simulator accepted TCP. After 25 seconds with no traffic, the socket timeout was still cleared and the stream reader was still alive, and the simulator log shows the client downloaded a packet. A direct-message decrypt was not completed. Not tried on a physical radio. This repository does not include a completed two-radio packet exchange.

Checked against public sources on 2026-10-06:

- `TCPInterface` in the meshtastic Python package opens `hostname` on port 4403 by default, and `sendText` takes a node id. `wantAck` defaults to false. This plugin passes `wantAck=False`.
- `DATA_PAYLOAD_LEN` in the Meshtastic protobuf is 233. This plugin caps a text chunk at 200 UTF-8 bytes so a packet is not truncated by that limit.
- Meshtastic's encryption page (docs 2.8) says direct messages since firmware 2.5 use public-key encryption after a key exchange, and that older firmware carried direct messages as channel packets. The same page says a channel key lets anyone impersonate a sender. Anyone who hears the radio can see the packet header, including the node ids. This plugin answers only when `pkiEncrypted` is true, never answers a channel packet, and does not pin a per-node public key.

## Install

Needs Hermes 0.21.4 or later, and the `meshtastic` package (GPL-3.0-only), which this plugin does not vendor. Install it into the same virtualenv Hermes uses. This plugin does not declare that dependency itself:

```bash
pip install meshtastic
```

Set one URL and the nodes that may talk:

```bash
export MESHTASTIC_URL=tcp://radio.example:4403
export MESHTASTIC_ALLOWED_NODES='!aabbccdd'
```

`serial://` and `ble://` are recognized and refused. This plugin opens `tcp://` only, because a serial or BLE open cannot be stopped. If the radio opens but its own node number cannot be read, the connection is closed and nothing is answered. Disconnect drops the text subscription before it closes the radio. A second connect on the same adapter drops the previous subscription first. A tcp URL cannot carry a user name, password, path, or query. A tcp open waits at most 20 seconds for that socket to connect, then clears the socket timeout, so a quiet radio does not stop the reader. After that socket is up, the library waits up to 30 seconds for the radio's own configuration. This plugin does not replace `socket.create_connection`. The library may reconnect on its own, and those later sockets are not given this 20 second limit. This plugin does not cap those retries. If the library reports the connection lost, this adapter stops running so Hermes can build another. `plugins.isolation: host` does not load this platform. Hermes reports that as in-process only, because a platform adapter runs on the gateway event loop. The default isolation is in-process, where the platform is registered. Node ids in the allowlist may be `!aabbccdd`, `!AABBCCDD`, or a decimal number. Hermes compares the environment text as written, so this adapter also publishes the canonical `!aabbccdd` form. A send that already put one chunk on the radio is final: Hermes does not send the whole reply again. A new adapter starts the hourly count and the seen packet ids over.

## Who can use it

`MESHTASTIC_ALLOWED_NODES` is the whole list. Empty means every packet is dropped before Hermes sees it, including slash commands and approval answers. An unreadable id refuses the connection instead of being ignored. The allowlist is not proof of who sent the packet. A channel key can impersonate a node id, so channel packets are dropped. A direct message is also dropped unless `pkiEncrypted` is true. This plugin does not pin a public key. A node id that is not on the list is dropped before a session starts.

A reply to a direct message this process just accepted is transmitted without a second approval prompt, because that is the answer the allowlisted node asked for. Only the task that is producing that reply may transmit. A nested task, tool progress, or interim text is refused and does not go on the radio. An approval question for that same node is transmitted, so the person can answer it. If the approval question cannot be sent (for example, the hourly cap is used up), Hermes still waits its approval timeout (default 300 s) and then refuses. Any other radio send is refused before the radio is called. The gateway does not wait on a person for that other send. If the reply fails before any chunk is on the radio, that packet id is forgotten, so the same packet can be accepted again. A chunk that already went out stays final, and that packet id stays seen.

Cron is not given a sender. Removing the plugin is not required to stop cron, because cron was never registered to transmit.

## Radio limits

Default gap is 20 seconds, and it cannot be set under 10 or over 3600. Default cap is 12 sends in a rolling hour on that adapter, and it cannot be set above 30. A new adapter, including one Hermes builds on reconnect, starts that hour over and forgets packet ids it has already seen, so a retransmission can be answered again. The same adapter keeps only the last 100 packet ids, so an older retransmission can be answered again. `nodes.json` is not the counter. One reply uses at most 4 chunks of 200 UTF-8 bytes (8 is the ceiling). Hermes is asked to hand this plugin up to 800 characters in one send. That cap is characters, not bytes. When the reply does not fit, the chunks that do fit are sent, the last one ends with ` [cut]`, and the Hermes result stays success with error text that the reply was cut. If the radio stops after a chunk has already gone out, the error says how many of the chunks were sent. Inside a send that does fit, a reply that would break the hourly cap is not started. A later send in the same hour waits out the gap instead of pretending the earlier text was never sent. These numbers are not a duty-cycle calculation. Regional rules (for example a 1% or 10% limit, or a dwell-time limit) are the operator's to follow, and the caps above can be lowered. An unsolicited send is refused and does not wait. The gap between chunks of an accepted reply is at most 3600 seconds.

`wantAck` is false on sends this plugin makes. That does not prove the firmware will never retransmit for its own reasons.

## What is stored

Under Hermes `plugin_data_dir`, `nodes.json` keeps up to 200 rows of node id, direction (`in` or `out`), byte count, and time. An inbound row is written only for a packet this radio accepted, after the handoff begins and before the reply is sent. If the event loop is not running, or the message cannot be handed over, the packet is not recorded and its id is not treated as already seen. Writes take a lock, write a temporary file, then replace `nodes.json`. A corrupt file is left as it is. The message text is not stored in that file. A chat display name is not stored there. The text you send is still in the Hermes session, because that is how the gateway turn runs. If `plugin_data_dir` cannot be loaded, nothing is written elsewhere. Uninstall does not delete `nodes.json`. Remove the `meshtastic-gateway` directory under Hermes plugin data to delete it.

## Disclosure

The agent can call this platform once it is connected. There is no daily cap beyond the hourly send cap above. This plugin starts no child process and is not a sandbox. A reply to an accepted direct message does not wait on approval or on an extra model call. It can wait the configured gap between chunks, at most 3600 seconds each. Progress and interim text are not sent. An approval question for the node in that turn is sent. An unsolicited send is refused and does not call Hermes approval, so the gateway does not wait on a person for that other send. `register()` registers one platform and no tool, hook, middleware, or CLI command. `tests/` is shipped and is not loaded by `register()`. `nodes.json` stays after uninstall; delete the `meshtastic-gateway` plugin-data directory to remove it. The Hermes session keeps the message text. `nodes.json` does not. Firmware 2.5+ direct messages are not readable with only the channel key after key exchange; older firmware's direct messages were. Checked with a fake radio and the real Hermes adapter on v0.21.4 and current main. One meshtasticd 2.7.26 simulator accepted TCP. After 25 seconds with no traffic, the socket timeout was still cleared and the stream reader was still alive, and the simulator log shows the client downloaded a packet. A direct-message decrypt was not completed. Not tried on a physical radio.

## Docs

- https://meshtastic.org/docs/overview/encryption/
- https://meshtastic.org/docs/overview/mesh-algo/
- https://github.com/meshtastic/python
- https://pypi.org/project/meshtastic/
- https://teknium.io/hermes-devices
