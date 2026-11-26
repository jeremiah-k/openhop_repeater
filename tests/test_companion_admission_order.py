"""Admission numbers keep delivery in arrival order.

The offline queue assigns the arrival number when the message is admitted,
so a persist that is delayed - an earlier subscriber suspending the frame
server's callback - cannot push a later message's row ahead of it: the row
carries the number the message already had, and the sync serves the lowest
outstanding admission number across memory and the store.
"""

from __future__ import annotations

import asyncio

import pytest
from openhop_core.companion.constants import (
    FRAME_OUTBOUND_PREFIX,
    RESP_CODE_NO_MORE_MESSAGES,
)
from openhop_core.companion.message_queue import MessageQueue
from openhop_core.companion.models import MessageEvent, QueuedMessage
from openhop_core.protocol.constants import TXT_TYPE_PLAIN

from repeater.companion.frame_server import CompanionFrameServer
from repeater.data_acquisition.sqlite_handler import SQLiteHandler

_HASH = "0x01"


class _Bridge:
    """Bridge double: offline queue plus per-event callback registries."""

    def __init__(self):
        self.message_queue = MessageQueue(max_size=100)
        self._callbacks: dict[str, list] = {}

    def sync_next_message(self):
        return self.message_queue.pop()

    def get_time(self):
        return 0

    def add_push_callback(self, event, cb):
        self._callbacks.setdefault(event, []).append(cb)

    def remove_push_callback(self, event, cb):
        callbacks = self._callbacks.get(event, [])
        if cb in callbacks:
            callbacks.remove(cb)

    def on_message_event(self, cb):
        self.add_push_callback("message_event", cb)

    def on_channel_message_event(self, cb):
        self.add_push_callback("channel_message_event", cb)

    def on_channel_data_event(self, cb):
        self.add_push_callback("channel_data_event", cb)

    def get_contacts(self):
        return []

    def get_channel(self, _idx):
        return None

    async def fire_message_event(self, event):
        for callback in list(self._callbacks.get("message_event", ())):
            await callback(event)


class _OpenWriter:
    """A live attached-transport double for the admission gate."""

    def write(self, _data):
        return None

    async def drain(self):
        return None

    def close(self):
        return None

    async def wait_closed(self):
        return None

    def is_closing(self):
        return False


def _client_payloads(queue: asyncio.Queue) -> list[bytes]:
    payloads = []
    while not queue.empty():
        raw = queue.get_nowait()
        assert raw[0] == FRAME_OUTBOUND_PREFIX
        payload = raw[3:]
        if payload and payload[0] == RESP_CODE_NO_MORE_MESSAGES:
            continue
        payloads.append(payload)
    return payloads


async def _wait_until(condition, spins: int = 2000) -> None:
    for _ in range(spins):
        if condition():
            return
        await asyncio.sleep(0)
    raise AssertionError("condition not reached within the spin budget")


@pytest.mark.asyncio
async def test_a_delayed_persist_keeps_the_first_message_first(tmp_path):
    handler = SQLiteHandler(tmp_path)
    bridge = _Bridge()
    fs = CompanionFrameServer(
        bridge,
        _HASH,
        port=0,
        client_idle_timeout_sec=None,
        sqlite_handler=handler,
    )
    fs._write_queue = asyncio.Queue(maxsize=16)
    fs._client_writer = _OpenWriter()
    fs._app_target_ver = 3

    # An earlier subscriber suspends the first message's persist callback.
    first = QueuedMessage(
        sender_key=b"\x11" * 32,
        txt_type=TXT_TYPE_PLAIN,
        timestamp=1000,
        text="first text",
        is_channel=False,
        path_len=0xFF,
    )
    second = QueuedMessage(
        sender_key=b"\x11" * 32,
        txt_type=TXT_TYPE_PLAIN,
        timestamp=1001,
        text="second text",
        is_channel=False,
        path_len=0xFF,
    )
    assert bridge.message_queue.push(first) and bridge.message_queue.push(second)
    assert (first.arrival_seq, second.arrival_seq) == (1, 2)  # admission order

    first_suspended = asyncio.Event()
    release_first = asyncio.Event()

    async def earlier_subscriber(event: MessageEvent):
        if event.queue_entry is first:
            first_suspended.set()
            await release_first.wait()

    bridge.add_push_callback("message_event", earlier_subscriber)

    async def event_for(entry: QueuedMessage, text: str, packet_hash: str) -> MessageEvent:
        return MessageEvent(
            sender_key=entry.sender_key,
            text=text,
            timestamp=entry.timestamp,
            txt_type=entry.txt_type,
            packet_hash=packet_hash,
            snr=0.0,
            rssi=0,
            sender_prefix=b"",
            path_len=entry.path_len,
            queued=True,
            queue_entry=entry,
            arrival_seq=entry.arrival_seq,
        )

    await fs.start()  # registers the frame server's callbacks after the gate

    first_task = asyncio.create_task(
        bridge.fire_message_event(await event_for(first, "first text", "HASH-1"))
    )
    await _wait_until(first_suspended.is_set)

    # The second message arrives while the first persist is suspended, and
    # reaches the store first.
    await asyncio.wait_for(
        bridge.fire_message_event(await event_for(second, "second text", "HASH-2")),
        timeout=5,
    )
    assert handler.companion_count_messages(_HASH) == 1
    row = handler.companion_peek_message(_HASH)
    assert row is not None and row[1]["text"] == "second text"

    release_first.set()
    await asyncio.wait_for(asyncio.gather(first_task), timeout=5)
    assert handler.companion_count_messages(_HASH) == 2

    with handler._connect() as conn:
        rows = conn.execute(
            "SELECT text, arrival_seq FROM companion_messages "
            "WHERE companion_hash = ? ORDER BY arrival_seq",
            (_HASH,),
        ).fetchall()
    assert [(r[0], r[1]) for r in rows] == [("first text", 1), ("second text", 2)]

    # The sync serves the first message first, then the second.
    await asyncio.wait_for(fs._cmd_sync_next_message(b""), timeout=5)
    await asyncio.wait_for(fs._cmd_sync_next_message(b""), timeout=5)
    payloads = [p[16:] for p in _client_payloads(fs._write_queue) if len(p) > 16]
    assert payloads == [b"first text", b"second text"]

    fs._client_writer.close()
    await fs.stop()
