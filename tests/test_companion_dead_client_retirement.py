"""A persisted row may only retire into a client that is still attached.

A sync runs while the client's socket is already dead
but the server has not processed the disconnect yet. Retiring a row on that
client's behalf loses the message — the frame dies with the client's write
queue, and a successor client finds nothing. The row must be kept and the
successor must be served.
"""

from __future__ import annotations

import asyncio
import struct
import threading
from collections import deque

import pytest
from openhop_core.companion.constants import (
    CMD_SYNC_NEXT_MESSAGE,
    FRAME_INBOUND_PREFIX,
)
from openhop_core.companion.models import QueuedMessage
from openhop_core.protocol.constants import TXT_TYPE_PLAIN

from repeater.companion.frame_server import CompanionFrameServer
from repeater.data_acquisition.sqlite_handler import SQLiteHandler

_HASH = "0x01"


def _command_frame(payload: bytes) -> bytes:
    return bytes([FRAME_INBOUND_PREFIX]) + struct.pack("<H", len(payload)) + payload


class _ScriptedReader:
    """Reader that replays scripted bytes, blocks when idle, EOF on close."""

    def __init__(self):
        self._chunks: deque[int] = deque()
        self._data = asyncio.Event()
        self._eof = False

    def feed(self, data: bytes) -> None:
        self._chunks.extend(data)
        self._data.set()

    def close(self) -> None:
        self._eof = True
        self._data.set()

    async def read(self, _n: int) -> bytes:
        while True:
            if self._chunks:
                return bytes([self._chunks.popleft()])
            if self._eof:
                return b""
            self._data.clear()
            if self._chunks:  # a feed raced the clear
                continue
            await self._data.wait()

    async def readexactly(self, n: int) -> bytes:
        if len(self._chunks) < n:
            raise asyncio.IncompleteReadError(partial=b"", expected=n)
        return bytes([self._chunks.popleft() for _ in range(n)])


class _RecordingWriter:
    """Minimal writer double: closing mirrors a dead socket on demand."""

    def __init__(self):
        self.written: list[bytes] = []
        self.closed = False

    def get_extra_info(self, _name):
        return None

    def write(self, data: bytes) -> None:
        self.written.append(data)

    async def drain(self) -> None:
        return None

    def close(self) -> None:
        self.closed = True

    async def wait_closed(self) -> None:
        return None

    def is_closing(self) -> bool:
        return self.closed


class _Bridge:
    """Bridge double exposing a real in-memory offline queue."""

    def __init__(self):
        from openhop_core.companion.message_queue import MessageQueue

        self.message_queue = MessageQueue(max_size=100)

    def sync_next_message(self):
        return self.message_queue.pop()

    def get_time(self):
        return 0

    def add_push_callback(self, _event, _cb):
        return None

    def remove_push_callback(self, _event, _cb):
        return None


async def _wait_until(condition, spins: int = 2000) -> None:
    for _ in range(spins):
        if condition():
            return
        await asyncio.sleep(0.005)
    raise AssertionError("condition not reached within the spin budget")


def _persist_dict(text: str, packet_hash: str) -> dict:
    return {
        "sender_key": b"\x11" * 32,
        "text": text,
        "timestamp": 1000,
        "txt_type": TXT_TYPE_PLAIN,
        "is_channel": False,
        "channel_idx": 0,
        "path_len": 0xFF,
        "packet_hash": packet_hash,
        "snr": 0.0,
        "rssi": 0,
        "sender_prefix": b"",
    }


@pytest.mark.asyncio
async def test_sync_into_a_dead_client_keeps_the_row_for_the_successor(tmp_path):
    """The c5 shape: sync admits into a dead transport; the row must survive."""
    handler = SQLiteHandler(tmp_path)
    bridge = _Bridge()
    fs = CompanionFrameServer(
        bridge,
        _HASH,
        port=0,
        client_idle_timeout_sec=None,
        sqlite_handler=handler,
    )

    entry = QueuedMessage(
        sender_key=b"\x11" * 32,
        txt_type=TXT_TYPE_PLAIN,
        timestamp=1000,
        text="persisted text",
        is_channel=False,
        path_len=0xFF,
    )
    bridge.message_queue.push(entry)
    await fs._persist_companion_message(_persist_dict("persisted text", "HASH-1"), entry)
    assert handler.companion_count_messages(_HASH) == 1
    assert bridge.message_queue.count == 0  # ownership moved to SQLite

    # Gate the sync command so the dead-client sync can be awaited exactly.
    original_sync = fs._cmd_handlers[CMD_SYNC_NEXT_MESSAGE]
    command_done = asyncio.Event()

    async def gated_sync(data):
        try:
            await original_sync(data)
        finally:
            command_done.set()

    fs._cmd_handlers[CMD_SYNC_NEXT_MESSAGE] = gated_sync

    reader_a, writer_a = _ScriptedReader(), _RecordingWriter()
    handler_a = asyncio.create_task(fs._handle_client(reader_a, writer_a))
    await _wait_until(lambda: fs._client_writer is writer_a and fs._writer_task is not None)

    # The client is killed: the socket is dead while the server still has the
    # client attached and a GET_MSG in flight.
    writer_a.closed = True
    reader_a.feed(_command_frame(bytes([CMD_SYNC_NEXT_MESSAGE])))
    await _wait_until(lambda: command_done.is_set())

    # The row must NOT have been retired for the dying client.
    assert handler.companion_count_messages(_HASH) == 1

    # The disconnect is processed; a successor client connects and must
    # receive exactly the message the dying one could not take.
    reader_a.close()
    await _wait_until(lambda: fs._client_writer is not writer_a)

    reader_b, writer_b = _ScriptedReader(), _RecordingWriter()
    handler_b = asyncio.create_task(fs._handle_client(reader_b, writer_b))
    await _wait_until(lambda: fs._client_writer is writer_b)
    reader_b.feed(_command_frame(bytes([CMD_SYNC_NEXT_MESSAGE])))
    await _wait_until(lambda: handler.companion_count_messages(_HASH) == 0)
    await _wait_until(lambda: writer_b.written)
    assert b"persisted text" in b"".join(writer_b.written)

    reader_b.close()
    await _wait_until(lambda: handler_a.done() and handler_b.done())
    await asyncio.gather(handler_a, handler_b)


@pytest.mark.asyncio
async def test_rolled_back_row_keeps_the_dedup_key(tmp_path):
    """A row rolled back for a client that left mid-retirement keeps its dedup key.

    The retirement rollback re-inserts the message whose row it already
    deleted. If the rollback drops ``packet_hash``, a later push of the same
    packet is accepted as a new row instead of rejected as a duplicate.
    """
    handler = SQLiteHandler(tmp_path)
    bridge = _Bridge()
    fs = CompanionFrameServer(
        bridge,
        _HASH,
        port=0,
        client_idle_timeout_sec=None,
        sqlite_handler=handler,
    )

    entry = QueuedMessage(
        sender_key=b"\x11" * 32,
        txt_type=TXT_TYPE_PLAIN,
        timestamp=1000,
        text="persisted text",
        is_channel=False,
        path_len=0xFF,
    )
    bridge.message_queue.push(entry)
    await fs._persist_companion_message(_persist_dict("persisted text", "HASH-1"), entry)
    assert handler.companion_count_messages(_HASH) == 1

    # Gate the retirement delete so the client can die while it is in flight.
    real_delete = handler.companion_delete_message
    delete_started = asyncio.Event()
    release_delete = threading.Event()

    def gated_delete(companion_hash, message_id, *args, **kwargs):
        delete_started.set()
        release_delete.wait(timeout=10)
        return real_delete(companion_hash, message_id, *args, **kwargs)

    handler.companion_delete_message = gated_delete

    original_sync = fs._cmd_handlers[CMD_SYNC_NEXT_MESSAGE]
    command_done = asyncio.Event()

    async def gated_sync(data):
        try:
            await original_sync(data)
        finally:
            command_done.set()

    fs._cmd_handlers[CMD_SYNC_NEXT_MESSAGE] = gated_sync

    reader_a, writer_a = _ScriptedReader(), _RecordingWriter()
    handler_a = asyncio.create_task(fs._handle_client(reader_a, writer_a))
    await _wait_until(lambda: fs._client_writer is writer_a and fs._writer_task is not None)

    # The frame is admitted, then the client dies while the delete runs: the
    # retirement is undone by rolling the row back.
    reader_a.feed(_command_frame(bytes([CMD_SYNC_NEXT_MESSAGE])))
    await _wait_until(lambda: delete_started.is_set())
    writer_a.closed = True
    release_delete.set()
    await _wait_until(lambda: command_done.is_set())
    assert handler.companion_count_messages(_HASH) == 1  # rolled back

    # The rolled-back row carries its dedup key, so the same packet pushed
    # again is rejected as a duplicate.
    row = handler.companion_peek_message(_HASH)
    assert row is not None and row[1]["packet_hash"] == "HASH-1"
    assert handler.companion_push_message(_HASH, _persist_dict("persisted text", "HASH-1")) is False

    reader_a.close()
    await _wait_until(lambda: handler_a.done())
    await asyncio.gather(handler_a)
