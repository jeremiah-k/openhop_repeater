"""Sync delivers the oldest outstanding message across memory and SQLite.

A message that arrived later but is still only in memory must not overtake
an older message that is already persisted. One arrival sequence spans both
stores and a sync serves whichever store holds the lowest outstanding
arrival number.
"""

from __future__ import annotations

import asyncio
import sqlite3

import pytest
from openhop_core.companion.constants import (
    FRAME_OUTBOUND_PREFIX,
    RESP_CODE_NO_MORE_MESSAGES,
)
from openhop_core.companion.message_queue import MessageQueue
from openhop_core.companion.models import QueuedMessage
from openhop_core.protocol.constants import TXT_TYPE_PLAIN

from repeater.companion.frame_server import CompanionFrameServer
from repeater.data_acquisition.sqlite_handler import SQLiteHandler

_HASH = "0x01"


class _Bridge:
    """Bridge double exposing a real in-memory offline queue."""

    def __init__(self, max_size: int = 100):
        self.message_queue = MessageQueue(max_size=max_size)

    def sync_next_message(self):
        """Pop the next message, exactly like the real bridge does."""
        return self.message_queue.pop()


def _server(handler: SQLiteHandler, bridge: _Bridge) -> CompanionFrameServer:
    """A frame server wired to ``handler`` with a client write queue attached."""
    fs = CompanionFrameServer.__new__(CompanionFrameServer)
    fs._message_lock = asyncio.Lock()
    fs._app_target_ver = 3
    fs.sqlite_handler = handler
    fs.companion_hash = _HASH
    fs.bridge = bridge
    fs._write_queue = asyncio.Queue(maxsize=16)
    fs._client_writer = _OpenWriter()
    # Mirror start(): raise the admission counter above the stored backlog, so
    # a fresh server's new arrivals sort behind the rows already persisted.
    fs.bridge.message_queue.seed_arrival_counter(
        handler.companion_max_arrival_seq(_HASH) or 0
    )
    return fs


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



def _msg(text: str, key_byte: bytes, packet_hash: str, *, is_channel: bool = False):
    """A queue entry and the matching persistence dict for one message."""
    entry = QueuedMessage(
        sender_key=b"" if is_channel else key_byte * 32,
        txt_type=TXT_TYPE_PLAIN,
        timestamp=1000,
        text=text,
        is_channel=is_channel,
        path_len=0xFF,
    )
    msg_dict = {
        "sender_key": entry.sender_key,
        "text": text,
        "timestamp": entry.timestamp,
        "txt_type": entry.txt_type,
        "is_channel": is_channel,
        "channel_idx": 0,
        "path_len": entry.path_len,
        "packet_hash": packet_hash,
        "snr": 0.0,
        "rssi": 0,
        "sender_prefix": b"",
    }
    return entry, msg_dict


def _client_payloads(queue: asyncio.Queue) -> list[bytes]:
    """The payloads the client received (NO_MORE_MESSAGES is not a message)."""
    payloads = []
    while not queue.empty():
        raw = queue.get_nowait()
        assert raw[0] == FRAME_OUTBOUND_PREFIX
        payload = raw[3:]
        if payload and payload[0] == RESP_CODE_NO_MORE_MESSAGES:
            continue
        payloads.append(payload)
    return payloads


@pytest.mark.asyncio
async def test_persisted_older_message_is_delivered_before_newer_memory_message(tmp_path):
    """Persist OLDER, accept NEWER in memory, sync: OLDER must come first.

    The in-memory queue is drained before the persisted rows by the current
    implementation, so the newer in-memory copy overtakes the older
    persisted message. One arrival sequence must keep them in arrival order.
    """
    handler = SQLiteHandler(tmp_path)
    bridge = _Bridge()
    fs = _server(handler, bridge)
    older_entry, older_dict = _msg("older", b"\x11", "HASH-OLDER")
    newer_entry, _ = _msg("newer", b"\x22", "HASH-NEWER")

    bridge.message_queue.push(older_entry)
    await fs._persist_companion_message(older_dict, older_entry)
    assert handler.companion_count_messages(_HASH) == 1

    # NEWER arrives while its persistence callback has not run yet.
    bridge.message_queue.push(newer_entry)

    await fs._cmd_sync_next_message(b"")
    await fs._cmd_sync_next_message(b"")

    payloads = _client_payloads(fs._write_queue)
    assert len(payloads) == 2
    assert payloads[0].endswith(b"older")
    assert payloads[1].endswith(b"newer")
    assert handler.companion_count_messages(_HASH) == 0


def _insert_legacy_row(handler: SQLiteHandler, text: str) -> None:
    """Insert a row the way a pre-arrival-sequence writer did (no arrival_seq)."""
    conn = sqlite3.connect(handler.sqlite_path)
    conn.execute(
        """
        INSERT INTO companion_messages
        (companion_hash, sender_key, txt_type, timestamp, text, is_channel,
         channel_idx, path_len, created_at)
        VALUES ('0x01', X'11', 2, 1000, ?, 0, 0, 255, 1.0)
    """,
        (text,),
    )
    conn.commit()
    conn.close()


@pytest.mark.asyncio
async def test_rows_without_an_arrival_number_keep_their_row_order(tmp_path):
    """Rows stored before the arrival_seq migration fall back to row order.

    The row order is the arrival order of the rows, so the fallback keeps the
    backlog in order and a new arrival still sorts behind it.
    """
    handler = SQLiteHandler(tmp_path)
    _insert_legacy_row(handler, "old-1")
    _insert_legacy_row(handler, "old-2")
    bridge = _Bridge()
    fs = _server(handler, bridge)
    fresh_entry, fresh_dict = _msg("fresh", b"\x22", "HASH-FRESH")

    bridge.message_queue.push(fresh_entry)
    await fs._persist_companion_message(fresh_dict, fresh_entry)
    assert handler.companion_count_messages(_HASH) == 3

    for _ in range(3):
        await fs._cmd_sync_next_message(b"")

    payloads = _client_payloads(fs._write_queue)
    assert len(payloads) == 3
    assert payloads[0].endswith(b"old-1")
    assert payloads[1].endswith(b"old-2")
    assert payloads[2].endswith(b"fresh")
    assert handler.companion_count_messages(_HASH) == 0


@pytest.mark.asyncio
async def test_unreadable_store_serves_memory_and_drops_the_order_claim(
    tmp_path, monkeypatch, caplog
):
    """A locked store must not stall new messages behind unreadable rows.

    The client gets the in-memory message, a warning is logged, and the
    persisted row is left in place.
    """
    import logging

    handler = SQLiteHandler(tmp_path)
    bridge = _Bridge()
    fs = _server(handler, bridge)
    stored_entry, stored_dict = _msg("row kept", b"\x11", "HASH-ROW")
    fresh_entry, _ = _msg("from memory", b"\x22", "HASH-MEM")

    bridge.message_queue.push(stored_entry)
    await fs._persist_companion_message(stored_dict, stored_entry)
    assert handler.companion_count_messages(_HASH) == 1

    def locked(companion_hash):
        raise sqlite3.OperationalError("database is locked")

    monkeypatch.setattr(handler, "companion_peek_message", locked)
    bridge.message_queue.push(fresh_entry)

    with caplog.at_level(logging.WARNING, logger="CompanionFrameServer"):
        await fs._cmd_sync_next_message(b"")

    payloads = _client_payloads(fs._write_queue)
    assert len(payloads) == 1 and payloads[0].endswith(b"from memory")
    warnings = [r for r in caplog.records if r.levelno == logging.WARNING]
    assert any("unreadable" in r.getMessage() for r in warnings)
    assert any("not guaranteed" in r.getMessage() for r in warnings)
    assert handler.companion_count_messages(_HASH) == 1


@pytest.mark.asyncio
async def test_restart_keeps_persisted_rows_ahead_of_a_new_arrival(tmp_path):
    """After a restart the arrival counter continues above the stored rows.

    The fresh frame server seeds its counter from the persisted backlog, so a
    message that arrives after the restart cannot overtake the rows that were
    already waiting. The new message keeps its stamped arrival number even
    when the store rejects its row (duplicate packet hash here).
    """
    first = SQLiteHandler(tmp_path)
    first_bridge = _Bridge()
    first_fs = _server(first, first_bridge)
    for text, key, hash_ in (("m1", b"\x11", "HASH-1"), ("m2", b"\x22", "HASH-2")):
        entry, msg_dict = _msg(text, key, hash_)
        first_bridge.message_queue.push(entry)
        await first_fs._persist_companion_message(msg_dict, entry)
    assert first.companion_count_messages(_HASH) == 2

    # Restart: a fresh handler and frame server over the same store.
    restarted = SQLiteHandler(tmp_path)
    bridge = _Bridge()
    fs = _server(restarted, bridge)
    # Same packet hash as m2: the row is rejected and the message stays in
    # memory, exactly the "new arrival not yet persisted" case.
    late_entry, late_dict = _msg("after restart", b"\x33", "HASH-2")

    bridge.message_queue.push(late_entry)
    await fs._persist_companion_message(late_dict, late_entry)
    assert getattr(late_entry, "arrival_seq", 0) > 2
    assert restarted.companion_count_messages(_HASH) == 2

    for _ in range(3):
        await fs._cmd_sync_next_message(b"")

    payloads = _client_payloads(fs._write_queue)
    assert len(payloads) == 3
    assert payloads[0].endswith(b"m1")
    assert payloads[1].endswith(b"m2")
    assert payloads[2].endswith(b"after restart")
    assert restarted.companion_count_messages(_HASH) == 0


class _OpenWriter:
    """A live attached-transport double for tests that stub the write queue."""

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
