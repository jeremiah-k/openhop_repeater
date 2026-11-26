"""Rows created before the arrival_seq migration keep their place in sync.

Migration 17 backfills every pre-existing companion_messages row with
``arrival_seq = id``. Rows written under a different companion hash are
never served while that companion's newer rows drain - the store
correctly filters by companion hash. For the same companion this pins
that backfilled rows are served first, in row order, ahead of every newer
arrival.
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

    def __init__(self):
        self.message_queue = MessageQueue(max_size=100)

    def sync_next_message(self):
        return self.message_queue.pop()


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


def _server(handler: SQLiteHandler, bridge: _Bridge) -> CompanionFrameServer:
    fs = CompanionFrameServer.__new__(CompanionFrameServer)
    fs._message_lock = asyncio.Lock()
    fs._app_target_ver = 3
    fs.sqlite_handler = handler
    fs.companion_hash = _HASH
    fs.bridge = bridge
    fs._write_queue = asyncio.Queue(maxsize=16)
    fs._client_writer = _OpenWriter()
    # Mirror start(): raise the admission counter above the stored backlog so
    # a new arrival sorts behind the pre-migration rows.
    fs.bridge.message_queue.seed_arrival_counter(
        handler.companion_max_arrival_seq(_HASH) or 0
    )
    return fs


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


def _insert_backfilled_row(handler: SQLiteHandler, text: str) -> int:
    """Insert a row the way the pre-migration writer did, then backfill it.

    The row carries no arrival number; ``UPDATE ... SET arrival_seq = id``
    reproduces exactly what migration 17 did to the standing volume.
    """
    conn = sqlite3.connect(handler.sqlite_path)
    cursor = conn.execute(
        """
        INSERT INTO companion_messages
        (companion_hash, sender_key, txt_type, timestamp, text, is_channel,
         channel_idx, path_len, created_at)
        VALUES (?, X'11', 0, 1000, ?, 0, 0, 255, 1.0)
    """,
        (_HASH, text),
    )
    conn.execute("UPDATE companion_messages SET arrival_seq = id WHERE arrival_seq IS NULL")
    conn.commit()
    conn.close()
    return cursor.lastrowid


@pytest.mark.asyncio
async def test_backfilled_pre_migration_rows_are_served_first_in_row_order(tmp_path):
    handler = SQLiteHandler(tmp_path)
    first_id = _insert_backfilled_row(handler, "pre-migration-1")
    second_id = _insert_backfilled_row(handler, "pre-migration-2")
    assert first_id < second_id

    # A different companion's backfilled rows are correctly NOT served.
    conn = sqlite3.connect(handler.sqlite_path)
    conn.execute(
        """
        INSERT INTO companion_messages
        (companion_hash, sender_key, txt_type, timestamp, text, is_channel,
         channel_idx, path_len, created_at, arrival_seq)
        VALUES ('0x09', X'22', 0, 1000, 'other companion', 0, 0, 255, 1.0, 4242)
    """
    )
    conn.commit()
    conn.close()

    bridge = _Bridge()
    fs = _server(handler, bridge)
    # The fresh server seeds its counter above the backfilled rows, so a new
    # arrival sorts behind the pre-migration backlog.
    entry = QueuedMessage(
        sender_key=b"\x11" * 32,
        txt_type=TXT_TYPE_PLAIN,
        timestamp=1000,
        text="new arrival",
        is_channel=False,
        path_len=0xFF,
    )
    bridge.message_queue.push(entry)
    # Stamped at admission, above the seeded floor: the entry sorts behind the
    # backfilled rows no matter when its persist lands.
    assert entry.arrival_seq == 3

    for _ in range(3):
        await fs._cmd_sync_next_message(b"")

    payloads = _client_payloads(fs._write_queue)
    assert [p[16:] for p in payloads] == [
        b"pre-migration-1",
        b"pre-migration-2",
        b"new arrival",
    ]
    assert handler.companion_count_messages(_HASH) == 0
    # The other companion's row is still there, untouched.
    conn = sqlite3.connect(handler.sqlite_path)
    remaining = conn.execute(
        "SELECT COUNT(*) FROM companion_messages WHERE companion_hash = '0x09'"
    ).fetchone()[0]
    conn.close()
    assert remaining == 1
