"""A locked store must not stall the queue and never lose a message.

Driven against a real second connection holding an
EXCLUSIVE lock on the store file (not a forced raise). The store runs in WAL
mode, so under such a lock reads still succeed from the last committed
snapshot while writes fail after their busy wait — which is why the degrade
hinges on giving the writes a short bound and retrying them, not on the
read fallback:

- sync must complete within a moment while the store is locked (each store
  round-trip carries a short busy bound instead of the 5 s default), serve
  memory when no row stands ahead of it, and never log the unreadable-store
  warning for a merely locked store;
- a persist that fails under the lock is retried until it lands once, so
  the message is neither dropped nor duplicated;
- a retirement whose delete fails under the lock keeps the row.
"""

from __future__ import annotations

import asyncio
import logging
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
_SYNC_DEADLINE_S = 2.5  # the degrade must land within a few seconds, not a busy timeout


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
    """A frame server wired to ``handler`` with an attached client queue."""
    fs = CompanionFrameServer.__new__(CompanionFrameServer)
    fs._message_lock = asyncio.Lock()
    fs._app_target_ver = 3
    fs.sqlite_handler = handler
    fs.companion_hash = _HASH
    fs.bridge = bridge
    fs._write_queue = asyncio.Queue(maxsize=16)
    fs._client_writer = _OpenWriter()
    # Mirror start(): raise the admission counter above the stored backlog so
    # entries admitted now sort behind rows an earlier run already persisted.
    seed_fn = getattr(bridge.message_queue, "seed_arrival_counter", None)
    if seed_fn is not None:
        seed_fn(handler.companion_max_arrival_seq(_HASH) or 0)
    return fs


def _msg(text: str, packet_hash: str):
    entry = QueuedMessage(
        sender_key=b"\x11" * 32,
        txt_type=TXT_TYPE_PLAIN,
        timestamp=1000,
        text=text,
        is_channel=False,
        path_len=0xFF,
    )
    msg_dict = {
        "sender_key": entry.sender_key,
        "text": text,
        "timestamp": entry.timestamp,
        "txt_type": entry.txt_type,
        "is_channel": False,
        "channel_idx": 0,
        "path_len": entry.path_len,
        "packet_hash": packet_hash,
        "snr": 0.0,
        "rssi": 0,
        "sender_prefix": b"",
    }
    return entry, msg_dict


class _ExclusiveLock:
    """A second REAL connection holding an EXCLUSIVE lock on the store."""

    def __init__(self, path: str):
        self._conn = sqlite3.connect(path, timeout=0.05)
        self._conn.execute("PRAGMA busy_timeout=50")
        self._conn.execute("BEGIN EXCLUSIVE")
        self._held = True

    def release(self):
        if self._held:
            self._conn.rollback()
            self._conn.close()
            self._held = False


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


def _texts(payloads: list[bytes]) -> list[bytes]:
    """The message text carried by each contact-message payload."""
    return [p[16:] for p in payloads if len(p) > 16]


async def _wait_for(condition, timeout_s: float = 14.0) -> None:
    for _ in range(int(timeout_s / 0.05)):
        if condition():
            return
        await asyncio.sleep(0.05)
    raise AssertionError("condition not reached in time")


@pytest.mark.asyncio
async def test_sync_completes_under_a_real_lock_and_serves_memory(tmp_path, caplog):
    """o3: a locked store must not stall syncs; memory is served during it."""
    handler = SQLiteHandler(tmp_path)
    bridge = _Bridge()
    fs = _server(handler, bridge)
    lock = _ExclusiveLock(str(handler.sqlite_path))
    try:
        fresh_entry, _ = _msg("from memory", "HASH-MEM")
        bridge.message_queue.push(fresh_entry)

        with caplog.at_level(logging.WARNING, logger="CompanionFrameServer"):
            await asyncio.wait_for(fs._cmd_sync_next_message(b""), timeout=_SYNC_DEADLINE_S)
            await asyncio.wait_for(fs._cmd_sync_next_message(b""), timeout=_SYNC_DEADLINE_S)
    finally:
        lock.release()

    payloads = _texts(_client_payloads(fs._write_queue))
    assert payloads == [b"from memory"]  # memory served; the next sync saw NO_MORE
    unreadable = [
        r for r in caplog.records if r.levelno == logging.WARNING and "unreadable" in r.getMessage()
    ]
    assert unreadable == []  # reads still work under a WAL exclusive lock
    assert handler.companion_count_messages(_HASH) == 0


@pytest.mark.asyncio
async def test_retirement_delete_failing_under_lock_keeps_the_row(tmp_path):
    """A delete that cannot run under the lock must not retire the row."""
    handler = SQLiteHandler(tmp_path)
    bridge = _Bridge()
    fs = _server(handler, bridge)
    entry, msg_dict = _msg("backlog", "HASH-BACKLOG")
    bridge.message_queue.push(entry)
    await fs._persist_companion_message(msg_dict, entry)
    assert handler.companion_count_messages(_HASH) == 1

    lock = _ExclusiveLock(str(handler.sqlite_path))
    try:
        await asyncio.wait_for(fs._cmd_sync_next_message(b""), timeout=_SYNC_DEADLINE_S)
        assert handler.companion_count_messages(_HASH) == 1  # row kept
    finally:
        lock.release()

    await asyncio.wait_for(fs._cmd_sync_next_message(b""), timeout=_SYNC_DEADLINE_S)
    payloads = _texts(_client_payloads(fs._write_queue))
    assert payloads[0] == b"backlog"
    assert handler.companion_count_messages(_HASH) == 0


@pytest.mark.asyncio
async def test_persist_failed_under_lock_is_retried_and_persisted_after_release(
    tmp_path,
):
    """A persist that fails under the lock is retried until it lands once."""
    handler = SQLiteHandler(tmp_path)
    bridge = _Bridge()
    fs = _server(handler, bridge)
    lock = _ExclusiveLock(str(handler.sqlite_path))
    late_entry, late_dict = _msg("during lock", "HASH-LATE")

    try:
        bridge.message_queue.push(late_entry)
        await asyncio.wait_for(
            fs._persist_companion_message(late_dict, late_entry), timeout=_SYNC_DEADLINE_S
        )
        # Retained in memory, not yet persisted: only the fast bound ran.
        assert handler.companion_count_messages(_HASH) == 0
        assert bridge.message_queue.count == 1
    finally:
        lock.release()

    # Wait for the whole ownership transfer: a poll that stops at the
    # committed row can observe it before the memory entry is removed.
    await _wait_for(
        lambda: handler.companion_count_messages(_HASH) == 1 and bridge.message_queue.count == 0
    )
    row = handler.companion_peek_message(_HASH)
    assert row is not None and row[1]["text"] == "during lock"
    assert bridge.message_queue.count == 0  # ownership moved to SQLite, no duplicate
    await asyncio.wait_for(fs._cmd_sync_next_message(b""), timeout=_SYNC_DEADLINE_S)
    assert handler.companion_count_messages(_HASH) == 0
    assert _texts(_client_payloads(fs._write_queue)) == [b"during lock"]


@pytest.mark.asyncio
async def test_live_message_accepted_during_lock_reaches_client_after_release(
    tmp_path,
):
    """o2: backlog first, then the live message; nothing lost, nothing duplicated."""
    handler = SQLiteHandler(tmp_path)
    bridge = _Bridge()
    fs = _server(handler, bridge)
    for index in range(5):
        entry, msg_dict = _msg(f"backlog-{index}", f"HASH-B{index}")
        bridge.message_queue.push(entry)
        await fs._persist_companion_message(msg_dict, entry)
    assert handler.companion_count_messages(_HASH) == 5

    # Restart: a fresh frame server over the same store.
    bridge = _Bridge()
    fs = _server(handler, bridge)
    lock = _ExclusiveLock(str(handler.sqlite_path))
    live_entry, live_dict = _msg("live during lock", "HASH-LIVE")
    try:
        bridge.message_queue.push(live_entry)
        await asyncio.wait_for(
            fs._persist_companion_message(live_dict, live_entry), timeout=_SYNC_DEADLINE_S
        )
        assert handler.companion_count_messages(_HASH) == 5  # not persisted yet
        assert bridge.message_queue.count == 1  # retained in memory
    finally:
        lock.release()

    # The bounded retry makes the live message durable once the store unlocks;
    # wait for the whole transfer, not just the committed row.
    await _wait_for(
        lambda: handler.companion_count_messages(_HASH) == 6 and bridge.message_queue.count == 0
    )
    assert bridge.message_queue.count == 0

    for _ in range(7):
        await asyncio.wait_for(fs._cmd_sync_next_message(b""), timeout=_SYNC_DEADLINE_S)

    payloads = _texts(_client_payloads(fs._write_queue))
    assert payloads == [
        b"backlog-0",
        b"backlog-1",
        b"backlog-2",
        b"backlog-3",
        b"backlog-4",
        b"live during lock",
    ]
    assert handler.companion_count_messages(_HASH) == 0


@pytest.mark.asyncio
async def test_unreadable_store_warns_once_per_episode(tmp_path, monkeypatch, caplog):
    """The read fallback warning is logged once per episode, not per sync."""
    handler = SQLiteHandler(tmp_path)
    bridge = _Bridge()
    fs = _server(handler, bridge)

    def locked(companion_hash, busy_timeout_ms=None):
        raise sqlite3.OperationalError("database is locked")

    monkeypatch.setattr(handler, "companion_peek_message", locked)
    for index in range(3):
        bridge.message_queue.push(_msg(f"m{index}", f"HASH-M{index}")[0])
        await asyncio.wait_for(fs._cmd_sync_next_message(b""), timeout=_SYNC_DEADLINE_S)

    payloads = _texts(_client_payloads(fs._write_queue))
    assert payloads == [b"m0", b"m1", b"m2"]
    warnings = [
        r for r in caplog.records if r.levelno == logging.WARNING and "unreadable" in r.getMessage()
    ]
    assert len(warnings) == 1  # once per episode, not once per sync


def test_bounded_busy_timeout_does_not_leak_into_the_cached_connection(tmp_path):
    """A short busy bound stays scoped to the call that asked for it.

    The bound is armed on the thread's cached connection for the bounded call
    and the default is restored on that connection's next unbounded call, so a
    later contact write on the same worker thread never runs under the short
    bound.
    """
    handler = SQLiteHandler(tmp_path)
    bounded = handler._connect(busy_timeout_ms=250)
    assert bounded.execute("PRAGMA busy_timeout").fetchone()[0] == 250

    # Same thread, same cached connection: the unbounded call must run under
    # the default again, not inherit the previous call's bound.
    unbounded = handler._connect()
    assert unbounded is bounded
    assert (
        unbounded.execute("PRAGMA busy_timeout").fetchone()[0]
        == SQLiteHandler._DEFAULT_BUSY_TIMEOUT_MS
    )
