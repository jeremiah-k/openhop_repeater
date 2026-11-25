"""Serialize memory-to-SQLite transfer and client handoff without compensation."""

from __future__ import annotations

import asyncio
from unittest.mock import patch

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
_TO_THREAD = "repeater.companion.frame_server.asyncio.to_thread"


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
    return fs


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


def _delivered(queue: asyncio.Queue, text: str) -> bool:
    """True when exactly one message frame carrying ``text`` reached the client."""
    payloads = _client_payloads(queue)
    return len(payloads) == 1 and payloads[0].endswith(text.encode())


class _Gate:
    """Deterministic stand-in for ``asyncio.to_thread``.

    The first call registers a release event and suspends until the test
    releases it, then runs the wrapped function inline.  This lets the test
    hold the SQLite write open while the client asks for the next message.
    Later calls run straight through: only the write of the message under test
    has to be held open, the follow-up calls have to be able to finish.
    """

    def __init__(self):
        self.releases: list[asyncio.Event] = []

    async def to_thread(self, func, /, *args, **kwargs):
        if self.releases:
            return func(*args, **kwargs)
        rel = asyncio.Event()
        self.releases.append(rel)
        await rel.wait()
        return func(*args, **kwargs)

    async def release(self, idx: int = 0) -> None:
        self.releases[idx].set()
        # Let the resumed coroutine run its synchronous tail.
        for _ in range(3):
            await asyncio.sleep(0)


async def _settle() -> None:
    """Yield enough for a freshly created task to reach its ``to_thread`` gate."""
    for _ in range(4):
        await asyncio.sleep(0)


# ---------------------------------------------------------------------------
# The duplicate delivery race
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_sync_inside_the_persist_window_delivers_the_message_once(tmp_path):
    """Sync waits for the ownership transfer instead of taking the memory copy."""
    handler = SQLiteHandler(tmp_path)
    bridge = _Bridge()
    fs = _server(handler, bridge)
    entry, msg_dict = _msg("only once", b"\x11", "HASH-RACE")

    gate = _Gate()
    with patch(_TO_THREAD, gate.to_thread):
        # The receive task pushed the message and started the SQLite write.
        bridge.message_queue.push(entry)
        persist = asyncio.create_task(fs._persist_companion_message(msg_dict, entry))
        await _settle()

        # The client asks for the next message inside the persist window.
        sync = asyncio.create_task(fs._cmd_sync_next_message(b""))
        await _settle()
        assert not sync.done() and fs._write_queue.empty()
        await gate.release(0)
        await persist
        await sync
        assert _delivered(fs._write_queue, "only once")

    # Exactly one delivery ...
    assert not _client_payloads(fs._write_queue)
    # ... and no leftover row that could deliver it a second time.
    assert handler.companion_count_messages(_HASH) == 0

    # A later sync finds nothing: the message is not delivered twice.
    await fs._cmd_sync_next_message(b"")
    assert not _client_payloads(fs._write_queue)
    assert handler.companion_count_messages(_HASH) == 0


@pytest.mark.asyncio
async def test_evicted_entry_keeps_its_row_and_is_delivered_later(tmp_path):
    """An entry that was evicted - not delivered - must keep its persisted row.

    The row is then the only copy of the message, so it has to survive and be
    delivered by a later sync.
    """
    handler = SQLiteHandler(tmp_path)
    bridge = _Bridge(max_size=1)
    fs = _server(handler, bridge)
    # A channel message is the only kind the offline queue evicts.
    entry, msg_dict = _msg("evicted", b"\x11", "HASH-EVICT", is_channel=True)

    gate = _Gate()
    with patch(_TO_THREAD, gate.to_thread):
        bridge.message_queue.push(entry)
        persist = asyncio.create_task(fs._persist_companion_message(msg_dict, entry))
        await _settle()

        # The queue is full: the oldest channel entry is evicted, not delivered.
        bridge.message_queue.push(QueuedMessage(sender_key=b"", is_channel=True, text="newer"))
        assert bridge.message_queue.peek() is not entry

        await gate.release(0)
        await persist

    assert entry.frame_enqueued is False
    # The row survived: it is the only copy of the evicted message.
    assert handler.companion_count_messages(_HASH) == 1

    # Drop the unpersisted neighbour so the sync serves the persisted message.
    while bridge.message_queue.pop() is not None:
        pass

    await fs._cmd_sync_next_message(b"")
    assert _delivered(fs._write_queue, "evicted")
    assert handler.companion_count_messages(_HASH) == 0


@pytest.mark.asyncio
async def test_handoff_before_persistence_callback_needs_no_packet_hash(tmp_path):
    handler = SQLiteHandler(tmp_path)
    bridge = _Bridge()
    fs = _server(handler, bridge)
    entry, msg_dict = _msg("no hash", b"\x11", "HASH-NONE")
    msg_dict["packet_hash"] = None
    bridge.message_queue.push(entry)
    await fs._cmd_sync_next_message(b"")
    assert _delivered(fs._write_queue, "no hash")
    await fs._persist_companion_message(msg_dict, entry)
    assert handler.companion_count_messages(_HASH) == 0


@pytest.mark.asyncio
async def test_cancelled_persist_finishes_transfer_before_sync(tmp_path):
    handler = SQLiteHandler(tmp_path)
    bridge = _Bridge()
    fs = _server(handler, bridge)
    entry, msg_dict = _msg("cancelled write", b"\x11", "HASH-CANCEL")
    bridge.message_queue.push(entry)
    gate = _Gate()
    with patch(_TO_THREAD, gate.to_thread):
        persist = asyncio.create_task(fs._persist_companion_message(msg_dict, entry))
        await _settle()
        persist.cancel()
        sync = asyncio.create_task(fs._cmd_sync_next_message(b""))
        await _settle()
        assert not sync.done() and not persist.done()
        await gate.release()
        with pytest.raises(asyncio.CancelledError):
            await persist
        await sync
    assert _delivered(fs._write_queue, "cancelled write")
    assert handler.companion_count_messages(_HASH) == 0


@pytest.mark.asyncio
async def test_replaced_client_does_not_consume_persisted_row(tmp_path):
    handler = SQLiteHandler(tmp_path)
    fs = _server(handler, _Bridge())
    _, msg_dict = _msg("old client", b"\x11", "HASH-OLD")
    assert handler.companion_push_message(_HASH, msg_dict)
    old_queue = fs._write_queue
    gate = _Gate()
    with patch(_TO_THREAD, gate.to_thread):
        sync = asyncio.create_task(fs._cmd_sync_next_message(b""))
        await _settle()
        fs._write_queue = asyncio.Queue()
        await gate.release()
        await sync
    assert old_queue.empty() and fs._write_queue.empty()
    assert handler.companion_count_messages(_HASH) == 1
    await fs._cmd_sync_next_message(b"")
    assert _delivered(fs._write_queue, "old client")


# ---------------------------------------------------------------------------
# A frame that never reached the client
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_message_whose_frame_could_not_be_queued_stays_deliverable(tmp_path):
    """A message popped for a client that never received the frame is kept.

    The entry was taken out of the queue but the frame could not be queued
    for the client, so the message was not delivered.  It goes back into the
    queue instead of being dropped.
    """
    handler = SQLiteHandler(tmp_path)
    bridge = _Bridge()
    fs = _server(handler, bridge)
    entry, msg_dict = _msg("keep me", b"\x11", "HASH-KEEP")

    bridge.message_queue.push(entry)
    # No client attached: the frame cannot be queued for anyone.
    fs._write_queue = None
    await fs._cmd_sync_next_message(b"")

    assert bridge.message_queue.count == 1
    assert entry.frame_enqueued is False

    # A client connects: the message is queued for it once and its persisted
    # row is retired.
    fs._write_queue = asyncio.Queue(maxsize=16)
    await fs._cmd_sync_next_message(b"")
    assert _delivered(fs._write_queue, "keep me")
    assert bridge.message_queue.count == 0


@pytest.mark.asyncio
async def test_committed_sqlite_row_is_not_visible_to_sync_before_transfer_finishes(tmp_path):
    handler = SQLiteHandler(tmp_path)
    bridge = _Bridge()
    fs = _server(handler, bridge)
    entry, msg = _msg("committed", b"\x11", "HASH-COMMITTED")
    bridge.message_queue.push(entry)
    committed, release = asyncio.Event(), asyncio.Event()
    real_to_thread = asyncio.to_thread

    async def held_write(func, /, *args, **kwargs):
        result = await real_to_thread(func, *args, **kwargs)
        if func == handler.companion_push_message:
            committed.set()
            await release.wait()
        return result

    with patch(_TO_THREAD, held_write):
        persist = asyncio.create_task(fs._persist_companion_message(msg, entry))
        await asyncio.wait_for(committed.wait(), 5)
        assert handler.companion_count_messages(_HASH) == 1
        assert bridge.message_queue.peek() is entry
        sync = asyncio.create_task(fs._cmd_sync_next_message(b""))
        await _settle()
        assert not sync.done() and fs._write_queue.empty()
        release.set()
        await asyncio.gather(persist, sync)
    assert _delivered(fs._write_queue, "committed")
    assert handler.companion_count_messages(_HASH) == 0
    assert bridge.message_queue.is_empty()
