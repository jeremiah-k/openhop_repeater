"""Durability of the repeater's persisted companion message queue.

The repeater stores queued companion messages in SQLite and hands them to the
companion client one at a time (``CMD_SYNC_NEXT_MESSAGE``).  The message must
only leave the database once it has actually been queued for the client: a
dropped frame (no client attached, oversized payload, full write queue) must
leave the row in place so the next sync attempt can deliver it.

The companion protocol has no end-to-end acknowledgement, so delivery is
*at-most-once* after a successful enqueue - the row is deleted as soon as the
frame is queued, and a client that disconnects before reading it loses that
copy.  These tests pin the boundary: nothing is lost *before* the enqueue.
"""

from __future__ import annotations

import asyncio
import struct
from types import SimpleNamespace

import pytest
from openhop_core.companion.constants import (
    FRAME_OUTBOUND_PREFIX,
    MAX_PAYLOAD_SIZE,
    RESP_CODE_NO_MORE_MESSAGES,
)
from openhop_core.companion.models import QueuedMessage
from openhop_core.protocol.constants import TXT_TYPE_PLAIN

from repeater.companion.frame_server import CompanionFrameServer
from repeater.data_acquisition.sqlite_handler import SQLiteHandler

_HASH = "0x01"


def _handler(tmp_path) -> SQLiteHandler:
    return SQLiteHandler(tmp_path)


def _frame_server(handler: SQLiteHandler) -> CompanionFrameServer:
    """A frame server wired to ``handler`` with no client attached."""
    fs = CompanionFrameServer.__new__(CompanionFrameServer)
    fs._message_lock = asyncio.Lock()
    fs._app_target_ver = 3
    fs.sqlite_handler = handler
    fs.companion_hash = _HASH
    fs.bridge = SimpleNamespace(
        sync_next_message=lambda: None,
        message_queue=SimpleNamespace(peek=lambda: None),
    )
    fs._write_queue = None
    return fs


def _persist(handler: SQLiteHandler, text: str, **overrides) -> None:
    msg = {
        "sender_key": b"\x11" * 32,
        "text": text,
        "timestamp": 1000,
        "txt_type": TXT_TYPE_PLAIN,
        "is_channel": False,
        "channel_idx": 0,
        "path_len": 0,
        "packet_hash": overrides.pop("packet_hash", f"ph-{text[:8]}"),
    }
    msg.update(overrides)
    assert handler.companion_push_message(_HASH, msg)


def _attach_client(fs: CompanionFrameServer) -> asyncio.Queue:
    """Attach a client write queue behind a live transport double."""
    fs._write_queue = asyncio.Queue(maxsize=16)
    fs._client_writer = _OpenWriter()
    return fs._write_queue


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



def _queued_payloads(queue: asyncio.Queue) -> list[bytes]:
    """Drain the write queue and return the frame payloads."""
    payloads = []
    while not queue.empty():
        raw = queue.get_nowait()
        assert raw[0] == FRAME_OUTBOUND_PREFIX
        payloads.append(raw[3:])
    return payloads


# ---------------------------------------------------------------------------
# Storage primitives
# ---------------------------------------------------------------------------


def test_companion_peek_message_is_non_destructive(tmp_path):
    handler = _handler(tmp_path)
    _persist(handler, "hello")
    _persist(handler, "world", packet_hash="ph-world")

    peeked = handler.companion_peek_message(_HASH)
    assert peeked is not None
    row_id, msg = peeked
    assert msg["text"] == "hello"
    assert "id" not in msg
    # Peeking must not consume the queue.
    assert handler.companion_count_messages(_HASH) == 2
    assert handler.companion_peek_message(_HASH)[0] == row_id


def test_companion_peek_message_returns_none_when_empty(tmp_path):
    handler = _handler(tmp_path)
    assert handler.companion_peek_message(_HASH) is None


def test_companion_delete_message_removes_only_the_named_row(tmp_path):
    handler = _handler(tmp_path)
    _persist(handler, "first")
    _persist(handler, "second", packet_hash="ph-second")

    row_id, _ = handler.companion_peek_message(_HASH)
    assert handler.companion_delete_message(_HASH, row_id) is True
    assert handler.companion_count_messages(_HASH) == 1
    remaining = handler.companion_peek_message(_HASH)
    assert remaining is not None
    assert remaining[1]["text"] == "second"
    # Deleting an unknown row is a no-op, not an error.
    assert handler.companion_delete_message(_HASH, row_id) is False


def test_companion_pop_message_still_works(tmp_path):
    handler = _handler(tmp_path)
    _persist(handler, "first")
    _persist(handler, "second", packet_hash="ph-second")

    msg = handler.companion_pop_message(_HASH)
    assert msg is not None
    assert msg["text"] == "first"
    assert "id" not in msg
    assert handler.companion_count_messages(_HASH) == 1
    assert handler.companion_pop_message(_HASH)["text"] == "second"
    assert handler.companion_pop_message(_HASH) is None
    assert handler.companion_count_messages(_HASH) == 0


# ---------------------------------------------------------------------------
# The message-loss defect
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_enqueue_failure_leaves_row_and_retry_delivers_once(tmp_path):
    """Pre-fix: the row was deleted by the pop and the frame was dropped.

    The message was gone for good - nothing was left to retry.
    """
    handler = _handler(tmp_path)
    _persist(handler, "keep me")
    fs = _frame_server(handler)

    # No client attached: the frame cannot be queued.
    await fs._cmd_sync_next_message(b"")
    assert handler.companion_count_messages(_HASH) == 1

    # A client connects and asks again: the message is queued for that client
    # once and its persisted row is retired.
    queue = _attach_client(fs)
    await fs._cmd_sync_next_message(b"")
    payloads = _queued_payloads(queue)
    assert len(payloads) == 1
    assert payloads[0].endswith(b"keep me")
    assert handler.companion_count_messages(_HASH) == 0

    # A third request finds nothing left - no duplicate delivery.
    await fs._cmd_sync_next_message(b"")
    assert _queued_payloads(queue) == [bytes([RESP_CODE_NO_MORE_MESSAGES])]
    assert handler.companion_count_messages(_HASH) == 0


@pytest.mark.asyncio
async def test_enqueue_failure_queues_no_message_frame(tmp_path):
    """A failed enqueue must not queue a message frame - and the row stays."""
    handler = _handler(tmp_path)
    _persist(handler, "keep me")
    fs = _frame_server(handler)

    queue = asyncio.Queue(maxsize=1)
    fs._write_queue = queue
    # Fill the queue so the next enqueue hits QueueFull.
    queue.put_nowait(bytes([FRAME_OUTBOUND_PREFIX]) + struct.pack("<H", 1) + b"\x00")
    await fs._cmd_sync_next_message(b"")

    assert handler.companion_count_messages(_HASH) == 1
    # Only the placeholder is in the queue; no message frame was queued.
    assert len(_queued_payloads(queue)) == 1


@pytest.mark.asyncio
async def test_success_deletes_exactly_one_row(tmp_path):
    handler = _handler(tmp_path)
    _persist(handler, "first")
    _persist(handler, "second", packet_hash="ph-second")
    fs = _frame_server(handler)
    queue = _attach_client(fs)

    await fs._cmd_sync_next_message(b"")
    payloads = _queued_payloads(queue)
    assert len(payloads) == 1
    assert payloads[0].endswith(b"first")
    # Only the delivered row is gone; the other message is untouched.
    assert handler.companion_count_messages(_HASH) == 1
    remaining = handler.companion_peek_message(_HASH)
    assert remaining is not None
    assert remaining[1]["text"] == "second"


@pytest.mark.asyncio
async def test_oversize_persisted_message_is_truncated_not_dropped(tmp_path):
    """An over-long message is shortened to fit the frame, not discarded."""
    handler = _handler(tmp_path)
    _persist(handler, "A" * 5000)
    fs = _frame_server(handler)
    queue = _attach_client(fs)

    await fs._cmd_sync_next_message(b"")
    payloads = _queued_payloads(queue)
    assert len(payloads) == 1
    assert len(payloads[0]) <= MAX_PAYLOAD_SIZE
    assert handler.companion_count_messages(_HASH) == 0


@pytest.mark.asyncio
async def test_empty_queue_replies_no_more_messages(tmp_path):
    handler = _handler(tmp_path)
    fs = _frame_server(handler)
    queue = _attach_client(fs)

    await fs._cmd_sync_next_message(b"")
    payloads = _queued_payloads(queue)
    assert payloads == [bytes([RESP_CODE_NO_MORE_MESSAGES])]


@pytest.mark.asyncio
async def test_bridge_queue_message_is_not_deleted_from_sqlite(tmp_path):
    """Messages taken from the in-memory bridge queue have no SQLite row."""
    handler = _handler(tmp_path)
    fs = _frame_server(handler)
    queue = _attach_client(fs)

    fs.bridge = SimpleNamespace(
        sync_next_message=lambda: QueuedMessage(
            sender_key=bytes(32), timestamp=1, text="from bridge"
        )
    )
    await fs._cmd_sync_next_message(b"")
    payloads = _queued_payloads(queue)
    assert len(payloads) == 1
    assert payloads[0].endswith(b"from bridge")
    assert handler.companion_count_messages(_HASH) == 0
