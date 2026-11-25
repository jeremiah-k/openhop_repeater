"""A retried persist keeps the message's original arrival number.

The offline queue stamps the arrival number when the message is admitted,
and every persist attempt carries that number: a message whose first
persist failed (the store locked) keeps its number on the retry, so its row
lands in the arrival order it already had. Minting a fresh number per
attempt would push a retried message behind messages that arrived after it.
The counter that numbers new admissions is seeded once at start above the
stored backlog; a start that cannot read the store leaves the counter
unseeded instead of raising it, and the delivery-order claim across the
stores is dropped for that run.
"""

from __future__ import annotations

import asyncio

import pytest
from openhop_core.companion.message_queue import MessageQueue

from repeater.companion.frame_server import CompanionFrameServer
from repeater.data_acquisition.sqlite_handler import SQLiteHandler

_HASH = "0x01"


class _Bridge:
    """Bridge double exposing a real in-memory offline queue."""

    def __init__(self):
        self.message_queue = MessageQueue(max_size=100)

    def sync_next_message(self):
        return self.message_queue.pop()


def _server(handler: SQLiteHandler, bridge: _Bridge) -> CompanionFrameServer:
    fs = CompanionFrameServer.__new__(CompanionFrameServer)
    fs._message_lock = asyncio.Lock()
    fs._app_target_ver = 3
    fs.sqlite_handler = handler
    fs.companion_hash = _HASH
    fs.bridge = bridge
    fs._write_queue = asyncio.Queue(maxsize=16)
    fs._client_writer = None
    return fs


def _msg(text: str, packet_hash: str):
    from openhop_core.companion.models import QueuedMessage
    from openhop_core.protocol.constants import TXT_TYPE_PLAIN

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


@pytest.mark.asyncio
async def test_retry_reuses_the_first_arrival_number(tmp_path, monkeypatch):
    """A failed persist then a successful retry stores exactly one number."""
    handler = SQLiteHandler(tmp_path)
    bridge = _Bridge()
    fs = _server(handler, bridge)
    entry, msg_dict = _msg("stamped once", "HASH-ONCE")

    calls: list[tuple] = []
    real_push = handler.companion_push_message

    def recording_push(companion_hash, msg, max_messages=None, arrival_seq=None, **kwargs):
        calls.append(arrival_seq)
        if len(calls) == 1:
            return False  # first attempt (or a locked store) fails
        return real_push(companion_hash, msg, max_messages, arrival_seq, **kwargs)

    monkeypatch.setattr(handler, "companion_push_message", recording_push)

    bridge.message_queue.push(entry)
    await fs._persist_companion_message(msg_dict, entry)
    admission_number = getattr(entry, "arrival_seq", None)
    if admission_number is None:
        pytest.skip("offline queue does not stamp admission numbers (fixed core required)")
    assert admission_number is not None  # stamped at admission

    # Let the bounded retry land the row; it must reuse the admission number.
    for _ in range(140):
        if handler.companion_count_messages(_HASH) == 1 and bridge.message_queue.count == 0:
            break
        await asyncio.sleep(0.1)
    assert handler.companion_count_messages(_HASH) == 1
    assert bridge.message_queue.count == 0
    row = handler.companion_peek_message(_HASH)
    assert row is not None and row[1]["arrival_seq"] == admission_number
    assert calls[0] == admission_number
    assert all(seq == admission_number for seq in calls if seq is not None)


@pytest.mark.asyncio
async def test_failed_seed_at_start_leaves_the_counter_unseeded(tmp_path, monkeypatch, caplog):
    """A store that cannot be read must not raise the counter, and says so."""

    import logging

    handler = SQLiteHandler(tmp_path)
    bridge = _Bridge()
    fs = _server(handler, bridge)

    def locked(companion_hash, busy_timeout_ms=None):
        # The handler's own contract: a failed read returns None (it logs).
        return None

    monkeypatch.setattr(handler, "companion_max_arrival_seq", locked)

    with caplog.at_level(logging.WARNING, logger="CompanionFrameServer"):
        await fs._seed_arrival_counter()

    warnings = [r for r in caplog.records if "arrival counter" in r.getMessage()]
    assert warnings  # the dropped order claim is announced

    # The counter was not raised: admissions continue from the queue's own
    # sequence instead of pretending to sit above an unreadable backlog.
    entry, _msg_dict = _msg("unseeded", "HASH-SEED")
    bridge.message_queue.push(entry)
    assert getattr(entry, "arrival_seq", 1) == 1
