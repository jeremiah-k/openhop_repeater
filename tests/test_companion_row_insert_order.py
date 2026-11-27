"""Sync order follows the arrival number, not the row insert order.

A message whose persist is retried after a
locked store lands its row when the lock lifts, so its row id reflects the
retry's insert time rather than its arrival. Ordering the queue by row id
then serves that message behind its own successors. The queue must order
by ``COALESCE(arrival_seq, id)``: the arrival number where one exists, the
row order (the pre-migration fallback) where not.
"""

from __future__ import annotations

import sqlite3


from repeater.data_acquisition.sqlite_handler import SQLiteHandler

_HASH = "0x01"


def _row(handler: SQLiteHandler, text: str, arrival_seq) -> int:
    conn = sqlite3.connect(handler.sqlite_path)
    cursor = conn.execute(
        """
        INSERT INTO companion_messages
        (companion_hash, sender_key, txt_type, timestamp, text, is_channel,
         channel_idx, path_len, created_at, arrival_seq)
        VALUES (?, X'11', 0, 1000, ?, 0, 0, 255, 1.0, ?)
    """,
        (_HASH, text, arrival_seq),
    )
    conn.commit()
    conn.close()
    return cursor.lastrowid


def test_peek_orders_by_arrival_number_not_insert_order(tmp_path):
    """A retried insert with a later row id still serves at its arrival."""
    handler = SQLiteHandler(tmp_path)
    # The message that arrived FIRST (lower arrival number) lands its row
    # LAST (higher row id): exactly the locked-store retry outcome.
    early_row = _row(handler, "arrived-second", 8)  # inserted first
    late_row = _row(handler, "arrived-first", 7)  # retried insert lands last
    assert late_row > early_row  # insert order contradicts arrival order

    row_id, msg = handler.companion_peek_message(_HASH)
    assert row_id == late_row and msg["text"] == "arrived-first"
    assert msg["arrival_seq"] == 7


def test_rows_without_a_number_still_keep_row_order(tmp_path):
    """Pre-migration rows (arrival_seq = id) keep serving in row order."""
    handler = SQLiteHandler(tmp_path)
    first = _row(handler, "old-1", None)  # pre-migration style
    conn = sqlite3.connect(handler.sqlite_path)
    conn.execute("UPDATE companion_messages SET arrival_seq = id WHERE arrival_seq IS NULL")
    conn.commit()
    conn.close()
    second = _row(handler, "old-2", None)
    conn = sqlite3.connect(handler.sqlite_path)
    conn.execute("UPDATE companion_messages SET arrival_seq = id WHERE arrival_seq IS NULL")
    conn.commit()
    conn.close()

    row_id, msg = handler.companion_peek_message(_HASH)
    assert row_id == first and msg["text"] == "old-1"
    assert msg["arrival_seq"] == first

    handler.companion_delete_message(_HASH, first)
    row_id, msg = handler.companion_peek_message(_HASH)
    assert row_id == second and msg["text"] == "old-2"
