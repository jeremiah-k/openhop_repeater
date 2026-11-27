"""
Repeater-specific CompanionFrameServer with SQLite persistence.

Thin subclass of :class:`openhop_core.companion.frame_server.CompanionFrameServer`
that adds SQLite-backed message, contact, and channel persistence via a
``sqlite_handler`` dependency.
"""

from __future__ import annotations

import asyncio
import logging
import shutil
from typing import Callable, Optional

from openhop_core.companion.constants import RESP_CODE_NO_MORE_MESSAGES
from openhop_core.companion.frame_server import CompanionFrameServer as _BaseFrameServer
from openhop_core.companion.models import QueuedMessage

logger = logging.getLogger("CompanionFrameServer")


class CompanionFrameServer(_BaseFrameServer):
    """Adds SQLite persistence for messages, contacts, and channels.

    Constructor signature is intentionally kept compatible with the
    previous monolithic implementation so ``main.py`` call-sites need
    zero changes.
    """

    # The companion queue's store round-trips carry this short busy bound so a
    # locked store degrades within a moment instead of stalling syncs and
    # persists for the connection's default busy timeout (5 s each).
    _STORE_BUSY_TIMEOUT_MS = 250
    # A persist that fails because the store is locked is retried at this pace
    # until this deadline expires; the message stays in memory (still syncable)
    # the whole time and is never dropped.
    _PERSIST_RETRY_INTERVAL_S = 1.0
    _PERSIST_RETRY_DEADLINE_S = 60.0
    # Class-level default so ``__new__``-built test doubles behave like a
    # server whose store is currently readable.
    _store_unreadable_episode = False

    def __init__(
        self,
        bridge,
        companion_hash: str,
        port: int = 5000,
        bind_address: str = "0.0.0.0",  # nosec B104 - intentional default for LAN reachability
        client_idle_timeout_sec: Optional[int] = 8 * 60 * 60,  # 8 hours
        sqlite_handler=None,
        local_hash: Optional[int] = None,
        stats_getter=None,
        control_handler=None,
        batt_getter: Callable[[], int] | None = None,
        storage_dir: str | None = None,
    ):
        super().__init__(
            bridge=bridge,
            companion_hash=companion_hash,
            port=port,
            bind_address=bind_address,
            client_idle_timeout_sec=client_idle_timeout_sec,
            device_model="openHop-Repeater-Companion",
            device_version=None,  # use FIRMWARE_VER_CODE from openhop-core
            build_date="13 Feb 2026",
            local_hash=local_hash,
            stats_getter=stats_getter,
            control_handler=control_handler,
        )
        self.sqlite_handler = sqlite_handler
        self._message_lock = asyncio.Lock()
        self.batt_getter = batt_getter
        self.storage_dir = storage_dir

    def _get_batt_and_storage(self) -> tuple[int, int, int]:
        """Report battery millivolts and storage usage to companion clients.

        The base-class hook returns zeros, so companion apps show no battery and
        no storage for a Python repeater. Battery comes from whichever sensor
        plug-in publishes one (an attached UPS HAT, or the battery of an
        openHop modem the repeater drives); storage is the filesystem holding
        the repeater's data directory.
        """
        millivolts = 0
        if self.batt_getter is not None:
            try:
                millivolts = int(self.batt_getter() or 0)
            except Exception:  # pragma: no cover - never break the companion link
                logger.debug("battery lookup failed", exc_info=True)
                millivolts = 0
        if millivolts < 0 or millivolts > 0xFFFF:
            millivolts = 0

        used_kb = total_kb = 0
        if self.storage_dir:
            try:
                usage = shutil.disk_usage(self.storage_dir)
                total_kb = min(usage.total // 1024, 0xFFFFFFFF)
                used_kb = min((usage.total - usage.free) // 1024, 0xFFFFFFFF)
            except OSError:
                logger.debug("storage lookup failed for %s", self.storage_dir, exc_info=True)
        return millivolts, used_kb, total_kb

    async def start(self) -> None:
        """Start persistence before accepting companion client connections."""
        if self.sqlite_handler:
            self.bridge.on_message_event(self._on_message_event)
            self.bridge.on_channel_message_event(self._on_channel_message_event)
            self.bridge.on_channel_data_event(self._on_channel_data_event)
            await self._seed_arrival_counter()
        await super().start()

    async def _seed_arrival_counter(self) -> None:
        """Start the offline queue's admission counter above the stored backlog.

        The offline queue assigns one monotonically increasing arrival number
        to every admitted message (``MessageQueue.push``), and the sync serves
        the lowest outstanding number across memory and the store. Rows
        persisted by an earlier run already carry such numbers, so the counter
        is raised once at start above the highest stored number — never with a
        per-message store read. When the store cannot be read the counter
        stays where it is: new messages may then interleave with the stored
        backlog, the delivery-order claim across the stores is dropped, and a
        warning is logged. Rows written before the arrival_seq migration have
        no stored number; the seed read covers them through their
        ``COALESCE(arrival_seq, id)`` fallback (row order, which is their
        arrival order).
        """
        seed = await asyncio.to_thread(
            self.sqlite_handler.companion_max_arrival_seq,
            self.companion_hash,
            self._STORE_BUSY_TIMEOUT_MS,
        )
        if seed is None:
            logger.warning(
                "Companion %s: could not seed the arrival counter from the store; "
                "delivery order across the persisted rows is not guaranteed",
                self.companion_hash,
            )
            return
        seed_fn = getattr(self.bridge.message_queue, "seed_arrival_counter", None)
        if seed_fn is None:
            # An offline queue that does not assign admission numbers (an
            # older core): rows are written without a number and fall back to
            # their row order at read time.
            logger.warning(
                "Companion %s: the offline queue does not assign admission "
                "numbers; delivery order across the persisted rows is not "
                "guaranteed",
                self.companion_hash,
            )
            return
        seed_fn(seed)

    # -----------------------------------------------------------------
    # Persistence hook overrides
    # -----------------------------------------------------------------

    async def _message_operation(self, operation, *args):
        """Serialize queue ownership transfers, including SQLite completion.

        Cancellation while waiting for the lock leaves memory untouched. Once
        started, finish the transfer before releasing the lock: cancelling an
        asyncio.to_thread await cannot cancel the underlying SQLite operation.
        Returns the operation's result.
        """
        async with self._message_lock:
            task = asyncio.create_task(operation(*args))
            try:
                return await asyncio.shield(task)
            except asyncio.CancelledError:
                while not task.done():
                    try:
                        await asyncio.shield(task)
                    except asyncio.CancelledError:
                        continue
                task.result()
                raise

    async def _persist_companion_message(self, msg_dict: dict, queue_entry=None) -> bool:
        if self.sqlite_handler:
            persisted = await self._message_operation(
                self._persist_message_locked, msg_dict, queue_entry
            )
            if not persisted:
                self._schedule_persist_retry(msg_dict, queue_entry)
            return persisted
        return False

    async def _persist_message_locked(self, msg_dict: dict, queue_entry) -> bool:
        """Transfer memory ownership to SQLite before another sync can read it.

        Returns whether the message is now durably stored (or needed no store).
        A message the store rejected stays in memory; the caller schedules a
        bounded retry so the row lands once the store is writable again.
        """
        # Another callback may have suspended before this persistence callback
        # ran. In that case a successful frame handoff already owns the entry.
        if getattr(queue_entry, "frame_enqueued", False):
            return True
        retention = getattr(
            self.bridge.message_queue,
            "max_size",
            getattr(self.bridge.message_queue, "_max_size", None),
        )
        if retention == 0:
            self._remove_queue_entry(queue_entry)
            return True
        _, persisted = await asyncio.to_thread(
            self._stamp_and_push, queue_entry, msg_dict, retention
        )
        if persisted:
            self._remove_queue_entry(queue_entry)
            return True
        logger.debug(
            "Companion %s: retaining message in memory after SQLite queue rejection",
            self.companion_hash,
        )
        return False

    def _schedule_persist_retry(self, msg_dict: dict, queue_entry) -> None:
        """Retry a failed persist at a bounded pace until the row lands.

        The message stays in memory the whole time — a sync may still serve it,
        and each retry attempt skips itself if the entry was frame-enqueued in
        the meantime. When the lock lifts the row is written exactly once,
        because the successful attempt removes the entry from memory under the
        same message lock that serializes syncs.
        """
        tasks: set[asyncio.Task] = getattr(self, "_persist_retry_tasks", None)
        if tasks is None:
            tasks = set()
            self._persist_retry_tasks = tasks
        loop = asyncio.get_running_loop()
        deadline = loop.time() + self._PERSIST_RETRY_DEADLINE_S

        async def _retry() -> None:
            while loop.time() < deadline:
                await asyncio.sleep(self._PERSIST_RETRY_INTERVAL_S)
                if getattr(queue_entry, "frame_enqueued", False):
                    return
                try:
                    if await self._message_operation(
                        self._persist_message_locked, msg_dict, queue_entry
                    ):
                        return
                except asyncio.CancelledError:
                    raise
                except Exception:
                    logger.debug(
                        "Companion %s: persist retry attempt failed",
                        self.companion_hash,
                        exc_info=True,
                    )

        task = asyncio.create_task(_retry(), name=f"companion-persist-retry-{id(queue_entry)}")
        tasks.add(task)
        task.add_done_callback(tasks.discard)

    def _stamp_and_push(self, queue_entry, msg_dict: dict, retention) -> tuple[Optional[int], bool]:
        """Persist an entry under its admission number (holds ``_message_lock``).

        The offline queue assigns one monotonically increasing arrival number
        to every accepted message at admission (``MessageQueue.push``, on the
        event loop's single writer), so the number order is the arrival order
        no matter when the persist runs or in which order persist callbacks
        reach the message lock. This call only carries that number into the
        store: nothing is minted here, so a persist retried after a locked
        store reuses the entry's original number and lands its row in the
        arrival order it already had.

        Returns ``(arrival_seq, persisted)``. ``arrival_seq`` is None when the
        entry cannot carry one (an event without a queue entry, or a core
        model that forbids new attributes); such a row falls back to its row
        order at read time.
        """
        seq = None
        if queue_entry is not None:
            try:
                seq = int(queue_entry.arrival_seq)
            except (AttributeError, TypeError, ValueError):
                seq = None
        persisted = bool(
            self.sqlite_handler.companion_push_message(
                self.companion_hash,
                msg_dict,
                retention,
                seq,
                busy_timeout_ms=self._STORE_BUSY_TIMEOUT_MS,
            )
        )
        return seq, persisted

    def _remove_queue_entry(self, queue_entry) -> bool:
        """Remove exactly the persisted entry from the bridge queue by identity.

        Falling back to ``pop_last`` when ``queue_entry`` is None would reopen the
        interleaving race (it could evict a newer, unpersisted entry), so an event
        from an older core that carries no entry is left in memory: a possible
        duplicate is preferable to losing a message.

        Returns True when the entry was found and removed, False when it was
        not in the queue any more - it was delivered to a client, evicted, or
        never queued.
        """
        if queue_entry is None:
            logger.debug(
                "Companion %s: no queue entry on persisted message; leaving in memory",
                self.companion_hash,
            )
            return False
        return bool(self.bridge.message_queue.remove(queue_entry))

    def _sync_next_from_persistence(self) -> Optional[tuple[int, QueuedMessage]]:
        """Peek the next persisted message in arrival order.

        The row is *not* removed here: it is deleted by
        :meth:`_cmd_sync_next_message` once the frame has really been queued
        for the client.  Returns ``(row_id, message)`` so exactly that row can
        be deleted afterwards.  The returned message carries the row's arrival
        number (its ``id`` when the row was stored without one) so the sync
        path can order it against the in-memory queue, and the raw row dict
        (``row_dict``, every column including the dedup key) so the sync path
        can re-insert the row unchanged if the client disappears while the
        delete runs.

        Raises when the store cannot be read; ``None`` means the queue is
        empty. The caller falls back to serving memory and drops the order
        claim when this raises.
        """
        if not self.sqlite_handler:
            return None
        peeked = self.sqlite_handler.companion_peek_message(
            self.companion_hash, busy_timeout_ms=self._STORE_BUSY_TIMEOUT_MS
        )
        if not peeked:
            return None
        row_id, msg_dict = peeked
        sender_prefix = msg_dict.get("sender_prefix", b"")
        if isinstance(sender_prefix, str):
            sender_prefix = bytes.fromhex(sender_prefix) if sender_prefix else b""
        msg = QueuedMessage(
            sender_key=msg_dict.get("sender_key", b""),
            txt_type=msg_dict.get("txt_type", 0),
            timestamp=msg_dict.get("timestamp", 0),
            text=msg_dict.get("text", ""),
            is_channel=bool(msg_dict.get("is_channel", False)),
            channel_idx=msg_dict.get("channel_idx", 0),
            path_len=msg_dict.get("path_len", 0),
            snr=float(msg_dict.get("snr") or 0.0),
            rssi=int(msg_dict.get("rssi") or 0),
            channel_data_type=int(msg_dict.get("channel_data_type") or 0),
            channel_data_payload=bytes(msg_dict.get("channel_data_payload") or b""),
            sender_prefix=sender_prefix,
        )
        try:
            msg.arrival_seq = int(msg_dict.get("arrival_seq", row_id))
        except AttributeError:
            pass  # a model that forbids new attributes: memory is served first
        try:
            msg.row_dict = msg_dict
        except AttributeError:
            pass  # a model that forbids new attributes: retirement cannot roll back
        return row_id, msg

    # -----------------------------------------------------------------
    # Non-blocking command overrides (keep event loop responsive)
    # -----------------------------------------------------------------

    async def _cmd_sync_next_message(self, data: bytes) -> None:
        """Transfer a single queued entry to this client's outbound queue.

        Sync delivers oldest first across the in-memory and persisted queues:
        every accepted message carries an arrival number, and the client gets
        the lowest outstanding number of the two stores. When the persisted
        store cannot be read (for example while it is locked), memory is
        served first, a warning is logged, and the order claim is dropped — a
        slow or locked store must not stall new messages behind rows that
        cannot be read right now.

        A successful enqueue is not a client receipt. A disconnect or process
        crash after enqueue can still lose a message; a failed SQLite delete
        can still cause a replay. The protocol has no acknowledgement to close
        that boundary.
        """
        await self._message_operation(self._sync_message_locked, self._write_queue)

    async def _sync_message_locked(self, queue) -> None:
        if self._write_queue is not queue:
            return
        # Peek both stores before popping anything so the client is served
        # from whichever store holds the lowest outstanding arrival number,
        # rather than "memory first".  A locked or broken store raises here
        # (the peek carries a short busy bound, so a real exclusive lock
        # surfaces within a moment instead of the default busy timeout) and
        # the sync degrades to memory-first below, warning once per lock
        # episode rather than once per sync.
        peek_ok = True
        try:
            peeked = await asyncio.to_thread(self._sync_next_from_persistence)
        except Exception:
            peek_ok = False
            peeked = None
            if not self._store_unreadable_episode:
                self._store_unreadable_episode = True
                logger.warning(
                    "Companion %s: persisted queue unreadable; serving in-memory messages first "
                    "and delivery order across the stores is not guaranteed",
                    self.companion_hash,
                    exc_info=True,
                )
        if peek_ok:
            self._store_unreadable_episode = False
        if self._write_queue is not queue:
            return
        memory_first = True
        if peeked is not None:
            head = self.bridge.message_queue.peek()
            head_seq = getattr(head, "arrival_seq", None) if head is not None else None
            row_seq = getattr(peeked[1], "arrival_seq", None)
            # Both sides carry arrival numbers assigned at admission (the
            # offline queue stamps every accepted entry at push; rows store
            # the same number, and rows written before the arrival_seq
            # migration report their row id, which is their arrival order), so
            # this compares true arrival order no matter when each persist
            # completed. An entry without an admission number — an event that
            # bypassed the queue, or a model that forbids attributes — carries
            # no order claim at all: it is served from memory first rather
            # than being treated as the newest message and held behind rows it
            # cannot be ordered against.
            memory_first = head_seq is None or row_seq is None or head_seq <= row_seq
        if memory_first and self._sync_memory_message():
            return
        if peeked is None:
            self._write_frame(bytes([RESP_CODE_NO_MORE_MESSAGES]))
            return
        row_id, msg = peeked
        if not self._write_frame(self._build_message_frame(msg)):
            return
        # Retirement may only confirm an admission that still holds: the frame
        # must belong to the client that is still the attached one, checked
        # here under the same message lock that admitted it. A client that
        # was replaced, evicted, closed, or is closing between admission and
        # retirement keeps the row for its successor.
        writer = self._client_writer
        if self._write_queue is not queue or writer is None or writer.is_closing():
            logger.info(
                "Companion %s: client left between frame admission and retirement; "
                "keeping the persisted row",
                self.companion_hash,
            )
            return
        deleted = await asyncio.to_thread(
            self.sqlite_handler.companion_delete_message,
            self.companion_hash,
            row_id,
            self._STORE_BUSY_TIMEOUT_MS,
        )
        if not deleted:
            # Do not stream the same retained row repeatedly on this client.
            # A subsequent reconnect can retry; no protocol-level receipt exists.
            logger.warning("Companion %s: queued message retirement failed", self.companion_hash)
            if self._write_queue is queue and self._client_writer is not None:
                self._client_writer.close()
            return
        # The delete ran in a worker thread, so the client may have been
        # replaced, evicted, or disconnected while it ran — the admitted frame
        # would die with the old client's write queue. Roll the row back so a
        # successor client is served instead.
        current = self._client_writer
        if (
            self._write_queue is not queue
            or current is not writer
            or current is None
            or current.is_closing()
        ):
            await self._rollback_retired_row(msg)

    async def _rollback_retired_row(self, msg: QueuedMessage) -> None:
        """Re-insert a row retired on behalf of a client that did not stay attached.

        The raw row dict peeked from the store is written back verbatim
        (including ``arrival_seq`` and the dedup key), so the successor client
        is served the same message in the same order. If the store refuses the
        re-insert because the row already exists again, the message is still
        queued; any other refusal is logged loudly because the only copy was
        the frame just dropped.
        """
        row_dict = getattr(msg, "row_dict", None)
        if row_dict is None:
            logger.error(
                "Companion %s: cannot roll back the retired row (no row dict); "
                "the message may be lost",
                self.companion_hash,
            )
            return
        retention = getattr(
            self.bridge.message_queue,
            "max_size",
            getattr(self.bridge.message_queue, "_max_size", None),
        )
        restored = await asyncio.to_thread(
            self.sqlite_handler.companion_push_message,
            self.companion_hash,
            row_dict,
            retention,
            row_dict.get("arrival_seq"),
        )
        if restored:
            logger.info(
                "Companion %s: kept the retired row for the next client", self.companion_hash
            )
        else:
            logger.error(
                "Companion %s: could not keep the retired row; the row was restored "
                "or rejected by the store and must be verified",
                self.companion_hash,
            )

    @staticmethod
    def _contact_to_dict(c) -> dict:
        """Convert a Contact object to a persistence dict."""
        pk = c.public_key if isinstance(c.public_key, bytes) else bytes.fromhex(c.public_key)
        raw_advert = getattr(c, "last_advert_packet", None)
        if isinstance(raw_advert, bytearray):
            raw_advert = bytes(raw_advert)
        elif isinstance(raw_advert, str):
            try:
                raw_advert = bytes.fromhex(raw_advert)
            except ValueError:
                raw_advert = None
        elif not isinstance(raw_advert, bytes):
            raw_advert = None
        return {
            "pubkey": pk,
            "name": c.name,
            "adv_type": c.adv_type,
            "flags": c.flags,
            "out_path_len": c.out_path_len,
            "out_path": (
                c.out_path
                if isinstance(c.out_path, bytes)
                else (bytes.fromhex(c.out_path) if c.out_path else b"")
            ),
            "last_advert_timestamp": c.last_advert_timestamp,
            "last_advert_packet": raw_advert,
            "lastmod": c.lastmod,
            "gps_lat": c.gps_lat,
            "gps_lon": c.gps_lon,
            "sync_since": c.sync_since,
        }

    async def _persist_contact(self, contact) -> None:
        """Upsert a single contact to SQLite (non-blocking)."""
        if not self.sqlite_handler:
            return
        contact_dict = self._contact_to_dict(contact)
        await asyncio.to_thread(
            self.sqlite_handler.companion_upsert_contact,
            self.companion_hash,
            contact_dict,
        )

    async def _save_contacts(self) -> None:
        """Persist all contacts to SQLite (non-blocking)."""
        if not self.sqlite_handler:
            return
        contacts = self.bridge.get_contacts()
        dicts = [self._contact_to_dict(c) for c in contacts]
        await asyncio.to_thread(
            self.sqlite_handler.companion_save_contacts,
            self.companion_hash,
            dicts,
        )

    async def _save_channels(self) -> None:
        """Persist channels to SQLite (non-blocking)."""
        if not self.sqlite_handler:
            return
        channels = []
        max_ch = getattr(getattr(self.bridge, "channels", None), "max_channels", 40)
        for idx in range(max_ch):
            ch = self.bridge.get_channel(idx)
            if ch is not None:
                channels.append(
                    {
                        "channel_idx": idx,
                        "name": ch.name,
                        "secret": ch.secret,
                    }
                )
        await asyncio.to_thread(
            self.sqlite_handler.companion_save_channels,
            self.companion_hash,
            channels,
        )

    async def stop(self) -> None:
        """Persist contacts and channels before stopping (so they survive daemon restart)."""
        if self.sqlite_handler:
            try:
                await self._save_contacts()
                await self._save_channels()
            except Exception as e:
                logger.warning("Failed to persist contacts/channels on stop: %s", e)
        tasks: set[asyncio.Task] = getattr(self, "_persist_retry_tasks", None) or set()
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        await super().stop()
