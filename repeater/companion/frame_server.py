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
        is raised once at start above the highest stored number -- never with
        a per-message store read. When the store cannot be read the counter
        stays where it is: new messages may then interleave with the stored
        backlog, the delivery-order claim across the stores is dropped, and a
        warning is logged. Rows written before the arrival_seq migration have
        no stored number; the seed read covers them through their
        ``COALESCE(arrival_seq, id)`` fallback (row order, which is their
        arrival order).
        """
        seed = await asyncio.to_thread(
            self.sqlite_handler.companion_max_arrival_seq, self.companion_hash
        )
        if seed is None:
            logger.warning(
                "Companion %s: could not seed the arrival counter from the store; "
                "delivery order across the persisted rows is not guaranteed",
                self.companion_hash,
            )
            return
        self.bridge.message_queue.seed_arrival_counter(seed)

    # -----------------------------------------------------------------
    # Persistence hook overrides
    # -----------------------------------------------------------------

    async def _message_operation(self, operation, *args) -> None:
        """Serialize queue ownership transfers, including SQLite completion.

        Cancellation while waiting for the lock leaves memory untouched. Once
        started, finish the transfer before releasing the lock: cancelling an
        asyncio.to_thread await cannot cancel the underlying SQLite operation.
        """
        async with self._message_lock:
            task = asyncio.create_task(operation(*args))
            try:
                await asyncio.shield(task)
            except asyncio.CancelledError:
                while not task.done():
                    try:
                        await asyncio.shield(task)
                    except asyncio.CancelledError:
                        continue
                task.result()
                raise

    async def _persist_companion_message(self, msg_dict: dict, queue_entry=None) -> None:
        if self.sqlite_handler:
            await self._message_operation(self._persist_message_locked, msg_dict, queue_entry)

    async def _persist_message_locked(self, msg_dict: dict, queue_entry) -> None:
        """Transfer memory ownership to SQLite before another sync can read it."""
        # Another callback may have suspended before this persistence callback
        # ran. In that case a successful frame handoff already owns the entry.
        if getattr(queue_entry, "frame_enqueued", False):
            return
        retention = getattr(
            self.bridge.message_queue,
            "max_size",
            getattr(self.bridge.message_queue, "_max_size", None),
        )
        if retention == 0:
            self._remove_queue_entry(queue_entry)
            return
        _, persisted = await asyncio.to_thread(
            self._stamp_and_push, queue_entry, msg_dict, retention
        )
        if persisted:
            self._remove_queue_entry(queue_entry)
        else:
            logger.debug(
                "Companion %s: retaining message in memory after SQLite queue rejection",
                self.companion_hash,
            )

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
                self.companion_hash, msg_dict, retention, seq
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
        path can order it against the in-memory queue.

        Raises when the store cannot be read; ``None`` means the queue is
        empty. The caller falls back to serving memory and drops the order
        claim when this raises.
        """
        if not self.sqlite_handler:
            return None
        peeked = self.sqlite_handler.companion_peek_message(self.companion_hash)
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
        # and the sync degrades to memory-first below.
        try:
            peeked = await asyncio.to_thread(self._sync_next_from_persistence)
        except Exception:
            peeked = None
            logger.warning(
                "Companion %s: persisted queue unreadable; serving in-memory messages first "
                "and delivery order across the stores is not guaranteed",
                self.companion_hash,
                exc_info=True,
            )
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
            # completed. An entry without an admission number -- an event that
            # bypassed the queue, or a model that forbids attributes -- carries
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
        deleted = await asyncio.to_thread(
            self.sqlite_handler.companion_delete_message, self.companion_hash, row_id
        )
        if not deleted:
            # Do not stream the same retained row repeatedly on this client.
            # A subsequent reconnect can retry; no protocol-level receipt exists.
            logger.warning("Companion %s: queued message retirement failed", self.companion_hash)
            if self._write_queue is queue and self._client_writer is not None:
                self._client_writer.close()

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
        await super().stop()
