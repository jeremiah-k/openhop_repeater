"""Shutdown order of the plugin manager.

The manager stops the plugins and retires crash supervision in a different
order depending on whether the operator enabled the managed shutdown policy:

* managed (the policy is configured): crash supervision is retired before the
  first plugin is stopped, so the crash supervisor cannot start a plugin again
  while the manager is draining the others.
* unmanaged (the default): the upstream order is kept - the plugins are
  stopped first and crash supervision is retired afterwards.
"""

from __future__ import annotations

import io
import logging
import os
import signal
from pathlib import Path
from typing import Any

import pytest

from repeater.plugins import manager as manager_module
from repeater.plugins import runtime as runtime_module
from repeater.plugins.manager import PluginManager
from repeater.plugins.manifest import PluginManifest, RuntimeSpec
from repeater.plugins.runtime import PluginRuntime
from repeater.plugins.shutdown_policy import (
    PLUGIN_STOP_OVERHEAD_SECONDS,
    SHUTDOWN_MARGIN_SECONDS,
    ShutdownPolicy,
)
from repeater.plugins.storage import PluginStorage


def _install_plugin(storage: PluginStorage, plugin_id: str, version: str = "1.0.0") -> None:
    storage.ensure_plugin_layout(plugin_id, version)
    storage.write_state(plugin_id, {"version": version, "enabled": True})
    storage.write_manifest(
        plugin_id,
        version,
        PluginManifest(
            schema=1,
            id=plugin_id,
            name="Demo",
            version=version,
            runtime=RuntimeSpec(type="python", entrypoint="demo-cli"),
        ),
    )
    storage.set_current(plugin_id, version)


class _RecordingRuntime:
    """Records the order in which the manager drives the runtime."""

    def __init__(self):
        self.calls: list[str] = []

    def running_plugin_ids(self, *, lock_wait=None) -> list[str]:
        return ["openhop.alpha"]

    def stop_supervisor(self) -> None:
        self.calls.append("stop_supervisor")

    def stop(self, plugin_id: str, *, timeout=None, lock_wait=None, deadline=None) -> None:
        self.calls.append("stop")


def test_managed_order_retires_supervision_before_the_first_stop(tmp_path: Path):
    storage = PluginStorage(tmp_path / "plugins")
    _install_plugin(storage, "openhop.alpha")

    runtime = _RecordingRuntime()
    manager = PluginManager(storage, runtime, shutdown_policy=ShutdownPolicy(5, 20, True))
    manager.stop_all()
    assert runtime.calls == ["stop_supervisor", "stop"]


def test_unmanaged_order_keeps_the_upstream_sequence(tmp_path: Path):
    storage = PluginStorage(tmp_path / "plugins")
    _install_plugin(storage, "openhop.alpha")

    runtime = _RecordingRuntime()
    manager = PluginManager(storage, runtime)
    manager.stop_all()
    assert runtime.calls == ["stop", "stop_supervisor"]


# --- managed drain deadline accounting --------------------------------------


class _FakeClock:
    """Controlled monotonic clock: sleeps advance it, nothing waits for real."""

    def __init__(self, start: float = 0.0):
        self.now = start

    def monotonic(self) -> float:
        return self.now

    def time(self) -> float:
        return self.now

    def sleep(self, seconds: float) -> None:
        self.now += max(0.0, float(seconds))


class _RestartHeldLock:
    """Runtime lock held by a simulated restart until a clock instant.

    ``acquire`` advances the fake clock by the time the caller spends
    waiting and succeeds only once the clock has reached the instant the
    restart releases the lock, so a lock wait is charged to the drain the
    way it is charged in production.
    """

    def __init__(self, clock: _FakeClock, busy_until: float):
        self._clock = clock
        self._busy_until = busy_until

    def acquire(self, timeout: float | None = None, blocking: bool = True) -> bool:
        now = self._clock.monotonic()
        if now >= self._busy_until:
            return True
        wait = self._busy_until - now if timeout is None else max(0.0, float(timeout))
        if now + wait >= self._busy_until:
            self._clock.sleep(self._busy_until - now)
            return True
        self._clock.sleep(wait)
        return False

    def release(self) -> None:
        pass

    def __enter__(self):
        self.acquire()
        return self

    def __exit__(self, *exc_info) -> None:
        self.release()


class _StuckProc:
    """A fake plugin process that ignores SIGTERM until it is killed."""

    def __init__(self, pid: int, *, exits_on_sigterm: bool = False):
        self.pid = pid
        self.process_group = None
        self.signals: list[int] = []
        self._code: int | None = None
        self._exits_on_sigterm = exits_on_sigterm
        self.stdout = io.BytesIO(b"")

    def poll(self):
        return self._code

    def send_signal(self, sig):
        self.signals.append(sig)
        if self._exits_on_sigterm and sig == signal.SIGTERM:
            self._code = 0

    def kill(self):
        self.signals.append(signal.SIGKILL)
        self._code = -9

    def wait(self, timeout=None):
        return self._code


@pytest.fixture(autouse=True)
def absent_process_groups(monkeypatch):
    # Fake PIDs must never signal real host processes.
    def raise_absent(*args):
        raise ProcessLookupError

    monkeypatch.setattr(os, "killpg", raise_absent)


@pytest.fixture
def fake_clock(monkeypatch):
    """Replace the manager's and runtime's clock with one the test drives."""
    clock = _FakeClock()
    monkeypatch.setattr(manager_module, "time", clock)
    monkeypatch.setattr(runtime_module, "time", clock)
    return clock


def _drain_runtime(
    tmp_path: Path, plugin_ids: list[str]
) -> tuple[PluginStorage, PluginRuntime, list[_StuckProc]]:
    """A real runtime over fake processes for the enabled plugins."""
    storage = PluginStorage(tmp_path / "plugins")
    for plugin_id in plugin_ids:
        _install_plugin(storage, plugin_id)
        bin_dir = storage.paths_for(plugin_id).venv_dir("1.0.0") / "bin"
        bin_dir.mkdir(parents=True, exist_ok=True)
        (bin_dir / "demo-cli").write_text("x")
        (bin_dir / "demo-cli").chmod(0o755)
    procs: list[_StuckProc] = []

    def fake_popen(cmd, **kwargs):
        proc = _StuckProc(pid=4000 + len(procs))
        procs.append(proc)
        return proc

    runtime = PluginRuntime(storage, popen_factory=fake_popen)
    for plugin_id in plugin_ids:
        runtime.start(plugin_id)
        # Route the fake process through the poll-based termination path so
        # the stop grace actually elapses instead of exiting on the absent
        # group.
        runtime._handles[plugin_id].process_group = None
    return storage, runtime, procs


def _record_stop_attempts(runtime: PluginRuntime, clock: _FakeClock) -> list[dict[str, Any]]:
    """Record every stop the manager attempts, with the clock instant."""
    attempts: list[dict[str, Any]] = []
    original_stop = runtime.stop

    def recording_stop(plugin_id: str, **kwargs):
        attempts.append({"plugin_id": plugin_id, "at": clock.monotonic(), **kwargs})
        return original_stop(plugin_id, **kwargs)

    runtime.stop = recording_stop  # type: ignore[assignment]
    return attempts


def _retire_supervisor_spending(
    runtime: PluginRuntime, clock: _FakeClock, seconds: float
) -> list[str]:
    """Retire crash supervision through the fake clock, spending budget."""
    calls: list[str] = []
    original_stop_supervisor = runtime.stop_supervisor

    def recorded_stop_supervisor() -> None:
        calls.append("stop_supervisor")
        if seconds:
            clock.sleep(seconds)
        original_stop_supervisor()

    runtime.stop_supervisor = recorded_stop_supervisor  # type: ignore[assignment]
    return calls


def test_restart_at_the_deadline_cannot_push_the_drain_past_the_grant(
    tmp_path: Path, fake_clock, caplog
):
    """The reported overrun: a restart holds the runtime lock to the last instant.

    One enabled plugin that ignores SIGTERM, a simulated restart holding the
    runtime lock until just before the 20 second grant expires, and one
    second spent retiring crash supervision. Every drain step is sized from
    what the deadline still has left, so the drain — the lock waits, the
    SIGTERM grace, the SIGKILL escalation, the reap and the log-thread join
    — ends at or before the grant instead of running a final escalation on a
    grace computed from the original budget.
    """
    clock = fake_clock
    storage, runtime, _procs = _drain_runtime(tmp_path, ["openhop.demo"])
    runtime._lock = _RestartHeldLock(clock, busy_until=19.9)
    attempts = _record_stop_attempts(runtime, clock)
    _retire_supervisor_spending(runtime, clock, seconds=1.0)
    manager = PluginManager(
        storage,
        runtime,
        shutdown_policy=ShutdownPolicy(20, 40, True),
        granted_stop_seconds=20,
    )

    with caplog.at_level(logging.DEBUG):
        manager.stop_all()

    assert clock.monotonic() <= 20.0, (
        f"the drain ended at {clock.monotonic():.2f}s against a 20s grant"
    )
    for attempt in attempts:
        remaining = 20.0 - attempt["at"]
        planned = attempt["timeout"] + PLUGIN_STOP_OVERHEAD_SECONDS + SHUTDOWN_MARGIN_SECONDS
        assert planned <= remaining + 1e-6, (
            f"a stop attempted at {attempt['at']:.2f}s asked for a "
            f"{attempt['timeout']:.2f}s grace with only {remaining:.2f}s left"
        )


def test_each_stop_is_sized_from_the_budget_remaining(tmp_path: Path, fake_clock):
    """The grace of a stop equals what remained when that stop started.

    The simulated restart releases the runtime lock while the drain can
    still afford to wait for it, so the running plugin is resolved and
    stopped. The deadline was anchored before crash supervision was retired,
    so the one second the retirement spends is charged: with 15 of the 20
    granted seconds left, the stop's grace is the 3 seconds that remain after
    the exit margin and the fixed per-plugin cleanup cost — not the 8 seconds
    the original grant would fund.
    """
    clock = fake_clock
    storage, runtime, proc = _drain_runtime(tmp_path, ["openhop.demo"])
    runtime._lock = _RestartHeldLock(clock, busy_until=5.0)
    attempts = _record_stop_attempts(runtime, clock)
    supervisor_calls = _retire_supervisor_spending(runtime, clock, seconds=1.0)
    manager = PluginManager(
        storage,
        runtime,
        shutdown_policy=ShutdownPolicy(20, 40, True),
        granted_stop_seconds=20,
    )

    manager.stop_all()

    assert supervisor_calls == ["stop_supervisor"]
    assert [attempt["plugin_id"] for attempt in attempts] == ["openhop.demo"]
    attempt = attempts[0]
    assert attempt["at"] == 5.0, "the drain waited for the lock within its allowance"
    assert attempt["timeout"] == pytest.approx(3.0)
    assert attempt["lock_wait"] == pytest.approx(0.0)
    assert proc[0].signals == [signal.SIGTERM, signal.SIGKILL]
    assert runtime._handles == {}
    assert clock.monotonic() <= 20.0


def test_a_plugin_the_budget_cannot_fund_is_skipped_by_name(tmp_path: Path, fake_clock, caplog):
    """Nothing left for a plugin: it is skipped, and the warning names it.

    Crash-supervision retirement spends past the whole grant while a
    simulated restart still holds the runtime lock. The lock frees near the
    deadline, but by then nothing of the budget remains, so the drain skips
    both plugins with a warning naming each and stops none of them; their
    processes are left to the deployment's outer wait and kill escalation.
    """
    clock = fake_clock
    storage, runtime, _procs = _drain_runtime(tmp_path, ["openhop.alpha", "openhop.beta"])
    runtime._lock = _RestartHeldLock(clock, busy_until=19.5)
    attempts = _record_stop_attempts(runtime, clock)
    _retire_supervisor_spending(runtime, clock, seconds=25.0)
    manager = PluginManager(
        storage,
        runtime,
        shutdown_policy=ShutdownPolicy(20, 40, True),
        granted_stop_seconds=20,
    )

    with caplog.at_level(logging.DEBUG):
        manager.stop_all()

    assert attempts == [], "a plugin the budget cannot fund must not be stopped"
    warnings = [
        record.getMessage() for record in caplog.records if record.levelno == logging.WARNING
    ]
    assert any("openhop.alpha" in message for message in warnings)
    assert any("openhop.beta" in message for message in warnings)


def test_sufficient_budget_keeps_the_configured_grace_and_order(tmp_path: Path, fake_clock):
    """With a sufficient budget the drain behaves exactly as documented.

    Two plugins that ignore SIGTERM, no lock contention, and a container wait
    that funds the documented requirement: crash supervision is retired
    first, both stops get the configured 20 second grace, each stop's lock
    allowance leaves the later steps' fixed costs and the exit margin, and
    the drain ends with the margin intact.
    """
    clock = fake_clock
    storage, runtime, procs = _drain_runtime(tmp_path, ["openhop.alpha", "openhop.beta"])
    attempts = _record_stop_attempts(runtime, clock)
    supervisor_calls = _retire_supervisor_spending(runtime, clock, seconds=0.0)
    manager = PluginManager(storage, runtime, shutdown_policy=ShutdownPolicy(20, 56, True))

    manager.stop_all()

    assert supervisor_calls == ["stop_supervisor"]
    assert [attempt["plugin_id"] for attempt in attempts] == [
        "openhop.alpha",
        "openhop.beta",
    ]
    assert [attempt["timeout"] for attempt in attempts] == [20.0, 20.0]
    # 56s funds both graces, both fixed costs and the margin exactly: the
    # first allowance leaves the second plugin's fixed cost and the margin,
    # the second its own.
    assert attempts[0]["lock_wait"] == pytest.approx(20.0)
    assert attempts[1]["lock_wait"] == pytest.approx(4.0)
    for stuck in procs:
        assert stuck.signals == [signal.SIGTERM, signal.SIGKILL]
    assert runtime._handles == {}
    assert clock.monotonic() <= 56.0 - SHUTDOWN_MARGIN_SECONDS


def test_stop_all_honours_a_deadline_passed_by_the_entrypoint(tmp_path: Path, fake_clock):
    """A deadline handed in by the manager entrypoint is the drain's budget.

    The entrypoint anchors the deadline when the stop signal is received,
    before it stops the IPC server. A manager told to stop with only 6 of
    the granted 20 seconds left sizes its stop from the 6 seconds, not from
    the grant.
    """
    clock = fake_clock
    storage, runtime, proc = _drain_runtime(tmp_path, ["openhop.demo"])
    attempts = _record_stop_attempts(runtime, clock)
    _retire_supervisor_spending(runtime, clock, seconds=0.0)
    manager = PluginManager(
        storage,
        runtime,
        shutdown_policy=ShutdownPolicy(20, 40, True),
        granted_stop_seconds=20,
    )

    manager.stop_all(deadline=clock.monotonic() + 6.0)

    assert [attempt["plugin_id"] for attempt in attempts] == ["openhop.demo"]
    assert attempts[0]["timeout"] == pytest.approx(0.0)
    assert proc[0].signals == [signal.SIGTERM, signal.SIGKILL]
    assert runtime._handles == {}
    assert clock.monotonic() <= 6.0
