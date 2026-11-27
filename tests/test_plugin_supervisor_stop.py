"""A supervisor that has been told to stop keeps crashed plugins stopped.

stop_supervisor() sets the stop event; from that moment the crash check
leaves a crashed, enabled plugin in the state an expected stop records
instead of restarting it. While the supervisor runs, the same crash is
restarted. A restart that is in flight when the managed drain begins
completes under the runtime lock, and the drain stops that plugin like
any other; a stop's lock waits share one allowance sized from what is
left of the shutdown budget after the stop's SIGTERM grace, so the lock
waits plus the grace stay inside the budget.
"""

from __future__ import annotations

import io
import os
import signal
import threading
import time
from pathlib import Path

import pytest

from repeater.plugins import runtime as runtime_module
from repeater.plugins.manager import PluginManager
from repeater.plugins.manifest import PluginManifest, RuntimeSpec
from repeater.plugins.runtime import PluginRuntime, PluginState
from repeater.plugins.shutdown_policy import ShutdownPolicy
from repeater.plugins.storage import PluginStorage


@pytest.fixture(autouse=True)
def absent_process_groups(monkeypatch):
    # Fake PIDs must never signal real host processes.
    def raise_absent(*args):
        raise ProcessLookupError

    monkeypatch.setattr(os, "killpg", raise_absent)


class FakeProc:
    def __init__(self, pid: int):
        self.pid = pid
        self._code: int | None = None
        self.signals: list[int] = []
        self.stdout = io.BytesIO(b"")

    def poll(self):
        return self._code

    def send_signal(self, sig):
        self.signals.append(sig)
        self._code = 0

    def kill(self):
        self._code = -9

    def wait(self, timeout=None):
        return self._code

    def exit(self, code=1):
        self._code = code


def _install_enabled_plugin(storage: PluginStorage, plugin_id: str = "openhop.demo") -> None:
    storage.ensure_plugin_layout(plugin_id, "0.1.0")
    storage.write_state(plugin_id, {"version": "0.1.0", "enabled": True})
    storage.write_manifest(
        plugin_id,
        "0.1.0",
        PluginManifest(
            schema=1,
            id=plugin_id,
            name="Demo",
            version="0.1.0",
            runtime=RuntimeSpec(type="python", entrypoint="demo-cli"),
        ),
    )
    storage.set_current(plugin_id, "0.1.0")
    bin_dir = storage.paths_for(plugin_id).venv_dir("0.1.0") / "bin"
    bin_dir.mkdir(parents=True, exist_ok=True)
    (bin_dir / "demo-cli").write_text("x")
    (bin_dir / "demo-cli").chmod(0o755)


def _runtime_with_recorded_spawns(storage: PluginStorage, procs: list[FakeProc]) -> PluginRuntime:
    def fake_popen(cmd, **kwargs):
        p = FakeProc(pid=4000 + len(procs))
        procs.append(p)
        return p

    return PluginRuntime(storage, popen_factory=fake_popen)


def test_stopping_supervisor_leaves_crashed_plugin_stopped(tmp_path: Path):
    storage = PluginStorage(tmp_path / "plugins")
    _install_enabled_plugin(storage)
    procs: list[FakeProc] = []
    runtime = _runtime_with_recorded_spawns(storage, procs)
    runtime.start("openhop.demo")
    procs[0].exit(1)
    runtime._supervise_stop.set()

    runtime._check_crashes()

    assert runtime.get_state("openhop.demo") == PluginState.STOPPED
    assert len(procs) == 1, "a stopping supervisor must not start the plugin"
    assert runtime._crash_times["openhop.demo"], "crash-loop accounting stays in place"


def test_running_supervisor_restarts_crashed_plugin(tmp_path: Path):
    storage = PluginStorage(tmp_path / "plugins")
    _install_enabled_plugin(storage)
    procs: list[FakeProc] = []
    runtime = _runtime_with_recorded_spawns(storage, procs)
    runtime.start("openhop.demo")
    procs[0].exit(1)
    assert not runtime._supervise_stop.is_set()

    runtime._check_crashes()

    assert runtime.get_state("openhop.demo") == PluginState.RUNNING
    assert len(procs) == 2


def test_managed_stop_all_stops_plugin_restarted_in_flight(tmp_path: Path):
    storage = PluginStorage(tmp_path / "plugins")
    _install_enabled_plugin(storage)
    procs: list[FakeProc] = []
    runtime = _runtime_with_recorded_spawns(storage, procs)

    restart_entered = threading.Event()
    restart_release = threading.Event()

    def gated_venv_check(plugin_id: str) -> None:
        restart_entered.set()
        assert restart_release.wait(timeout=30), "restart gate never released"

    runtime.start_supervisor()
    try:
        runtime.start("openhop.demo")
        # Arm the gate only for the supervisor's restart, not the start above.
        runtime.ensure_venv_compatible = gated_venv_check  # type: ignore[assignment]
        procs[0].exit(1)
        assert restart_entered.wait(timeout=10), "supervisor never entered the restart"

        manager = PluginManager(
            storage,
            runtime,
            shutdown_policy=ShutdownPolicy(
                stop_timeout_seconds=5,
                container_shutdown_timeout_seconds=40,
                stop_before_repeater=True,
            ),
        )
        stopper = threading.Thread(target=manager.stop_all, daemon=True)
        stopper.start()
        deadline = time.monotonic() + 10
        while not runtime._supervise_stop.is_set() and time.monotonic() < deadline:
            time.sleep(0.005)
        assert runtime._supervise_stop.is_set(), "stop_all never set the stop event"

        restart_release.set()
        stopper.join(timeout=30)
        assert not stopper.is_alive(), "stop_all never finished"
    finally:
        restart_release.set()
        runtime.stop_supervisor()

    assert runtime.get_state("openhop.demo") == PluginState.STOPPED
    assert runtime.running_plugin_ids() == []


def test_managed_drain_bounds_wait_on_restart_in_flight(tmp_path: Path):
    """The drain waits for an in-flight restart only within its budget.

    A restart that was already running when the drain began holds the
    runtime lock while it completes and can take far longer than one
    plugin's stop cost. The drain stops waiting when the budget runs out
    instead of blocking on the restart, and leaves the leftover process to
    the deployment's outer wait and kill escalation.
    """
    storage = PluginStorage(tmp_path / "plugins")
    _install_enabled_plugin(storage)
    procs: list[FakeProc] = []
    runtime = _runtime_with_recorded_spawns(storage, procs)

    restart_entered = threading.Event()
    restart_release = threading.Event()

    def gated_venv_check(plugin_id: str) -> None:
        restart_entered.set()
        assert restart_release.wait(timeout=60), "restart gate never released"

    budget = 1
    drain_started: list[float] = []
    original_stop_supervisor = runtime.stop_supervisor

    def timed_stop_supervisor() -> None:
        original_stop_supervisor()
        drain_started.append(time.monotonic())

    runtime.stop_supervisor = timed_stop_supervisor  # type: ignore[assignment]
    runtime.start_supervisor()
    try:
        runtime.start("openhop.demo")
        # Arm the gate only for the supervisor's restart, not the start above.
        runtime.ensure_venv_compatible = gated_venv_check  # type: ignore[assignment]
        procs[0].exit(1)
        assert restart_entered.wait(timeout=10), "supervisor never entered the restart"

        manager = PluginManager(
            storage,
            runtime,
            shutdown_policy=ShutdownPolicy(
                stop_timeout_seconds=5,
                container_shutdown_timeout_seconds=40,
                stop_before_repeater=True,
            ),
            granted_stop_seconds=budget,
        )
        stopper = threading.Thread(target=manager.stop_all, daemon=True)
        stopper.start()
        stopper.join(timeout=30)
        assert not stopper.is_alive(), "drain waited for the restart beyond its budget"
        assert drain_started, "stop_all never reached the managed drain"
        drain_elapsed = time.monotonic() - drain_started[0]
        assert drain_elapsed <= budget + 0.5, (
            f"drain spent {drain_elapsed:.2f}s against a {budget}s budget"
        )
        assert not restart_release.is_set(), "drain must not require the restart to finish"
        assert runtime._supervise_stop.is_set(), "drain never retired crash supervision"
        assert len(procs) == 1, "the abandoned restart must not spawn behind the drain"
        assert list(runtime._handles) == []
        assert runtime._runtime_state.get("openhop.demo") == PluginState.STOPPED
    finally:
        restart_release.set()
        runtime.stop_supervisor()
        if runtime.running_plugin_ids():
            runtime.stop("openhop.demo")


def test_stop_shares_one_lock_allowance_across_its_two_waits(tmp_path: Path):
    """One stop's lock waits share a single allowance.

    The SIGTERM grace runs between the two lock acquisitions of a stop. A
    restart that takes the lock during that grace must not reset the
    bounded caller's budget: the second acquisition gets only what is
    left of the same allowance, so one stop never spends more than its
    lock allowance waiting on the runtime lock in total.
    """
    storage = PluginStorage(tmp_path / "plugins")
    _install_enabled_plugin(storage)
    procs: list[FakeProc] = []

    class SlowTerminateProc(FakeProc):
        """A live process whose termination loop runs out its stop grace."""

        def __init__(self, pid: int, terminate_started: threading.Event):
            super().__init__(pid)
            self.process_group = None
            self._terminate_started = terminate_started

        def poll(self):
            # Stay live for the whole SIGTERM grace: the termination loop of
            # _terminate_handle keeps polling until the grace runs out.
            if not self._terminate_started.is_set():
                self._terminate_started.set()
            return None

    terminate_started = threading.Event()
    lock_taken = threading.Event()
    release_lock = threading.Event()

    def fake_popen(cmd, **kwargs):
        p = SlowTerminateProc(pid=4000 + len(procs), terminate_started=terminate_started)
        procs.append(p)
        return p

    runtime = PluginRuntime(storage, popen_factory=fake_popen)
    runtime.start("openhop.demo")
    # Route the fake process through the poll-based termination path so the
    # stop grace actually elapses instead of exiting on the absent group.
    runtime._handles["openhop.demo"].process_group = None

    def restart_takes_the_lock_during_the_grace():
        assert terminate_started.wait(timeout=5), "stop never entered the grace"
        with runtime._lock:
            lock_taken.set()
            assert release_lock.wait(timeout=5), "lock holder never released"

    holder = threading.Thread(target=restart_takes_the_lock_during_the_grace, daemon=True)
    holder.start()

    entered = time.monotonic()
    with pytest.raises(RuntimeError, match="lock busy"):
        runtime.stop("openhop.demo", timeout=0.5, lock_wait=1.0)
    elapsed = time.monotonic() - entered

    release_lock.set()
    holder.join(timeout=5)
    # Retire the stop the ordinary way so the fake process is reaped.
    runtime.stop("openhop.demo", timeout=0.5)

    assert lock_taken.is_set(), "the restart never took the lock during the grace"
    # The stop must not have spent more than its 1.0s allowance on the lock:
    # about 0.5s of SIGTERM grace plus one 0.5s second wait, not the 1.5s an
    # allowance reset after the grace would cost.
    assert 0.9 <= elapsed <= 1.3, f"stop spent {elapsed:.2f}s against a 1.0s allowance"


def test_stop_bounds_every_wait_by_the_deadline_it_is_given(tmp_path: Path, monkeypatch):
    """stop() caps all five of its waits at one monotonic deadline.

    A stuck plugin process, a 5 second stop timeout, a 10 second lock
    allowance and a 3 second deadline: the SIGTERM grace may only run to the
    deadline, the SIGKILL reap wait, the second lock acquisition and the
    log-thread join get only what is left, so one bounded stop never
    outruns the deadline its caller computed from the drain budget.
    """

    class Clock:
        def __init__(self):
            self.now = 0.0

        def monotonic(self):
            return self.now

        def sleep(self, seconds):
            self.now += max(0.0, float(seconds))

    clock = Clock()
    monkeypatch.setattr(runtime_module, "time", clock)

    class ReapingProc(FakeProc):
        """Stays live through the SIGTERM grace; the reap consumes its wait."""

        def __init__(self, pid: int):
            super().__init__(pid)
            self.process_group = None

        def poll(self):
            return self._code

        def send_signal(self, sig):
            # Ignore SIGTERM like a plugin that never drains in time.
            self.signals.append(sig)

        def kill(self):
            self.signals.append(signal.SIGKILL)
            self._code = -9

        def wait(self, timeout=None):
            if timeout is not None:
                clock.sleep(timeout)
            return self._code

    procs: list[ReapingProc] = []

    def fake_popen(cmd, **kwargs):
        proc = ReapingProc(pid=4000 + len(procs))
        procs.append(proc)
        return proc

    storage = PluginStorage(tmp_path / "plugins")
    _install_enabled_plugin(storage)
    runtime = PluginRuntime(storage, popen_factory=fake_popen)
    runtime.start("openhop.demo")
    # Route the fake process through the poll-based termination path so the
    # stop grace actually elapses instead of exiting on the absent group.
    runtime._handles["openhop.demo"].process_group = None

    entered = clock.monotonic()
    state = runtime.stop("openhop.demo", timeout=5, lock_wait=10, deadline=3.0)

    assert state == PluginState.STOPPED
    assert procs[0].signals == [signal.SIGTERM, signal.SIGKILL]
    # The SIGTERM grace was cut at the deadline and nothing was left for a
    # longer reap, second lock wait or log-thread join afterwards.
    assert 2.9 <= clock.monotonic() - entered <= 3.1, (
        f"stop spent {clock.monotonic() - entered:.2f}s against a 3s deadline"
    )
    assert runtime._handles == {}
