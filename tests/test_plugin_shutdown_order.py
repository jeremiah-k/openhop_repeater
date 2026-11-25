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

from pathlib import Path

from repeater.plugins.manager import PluginManager
from repeater.plugins.manifest import PluginManifest, RuntimeSpec
from repeater.plugins.shutdown_policy import ShutdownPolicy
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

    def running_plugin_ids(self) -> list[str]:
        return ["openhop.alpha"]

    def stop_supervisor(self) -> None:
        self.calls.append("stop_supervisor")

    def stop(self, plugin_id: str, *, timeout=None) -> None:
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
