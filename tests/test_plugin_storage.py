"""Tests for plugin filesystem layout and state."""

from pathlib import Path

import pytest

from repeater.plugins.manifest import PluginManifest, RuntimeSpec
from repeater.plugins.storage import PluginStorage, safe_join


def test_safe_path_generation(tmp_path: Path):
    storage = PluginStorage(tmp_path / "plugins")
    paths = storage.paths_for("openhop.demo")
    assert paths.data_dir == storage.root / "openhop.demo" / "data"
    assert paths.venv_dir("1.0.0").name == "venv"


def test_safe_join_rejects_traversal(tmp_path: Path):
    root = tmp_path / "root"
    root.mkdir()
    (root / "ok.txt").write_text("x")
    assert safe_join(root, "ok.txt").is_file()
    with pytest.raises(ValueError):
        safe_join(root, "..", "etc", "passwd")


def test_version_dirs_separate_and_data_survives(tmp_path: Path):
    storage = PluginStorage(tmp_path / "plugins")
    plugin_id = "openhop.demo"
    storage.ensure_plugin_layout(plugin_id, "1.0.0")
    storage.ensure_plugin_layout(plugin_id, "2.0.0")
    paths = storage.paths_for(plugin_id)
    (paths.data_dir / "config.json").write_text('{"a":1}')
    storage.write_state(plugin_id, {"version": "1.0.0", "enabled": False})
    storage.write_manifest(
        plugin_id,
        "1.0.0",
        PluginManifest(
            schema=1,
            id=plugin_id,
            name="Demo",
            version="1.0.0",
            runtime=RuntimeSpec(type="python", entrypoint="demo"),
        ),
    )
    storage.set_current(plugin_id, "1.0.0")

    assert paths.release_dir("1.0.0").is_dir()
    assert paths.release_dir("2.0.0").is_dir()
    assert (paths.data_dir / "config.json").read_text() == '{"a":1}'

    # Reinstall/remove code keeps data by default
    storage.remove_release_code(plugin_id, keep_data=True)
    assert (paths.data_dir / "config.json").is_file()
    assert not paths.releases_dir.exists()
    assert storage.read_state(plugin_id) is None


def test_config_read_write_roundtrip(tmp_path: Path):
    storage = PluginStorage(tmp_path / "plugins")
    plugin_id = "openhop.demo"
    storage.ensure_plugin_layout(plugin_id, "1.0.0")
    storage.write_state(plugin_id, {"version": "1.0.0", "enabled": False})

    assert storage.read_config(plugin_id) == {}
    storage.write_config(plugin_id, {"nomad_url": "http://127.0.0.1:8080", "one_shot": True})
    assert storage.read_config(plugin_id)["nomad_url"] == "http://127.0.0.1:8080"
    assert storage.config_path(plugin_id).is_file()


def test_uninstall_delete_data(tmp_path: Path):
    storage = PluginStorage(tmp_path / "plugins")
    plugin_id = "openhop.demo"
    storage.ensure_plugin_layout(plugin_id, "1.0.0")
    paths = storage.paths_for(plugin_id)
    (paths.data_dir / "x").write_text("y")
    storage.write_state(plugin_id, {"version": "1.0.0", "enabled": True})
    storage.remove_release_code(plugin_id, keep_data=False)
    assert not paths.data_dir.exists()


def test_runtime_file_has_a_bounded_regular_file_contract(tmp_path):
    import os

    storage = PluginStorage(tmp_path / "plugins")
    plugin_id = "openhop.demo"
    storage.ensure_plugin_layout(plugin_id, "1.0.0")
    path = storage.runtime_path(plugin_id)
    assert storage.read_runtime(plugin_id) == {}
    path.write_text('{"ok": true}')
    assert storage.read_runtime(plugin_id) == {"ok": True}
    path.write_bytes(b" " * (64 * 1024 + 1))
    with pytest.raises(ValueError, match="byte limit"):
        storage.read_runtime(plugin_id)
    path.unlink()
    path.symlink_to(tmp_path / "other")
    (tmp_path / "other").write_text('{"private": true}')
    with pytest.raises(ValueError, match="invalid"):
        storage.read_runtime(plugin_id)
    path.unlink()
    os.mkfifo(path)
    with pytest.raises(ValueError, match="regular file"):
        storage.read_runtime(plugin_id)


def test_runtime_read_refuses_a_symlinked_data_directory(tmp_path):
    storage = PluginStorage(tmp_path / "plugins")
    plugin_id = "openhop.demo"
    storage.ensure_plugin_layout(plugin_id, "1.0.0")
    paths = storage.paths_for(plugin_id)
    outside = tmp_path / "outside"
    outside.mkdir()
    (outside / "runtime.json").write_text('{"ok": true}')
    paths.data_dir.rmdir()
    paths.data_dir.symlink_to(outside)
    with pytest.raises(ValueError, match="invalid"):
        storage.read_runtime(plugin_id)


def test_runtime_read_refuses_a_symlinked_plugin_directory(tmp_path):
    import shutil

    storage = PluginStorage(tmp_path / "plugins")
    plugin_id = "openhop.demo"
    storage.ensure_plugin_layout(plugin_id, "1.0.0")
    paths = storage.paths_for(plugin_id)
    outside = tmp_path / "outside"
    (outside / "data").mkdir(parents=True)
    (outside / "data" / "runtime.json").write_text('{"ok": true}')
    shutil.rmtree(paths.plugin_dir)
    paths.plugin_dir.symlink_to(outside)
    with pytest.raises(ValueError, match="invalid"):
        storage.read_runtime(plugin_id)


def test_runtime_parse_normalises_digit_limit_and_rejects_null(tmp_path):
    storage = PluginStorage(tmp_path / "plugins")
    plugin_id = "openhop.demo"
    storage.ensure_plugin_layout(plugin_id, "1.0.0")
    path = storage.runtime_path(plugin_id)
    # An integer above Python's digit limit raises a plain ValueError that
    # must not escape the promised invalid-runtime error.
    path.write_bytes(b'{"n": 1' + b"0" * 5000 + b"}")
    with pytest.raises(ValueError, match="invalid"):
        storage.read_runtime(plugin_id)
    # The documented observation is a JSON object: a parsed null is invalid;
    # only a missing file reads as empty.
    path.write_text("null")
    with pytest.raises(ValueError, match="JSON object"):
        storage.read_runtime(plugin_id)
    path.unlink()
    assert storage.read_runtime(plugin_id) == {}
