"""Policy validation and ordered container shutdown without native hardware."""

from __future__ import annotations

import signal
from types import SimpleNamespace
from unittest.mock import Mock

import pytest

from repeater.plugins.container_supervisor import ContainerSupervisor
from repeater.plugins.manager import PluginManager
from repeater.plugins.shutdown_policy import (
    MANAGER_STOP_TIMEOUT_ENV,
    STOP_TIMEOUT_ENV,
    ShutdownBudgetError,
    ShutdownPolicy,
    advertised_stop_timeout_seconds,
    check_container_budget,
    check_outer_deadline,
    drain_grace_seconds,
    effective_stop_budget,
    effective_container_timeout,
    granted_manager_stop_timeout_seconds,
    max_drainable_plugins,
    managed_shutdown_configured,
    required_container_timeout,
    required_outer_timeout,
    shutdown_policy,
)


def test_existing_defaults_and_opt_in():
    assert shutdown_policy({}) == ShutdownPolicy(5, 8, False)
    assert shutdown_policy(
        {
            "plugins": {
                "stop_timeout_seconds": 20,
                "container_shutdown_timeout_seconds": 40,
                "stop_before_repeater": True,
            }
        }
    ) == ShutdownPolicy(20, 40, True)


@pytest.mark.parametrize(
    "settings",
    [
        {"stop_timeout_seconds": True},
        {"stop_timeout_seconds": 21},
        {"stop_timeout_seconds": 4},
        {"stop_timeout_seconds": 5.0},
        {"container_shutdown_timeout_seconds": 321},
        {"container_shutdown_timeout_seconds": "40"},
        {"stop_before_repeater": "true"},
        {"stop_before_repeater": True},
        {
            "stop_timeout_seconds": 20,
            "container_shutdown_timeout_seconds": 29,
            "stop_before_repeater": True,
        },
    ],
)
def test_unsafe_or_inconsistent_policy_rejected(settings):
    with pytest.raises(ValueError, match="plugins|ordered shutdown"):
        shutdown_policy({"plugins": settings})


# --- D3: the rule must account for the enabled plugin count ---------------


def test_budget_scales_with_enabled_plugin_count():
    # One plugin costs its SIGTERM grace plus the SIGKILL reap, plus the
    # manager's own exit margin.
    assert required_container_timeout(1, 20) == 32
    assert required_container_timeout(2, 20) == 56
    assert required_container_timeout(3, 20) == 80
    assert required_container_timeout(1, 5) == 17


def test_single_plugin_rule_matches_documented_twelve_second_margin():
    # The documented rule for one plugin is "stop timeout plus twelve seconds".
    for stop in (5, 10, 20):
        assert required_container_timeout(1, stop) == stop + 12


@pytest.mark.parametrize("plugin_count", [1, 2, 3])
def test_two_plugins_cannot_reuse_the_single_plugin_budget(plugin_count):
    config = {
        "plugins": {
            "stop_timeout_seconds": 20,
            "container_shutdown_timeout_seconds": 32,
            "stop_before_repeater": True,
        }
    }
    if plugin_count == 1:
        assert shutdown_policy(config, plugin_count=1) == ShutdownPolicy(20, 32, True)
        return
    with pytest.raises(ShutdownBudgetError, match="at least"):
        shutdown_policy(config, plugin_count=plugin_count)


def test_budget_error_names_the_required_seconds_and_count():
    config = {
        "plugins": {
            "stop_timeout_seconds": 20,
            "container_shutdown_timeout_seconds": 32,
            "stop_before_repeater": True,
        }
    }
    with pytest.raises(ShutdownBudgetError) as excinfo:
        shutdown_policy(config, plugin_count=2)
    message = str(excinfo.value)
    assert "56s" in message
    assert "2 enabled service plugin(s)" in message


def test_max_drainable_plugins_reports_the_count_ceiling():
    assert max_drainable_plugins(20, 32) == 1
    assert max_drainable_plugins(20, 56) == 2
    assert max_drainable_plugins(20, 80) == 3
    assert max_drainable_plugins(5, 17) == 1


def test_unmanaged_installs_keep_their_defaults():
    # No managed shutdown keys: the existing five-second grace, eight-second
    # container wait and parallel signalling remain, whatever the plugin count.
    config = {"plugins": {"enabled": True}}
    assert managed_shutdown_configured(config) is False
    assert shutdown_policy(config, plugin_count=3) == ShutdownPolicy(5, 8, False)


def test_drain_grace_is_bounded_by_the_plugin_count():
    policy = ShutdownPolicy(20, 80, True)
    # Three plugins at twenty seconds each fit exactly in 80s, so the full
    # grace is kept.
    assert drain_grace_seconds(policy, 3, 80) == 20
    # A fourth plugin no longer fits, so the per-plugin grace shrinks and the
    # sequential drain still completes inside the budget.
    grace = drain_grace_seconds(policy, 4, 80)
    assert grace < 20
    assert 4 * (grace + 4) + 8 <= 80


def test_drain_grace_never_exceeds_the_configured_stop_timeout():
    policy = ShutdownPolicy(20, 300, True)
    assert drain_grace_seconds(policy, 1, 300) == 20
    assert drain_grace_seconds(policy, 0, 300) == 20


def test_effective_budget_is_capped_by_the_granted_deadline():
    policy = ShutdownPolicy(20, 40, True)
    assert effective_stop_budget(policy, None) == 40
    assert effective_stop_budget(policy, 15) == 15
    assert effective_stop_budget(policy, 90) == 40


# --- D4: the outer deployment deadline must cover the budget ----------------


def test_outer_timeout_must_cover_both_service_waits():
    assert required_outer_timeout(40) == 90
    assert required_outer_timeout(8) == 26


def test_advertised_stop_timeout_is_read_from_the_environment():
    assert advertised_stop_timeout_seconds({}) is None
    assert advertised_stop_timeout_seconds({STOP_TIMEOUT_ENV: "90"}) == 90
    assert advertised_stop_timeout_seconds({STOP_TIMEOUT_ENV: "nonsense"}) is None
    assert advertised_stop_timeout_seconds({STOP_TIMEOUT_ENV: "0"}) is None


def test_granted_manager_stop_timeout_is_read_from_the_environment():
    assert granted_manager_stop_timeout_seconds({}) is None
    assert granted_manager_stop_timeout_seconds({MANAGER_STOP_TIMEOUT_ENV: "40"}) == 40
    assert granted_manager_stop_timeout_seconds({MANAGER_STOP_TIMEOUT_ENV: "x"}) is None


def test_short_outer_deadline_is_reported(caplog):
    with caplog.at_level("WARNING"):
        check_outer_deadline(90, scope="container shutdown", environ={STOP_TIMEOUT_ENV: "15"})
    assert "at least 90s" in caplog.text
    assert "15s" in caplog.text


def test_sufficient_outer_deadline_is_not_reported(caplog):
    with caplog.at_level("WARNING"):
        check_outer_deadline(90, scope="container shutdown", environ={STOP_TIMEOUT_ENV: "90"})
    assert "at least 90s" not in caplog.text


def test_undeclared_outer_deadline_is_reported(caplog):
    with caplog.at_level("WARNING"):
        check_outer_deadline(90, scope="container shutdown", environ={})
    assert "at least 90s" in caplog.text


# --- ordered shutdown behaviour --------------------------------------------


def test_ordered_shutdown_waits_for_manager_before_signalling_repeater():
    events = []
    supervisor = ContainerSupervisor(
        "/tmp/config.yaml",
        manager_enabled=True,
        stop_plugins_first=True,
        shutdown_timeout=40,
        signal_process_group=lambda process, sig: events.append(("signal", process.pid, sig)),
    )
    supervisor.manager_process = SimpleNamespace(
        pid=101, wait=lambda timeout: events.append(("wait", 101, timeout))
    )
    supervisor.repeater_process = SimpleNamespace(
        pid=102, wait=lambda timeout: events.append(("wait", 102, timeout))
    )
    supervisor.request_shutdown(signal.SIGTERM)
    assert events == []  # signal handler requests, normal context waits
    supervisor._finish()
    assert events == [
        ("signal", 101, signal.SIGTERM),
        ("wait", 101, 40),
        ("signal", 102, signal.SIGTERM),
        ("wait", 102, 40),
    ]


def test_manager_stops_crash_supervision_before_any_plugin():
    runtime = Mock()
    storage = Mock()
    storage.list_plugin_ids.return_value = ["one", "two"]
    runtime.running_plugin_ids.return_value = []
    manager = PluginManager(storage, runtime, shutdown_policy=ShutdownPolicy(5, 20, True))
    manager.stop_all()
    assert [call[0] for call in runtime.mock_calls] == [
        "stop_supervisor",
        "running_plugin_ids",
        "stop",
        "stop",
    ]


def test_manager_bounds_the_drain_by_the_granted_deadline():
    runtime = Mock()
    storage = Mock()
    storage.list_plugin_ids.return_value = ["one", "two"]
    storage.list_enabled_service_plugin_ids.return_value = ["one"]
    runtime.running_plugin_ids.return_value = ["one", "two"]
    manager = PluginManager(
        storage, runtime, shutdown_policy=ShutdownPolicy(20, 80, True), granted_stop_seconds=30
    )
    manager.stop_all()
    stops = [call for call in runtime.mock_calls if call[0] == "stop"]
    assert len(stops) == 2
    first, last = stops
    # Each stop is sized from what the granted deadline still has left when
    # it starts: the first owes the second plugin's fixed cost as well, the
    # last only its own cost plus the exit margin. Both keep the whole
    # sequential drain inside the granted deadline.
    assert first.kwargs["timeout"] < 20
    assert 2 * (first.kwargs["timeout"] + 4) + 8 <= 30
    assert last.kwargs["timeout"] < 20
    assert (last.kwargs["timeout"] + 4) + 8 <= 30


def test_outer_grant_caps_both_waits():
    policy = ShutdownPolicy(20, 40, True)
    assert effective_container_timeout(policy, None) == 40
    assert effective_container_timeout(policy, 90) == 40
    assert effective_container_timeout(policy, 50) == 20
    assert effective_container_timeout(policy, 8) == 0
    assert granted_manager_stop_timeout_seconds({MANAGER_STOP_TIMEOUT_ENV: "0"}) == 0


def test_underfunded_container_wait_is_reported_not_refused(caplog):
    """A container wait that cannot fund the enabled plugins is reported.

    The enabled plugin count changes before shutdown, so the count at parse
    time must not refuse the manager from starting. The manager re-derives
    the per-plugin grace from the processes that are actually running.
    """
    policy = ShutdownPolicy(20, 40, True)
    # Two plugins at a 20s grace need 2 * 24 + 8 = 56s, but only 40s is set.
    with caplog.at_level("WARNING"):
        check_container_budget(policy, 2)
    assert "needs 56s" in caplog.text
    assert "2 enabled service plugin(s)" in caplog.text


def test_funded_container_wait_is_not_reported(caplog):
    policy = ShutdownPolicy(20, 56, True)
    with caplog.at_level("WARNING"):
        check_container_budget(policy, 2)
    assert caplog.text == ""
