"""Opt-in bounded plugin shutdown budgets, independent of plugin code."""

from __future__ import annotations

import logging
import os
from dataclasses import dataclass
from typing import Any, Mapping, Optional

logger = logging.getLogger("PluginShutdownPolicy")

# Per-plugin cost of one managed stop beyond its SIGTERM grace: the SIGKILL
# reap of the plugin's process group and the per-handle log-thread join.
PLUGIN_STOP_OVERHEAD_SECONDS = 4

# Fixed margin for the manager's own exit after the last plugin stopped:
# supervisor retirement and IPC/manager exit. This is an allowance, not a
# proof that arbitrary blocked OS operations will terminate.
SHUTDOWN_MARGIN_SECONDS = 8

# Margin the outer deployment needs on top of the container's own waits: the
# second service wait plus the kill escalation.
OUTER_MARGIN_SECONDS = 10

# Through which a deployment declares the stop deadline it grants to the
# repeater process: the systemd TimeoutStopSec of the running unit, the Docker
# stop_grace_period, or the HA add-on stop timeout. The container supervisor
# overrides it for the plugin manager child with the deadline it grants that
# child.
STOP_TIMEOUT_ENV = "OPENHOP_STOP_TIMEOUT_SECONDS"

# Through which the container supervisor declares the deadline it grants the
# plugin manager child it supervises.
MANAGER_STOP_TIMEOUT_ENV = "OPENHOP_MANAGER_STOP_TIMEOUT_SECONDS"


class ShutdownPolicyError(ValueError):
    """The configured shutdown policy is malformed."""


class ShutdownBudgetError(ValueError):
    """The configured shutdown budget cannot cover the enabled plugins."""


@dataclass(frozen=True)
class ShutdownPolicy:
    stop_timeout_seconds: int = 5
    container_shutdown_timeout_seconds: int = 8
    stop_before_repeater: bool = False


def _seconds(config: dict[str, Any], key: str, default: int, maximum: int) -> int:
    value = config.get(key, default)
    if type(value) is not int or not default <= value <= maximum:
        raise ShutdownPolicyError(f"plugins.{key} must be an integer from {default} to {maximum}")
    return value


def required_container_timeout(plugin_count: int, stop_timeout_seconds: int) -> int:
    """Seconds the plugin manager needs to drain plugins and exit.

    The manager stops installed plugins sequentially, so the drain time grows
    with the number of enabled service plugins. One plugin costs its SIGTERM
    grace plus the SIGKILL reap and log-thread join; the fixed margin covers
    the manager's own exit. For one plugin this is the stop timeout plus
    twelve seconds.
    """
    count = max(0, int(plugin_count))
    return count * (stop_timeout_seconds + PLUGIN_STOP_OVERHEAD_SECONDS) + SHUTDOWN_MARGIN_SECONDS


def required_outer_timeout(container_shutdown_timeout_seconds: int) -> int:
    """Seconds the outer deployment must allow for the whole ordered stop.

    The container supervisor waits for the plugin manager and then for the
    repeater, each with the container shutdown timeout, before it escalates.
    """
    return 2 * max(0, int(container_shutdown_timeout_seconds)) + OUTER_MARGIN_SECONDS


def effective_container_timeout(policy: ShutdownPolicy, granted_seconds: Optional[int]) -> int:
    """Conservatively divide a declared outer grant over both service waits."""
    if granted_seconds is None:
        return policy.container_shutdown_timeout_seconds
    return min(
        policy.container_shutdown_timeout_seconds,
        max(0, (granted_seconds - OUTER_MARGIN_SECONDS) // 2),
    )


def max_drainable_plugins(
    stop_timeout_seconds: int, container_shutdown_timeout_seconds: int
) -> int:
    """How many enabled service plugins fit in the container shutdown budget."""
    allowed = 0
    while (
        required_container_timeout(allowed + 1, stop_timeout_seconds)
        <= container_shutdown_timeout_seconds
    ):
        allowed += 1
    return allowed


def managed_shutdown_configured(config: dict[str, Any]) -> bool:
    """Whether the operator explicitly configured the managed shutdown budgets."""
    plugins = config.get("plugins")
    if not isinstance(plugins, dict):
        return False
    if plugins.get("stop_before_repeater") is True:
        return True
    return "stop_timeout_seconds" in plugins or "container_shutdown_timeout_seconds" in plugins


def drain_grace_seconds(policy: ShutdownPolicy, plugin_count: int, budget_seconds: float) -> float:
    """Per-plugin SIGTERM grace that keeps the whole drain inside the budget.

    The manager stops plugins sequentially, so the grace each plugin receives
    is bounded by the shutdown budget divided over the plugins that actually
    run. The bound never exceeds the configured stop timeout.
    """
    count = max(0, int(plugin_count))
    configured = float(policy.stop_timeout_seconds)
    if count == 0:
        return configured
    available = budget_seconds - SHUTDOWN_MARGIN_SECONDS
    per_plugin = available / count - PLUGIN_STOP_OVERHEAD_SECONDS
    return max(0.0, min(configured, per_plugin))


def effective_stop_budget(policy: ShutdownPolicy, granted_seconds: Optional[int] = None) -> float:
    """Seconds the plugin manager may spend draining plugins and exiting.

    The configured container timeout is the budget. A deployment that grants
    less (systemd TimeoutStopSec, Docker stop_grace_period, the HA add-on stop
    timeout) lowers it, so the drain never overruns the deadline the
    deployment actually enforces.
    """
    budget = float(policy.container_shutdown_timeout_seconds)
    if granted_seconds is not None:
        budget = min(budget, float(granted_seconds))
    return budget


def advertised_stop_timeout_seconds(environ: Optional[Mapping[str, str]] = None) -> Optional[int]:
    """Stop deadline the deployment declares for the repeater, if any."""
    env = os.environ if environ is None else environ
    raw = str(env.get(STOP_TIMEOUT_ENV, "")).strip()
    if not raw:
        return None
    try:
        value = int(raw)
    except ValueError:
        logger.warning("%s is not an integer number of seconds; ignoring it", STOP_TIMEOUT_ENV)
        return None
    if value <= 0:
        logger.warning("%s must be positive; ignoring it", STOP_TIMEOUT_ENV)
        return None
    return value


def granted_manager_stop_timeout_seconds(
    environ: Optional[Mapping[str, str]] = None,
) -> Optional[int]:
    """Deadline the container supervisor grants the plugin manager child."""
    env = os.environ if environ is None else environ
    raw = str(env.get(MANAGER_STOP_TIMEOUT_ENV, "")).strip()
    if not raw:
        return None
    try:
        value = int(raw)
    except ValueError:
        logger.warning(
            "%s is not an integer number of seconds; ignoring it", MANAGER_STOP_TIMEOUT_ENV
        )
        return None
    if value < 0:
        logger.warning("%s must be non-negative; ignoring it", MANAGER_STOP_TIMEOUT_ENV)
        return None
    return value


def check_outer_deadline(
    required_seconds: int,
    *,
    scope: str,
    environ: Optional[Mapping[str, str]] = None,
) -> None:
    """Warn when the deployment's stop deadline is shorter than required.

    The deadline belongs to the deployment (systemd TimeoutStopSec, Docker
    stop_grace_period, HA add-on stop timeout), so a mismatch is reported
    rather than silently corrected; the repeater keeps running and the
    operator decides.
    """
    advertised = advertised_stop_timeout_seconds(environ)
    if advertised is None:
        logger.warning(
            "%s needs a stop deadline of at least %ds; declare %s (systemd "
            "TimeoutStopSec, Docker stop_grace_period, HA OPENHOP_ADDON_STOP_TIMEOUT)",
            scope,
            required_seconds,
            STOP_TIMEOUT_ENV,
        )
        return
    if advertised < required_seconds:
        logger.warning(
            "%s needs a stop deadline of at least %ds but the deployment grants %ds; "
            "raise %s (systemd TimeoutStopSec, Docker stop_grace_period, HA "
            "OPENHOP_ADDON_STOP_TIMEOUT) to at least %ds",
            scope,
            required_seconds,
            advertised,
            STOP_TIMEOUT_ENV,
            required_seconds,
        )


def check_shutdown_budget(policy: ShutdownPolicy, plugin_count: int) -> None:
    """Verify the configured budget covers the drain of the enabled plugins.

    The manager stops enabled service plugins sequentially, so the budget must
    cover every plugin, not just the first one.
    """
    required = required_container_timeout(plugin_count, policy.stop_timeout_seconds)
    if policy.container_shutdown_timeout_seconds < required:
        raise ShutdownBudgetError(
            f"plugins.container_shutdown_timeout_seconds must be at least {required}s "
            f"for {plugin_count} enabled service plugin(s) at a "
            f"{policy.stop_timeout_seconds}s plugin stop timeout"
        )


def check_container_budget(policy: ShutdownPolicy, plugin_count: int) -> None:
    """Warn when the configured container wait cannot fund the enabled plugins.

    This is not a refusal. The enabled plugin count changes before shutdown,
    so a stale count must not block the manager from starting; the manager
    re-derives the per-plugin grace from the processes that are actually
    running. The warning tells the operator that the configured stop timeout
    will not be honoured in full for the plugins enabled right now.
    """
    required = required_container_timeout(plugin_count, policy.stop_timeout_seconds)
    if policy.container_shutdown_timeout_seconds < required:
        logger.warning(
            "plugins.container_shutdown_timeout_seconds is %ds but draining %d enabled "
            "service plugin(s) at a %ds plugin stop timeout needs %ds; the per-plugin "
            "grace will be reduced to fit",
            policy.container_shutdown_timeout_seconds,
            plugin_count,
            policy.stop_timeout_seconds,
            required,
        )


def shutdown_policy(config: dict[str, Any], *, plugin_count: int = 1) -> ShutdownPolicy:
    """Parse and validate the managed shutdown policy.

    Raises ShutdownPolicyError for a malformed policy and ShutdownBudgetError
    when the configured container timeout cannot cover the drain of
    ``plugin_count`` enabled service plugins.
    """
    plugins = config.get("plugins")
    if not isinstance(plugins, dict):
        return ShutdownPolicy()
    # Ordinary lifecycle IPC has a 30s deadline. Leave margin for the
    # subsequent process reap and log-thread join; do not turn stop into an
    # unbounded operation or change the IPC completion protocol here.
    stop = _seconds(plugins, "stop_timeout_seconds", 5, 20)
    container = _seconds(plugins, "container_shutdown_timeout_seconds", 8, 300)
    ordered = plugins.get("stop_before_repeater", False)
    if type(ordered) is not bool:
        raise ShutdownPolicyError("plugins.stop_before_repeater must be a boolean")
    policy = ShutdownPolicy(stop, container, ordered)
    if managed_shutdown_configured(config):
        check_shutdown_budget(policy, plugin_count)
    return policy
