# openHop Plugins

The plugin system installs and supervises **external** applications around the
Repeater. Plugins are not imported into the Repeater process. They talk to
existing interfaces (REST, WebSocket/SSE, Companion frame server, static web).

## Trust and access

Plugins are trusted applications, **not sandboxed code**. A separate process and
virtualenv keep Python dependencies separate; they do not create a filesystem,
network, or credential boundary. Install only plugins and dependencies you trust.
Native plugins currently run under the `repeater` service account; container
plugins run under the configured container account. Never run a plugin manager as
root merely to work around installation permissions.

The current Repeater API tokens are administrator-equivalent. A token that can
read status can also administer plugins, install uploaded wheels, and read plugin
settings. Do not hand these tokens to untrusted or telemetry-only consumers.
Scoped integration credentials would require a separate authorization feature.

Schema-2 catalogue checksums approve the plugin wheel itself. Python dependencies
resolved by pip are not covered by that checksum and can change on installation
or environment rebuild. The curated catalogue is not a claim that the complete
transitive dependency set is hash-locked. Deployments requiring reproducibility
should use a controlled, pinned dependency source.

## Layout

Default root (under Repeater storage):

```text
/var/lib/openhop_repeater/plugins/
└── openhop.nomad/
    ├── releases/0.1.0/
    │   ├── openhop-plugin.json
    │   ├── ui/                 # optional application UI assets
    │   └── venv/               # isolated Python environment
    ├── current -> releases/0.1.0
    ├── data/                   # persistent plugin-owned data
    ├── logs/plugin.log
    └── state.json              # manager metadata only (id, version, enabled)
```

- Code and virtualenv are **version-specific**.
- The installed wheel is retained with each release so its virtualenv can be
  rebuilt automatically if a future container image changes Python versions.
- `data/` is **not** version-specific and is never interpreted by the manager.
- IPC socket default: `/var/lib/openhop_repeater/plugin-manager.sock`.

Override with config:

```yaml
plugins:
  enabled: true
  # root: "/var/lib/openhop_repeater/plugins"
  # socket: "/var/lib/openhop_repeater/plugin-manager.sock"
```

## Manifest (`openhop-plugin.json`)

Schema version **1**:

```json
{
  "schema": 1,
  "id": "openhop.nomad",
  "name": "NOMAD Bridge",
  "version": "0.1.0",
  "runtime": {
    "type": "python",
    "entrypoint": "meshcore-nomad-bridge"
  }
}
```

UI-only application plugin:

```json
{
  "schema": 1,
  "id": "openhop.console",
  "name": "openHop Console",
  "version": "1.0.0",
  "ui": {
    "type": "application",
    "entry": "ui/index.html"
  }
}
```

A plugin may declare both `runtime` and `ui`.

The UI entry must be inside a dedicated asset subtree such as `ui/index.html`.
A release-root entry (`index.html`), dot/parent path segments, and reserved
runtime/storage subtrees (`venv`, `data`, `logs`, `releases`, `current`) are
rejected. Existing plugins using these invalid layouts must be repackaged;
serving the whole release directory is intentionally no longer supported.

## Plugin manager service

Native installs:

```bash
sudo cp openhop-plugin-manager.service /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable --now openhop-plugin-manager
```

The manager starts enabled service plugins on boot and supervises crashes
(restarts with a simple crash-loop limit: 5 unexpected exits in 60s → `FAILED`).
With the managed shutdown policy enabled, crash supervision is retired before
the first plugin is stopped, so a plugin that crashes while another plugin is
being stopped is not started again during the drain. A restart that is in
flight when the drain begins completes first, and the drain stops that plugin
before the manager exits. Without that policy the
manager keeps the upstream order: it stops the plugins first and retires crash
supervision afterwards.

Repeater does **not** require the manager. If the socket is missing, plugin API
calls return HTTP 503 and normal Repeater operation continues.

The Docker image runs Repeater and the manager under a small process supervisor,
with `tini` as PID 1 to reap orphaned children. It forwards shutdown signals to
both services and their plugin processes, and restarts the manager if it exits
unexpectedly while Repeater remains running. Set `OPENHOP_PLUGIN_MANAGER=0` to
run Repeater without the manager.

### Native upgrade prerequisite

Existing native installations must run the updated `manage.sh upgrade` once as
an administrator to replace the privileged OTA helper. Installing only a new
Python wheel cannot safely replace an existing root-owned helper. Subsequent
OTA upgrades through the refreshed helper install the manager unit from the
installed package; no source checkout is required on the host.

The helper runs from a trusted directory with a clean environment and isolated
Python module lookup. It rejects writable/non-root-owned execution trees instead
of silently granting existing code root ownership. If this check fails, an
administrator must restore the application environment from a trusted package.
Do not bypass the check or make the venv service-writable.

## Operation safety and limits

The manager serves bounded concurrent IPC requests, so a slow installation does
not monopolize the socket. Mutations of the same plugin are serialized;
conflicting requests return a busy response rather than interleaving state.
Local wheel inputs are copied to manager-owned staging before installation.

Installation has a longer IPC budget than ordinary status requests. If transport
is lost after dispatch, HTTP 504 with `outcome: unknown` means the operation may
still finish: **it does not mean cancellation or rollback**. Inspect plugin
status before retrying. An uploaded source is retained when completion is
ambiguous so an accepted install cannot lose its input.

Venv creation and pip execution are time-limited, installer output is bounded,
and plugin stdout/stderr are drained into bounded logs while the plugin runs.
Stop/disable terminate the supervised process group, including ordinary child
workers. This is process supervision, not containment of malicious applications
that deliberately detach into new sessions or write other files.

Failed Python-version rebuilds restore the previous venv and remain retryable;
the new marker is accepted only after the new environment and entrypoint exist.
A rebuild-intent record is reconciled before version checks after a manager
restart, so interruption during replacement cannot silently accept a partial
venv. This provides process-interruption recovery, not guaranteed filesystem
durability across power loss; interruption during cleanup can leave an unused
backup.

Archive extraction limits expanded member size and total size and rejects
symlink-backed destinations. Catalogue JSON and wheel response bodies have size
limits and elapsed-time budgets; configured catalogue URLs must use HTTPS.
DNS resolution and urllib connection/header setup are not guaranteed to obey a
strict whole-request wall-clock deadline.

## Install a local wheel

Via API (authenticated):

```bash
curl -X POST -H "Authorization: Bearer $TOKEN" \
  -F "wheel=@openhop_nomad_plugin-0.1.0-py3-none-any.whl" \
  http://127.0.0.1:8000/api/plugins/install
```

Or JSON with a host path:

```bash
curl -X POST -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"wheel_path":"/tmp/plugin.whl"}' \
  http://127.0.0.1:8000/api/plugins/install
```

Installation creates an isolated venv under the plugin release directory and
**never** mutates the Repeater virtualenv.

## Enable / disable / lifecycle

```bash
# Enable (also starts service plugins)
curl -X POST -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"id":"openhop.nomad"}' \
  http://127.0.0.1:8000/api/plugins/enable

curl -X POST .../api/plugins/disable   -d '{"id":"openhop.nomad"}'
curl -X POST .../api/plugins/start     -d '{"id":"openhop.nomad"}'
curl -X POST .../api/plugins/stop      -d '{"id":"openhop.nomad"}'
curl -X POST .../api/plugins/restart   -d '{"id":"openhop.nomad"}'
```

List / status / logs:

```bash
curl -H "Authorization: Bearer $TOKEN" http://127.0.0.1:8000/api/plugins/
curl -H "Authorization: Bearer $TOKEN" http://127.0.0.1:8000/api/plugins/openhop.nomad
curl -H "Authorization: Bearer $TOKEN" \
  "http://127.0.0.1:8000/api/plugins/logs?id=openhop.nomad&tail=100"
```

## Plugin configuration

Each plugin owns `$OPENHOP_PLUGIN_DATA/config.json`. The manager treats it as an
opaque JSON object — it does not validate plugin-specific keys.

Plugins can declare editor defaults in the manifest:

```json
{
  "config": {
    "defaults": {
      "meshcore_host": "127.0.0.1",
      "nomad_url": "http://127.0.0.1:8080"
    }
  }
}
```

Optionally ship `config.default.json` next to `openhop-plugin.json` in the wheel
(`share/openhop/plugins/<id>/`). On install, if `data/config.json` is missing,
the manager seeds it from those defaults. The settings UI shows defaults when no
saved config exists, and offers **Reset to defaults**.

API:

```bash
# Read
curl -H "Authorization: Bearer $TOKEN" \
  "http://127.0.0.1:8000/api/plugins/settings?id=openhop.nomad"

# Write (optionally restart service plugins)
curl -X POST -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{
    "id": "openhop.nomad",
    "restart": true,
    "config": {
      "nomad_url": "http://192.168.0.170:8080",
      "nomad_model": "qwen2.5:3b-instruct",
      "meshcore_host": "127.0.0.1",
      "meshcore_port": 5001
    }
  }' \
  http://127.0.0.1:8000/api/plugins/settings
```

The Plugins page in the web UI provides a Settings dialog for the same file.

## Service plugin environment

Started processes receive:

| Variable | Example |
|----------|---------|
| `OPENHOP_PLUGIN_ID` | `openhop.nomad` |
| `OPENHOP_PLUGIN_DATA` | `/var/lib/openhop_repeater/plugins/openhop.nomad/data` |

Plugin-specific configuration (for example NOMAD `config.json`) lives under
`$OPENHOP_PLUGIN_DATA` and is owned by the plugin.

## Application UI plugins

Enabled UI plugins are served by Repeater at:

```text
/plugins/{id}/
/plugins/{id}/assets/...
```

Path traversal is rejected. Missing paths fall back to the manifest `ui.entry`
document (SPA-friendly). Disabled plugins are not exposed.

## Uninstall

Default **keeps** `data/`:

```bash
curl -X DELETE -H "Authorization: Bearer $TOKEN" \
  http://127.0.0.1:8000/api/plugins/openhop.nomad
```

Delete data as well:

```bash
curl -X DELETE -H "Authorization: Bearer $TOKEN" \
  "http://127.0.0.1:8000/api/plugins/openhop.nomad?delete_data=true"
```

## NOMAD acceptance

1. Build the `openhop-nomad-plugin` wheel.
2. Install via `/api/plugins/install`.
3. Enable a Repeater Companion frame server reachable from the plugin. In the
   standard Docker image, `127.0.0.1:5001` reaches the same container.
4. Write `$OPENHOP_PLUGIN_DATA/config.json` with `nomad_url` / `nomad_model`.
5. Enable the plugin — `meshcore-nomad-bridge` should report `RUNNING`.
6. Confirm its logs show a successful Companion connection, not only a running
   retry loop. A `RUNNING` process can still be reconnecting to a disabled frame
   server.

## Future work (not in this version)

- Web Component dashboard widgets and settings panels
- Permission scopes, signing, non-Python runtimes
- Plugin SDK

## Plugin catalogue

Repeater browses the curated static catalogue from openHop's R2-backed endpoint.
Update checks read only that catalogue and do not query the GitHub API. When a
user installs an approved plugin, Repeater downloads the exact wheel directly
from the plugin's GitHub Release.

Default catalogue URL:

```text
https://repeater-plugins.openhop.dev/catalogue.json
```

Override with:

```yaml
plugins:
  catalogue_url: "https://repeater-plugins.openhop.dev/catalogue.json"
```

Catalogue schema 2 records each plugin's currently approved `version`, exact
GitHub Release `wheel_url`, and lowercase `sha256`. The manager validates that
the URL belongs to the plugin repository, follows only GitHub's HTTPS release
asset redirect, verifies the checksum before installation, and confirms the
wheel manifest ID and version match the catalogue. Publishing a newer GitHub
Release does not make it available until the catalogue maintainers approve its
metadata in the R2 catalogue.

Schema 1 repository-only catalogues remain readable for compatibility with
custom deployments, but the default openHop catalogue uses schema 2 and needs
no GitHub token.


```bash
# List catalogue (annotated with installed / latest when available)
curl -H "Authorization: Bearer $TOKEN" \
  http://127.0.0.1:8000/api/plugins/catalogue

# Install the currently approved catalogue version (enabled by default)
curl -X POST -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"id":"openhop.nomad"}' \
  http://127.0.0.1:8000/api/plugins/catalogue_install

# Check for updates
curl -H "Authorization: Bearer $TOKEN" \
  "http://127.0.0.1:8000/api/plugins/updates?id=openhop.nomad"

# Apply update
curl -X POST -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"id":"openhop.nomad"}' \
  http://127.0.0.1:8000/api/plugins/update
```

R2 catalogue or GitHub Release download outages do not affect already-installed
plugins or Repeater startup. Local `.whl` install continues to work unchanged.


## Optional longer service shutdown

Plugins that drain durable work before exiting may need more than the default
five-second stop budget. The following is an opt-in candidate deployment policy:

```yaml
plugins:
  enabled: true
  stop_timeout_seconds: 20
  container_shutdown_timeout_seconds: 40
  stop_before_repeater: true
```

The first setting is the per-plugin SIGTERM grace (integer 5–20s). The limit
preserves margin within the existing 30-second ordinary lifecycle IPC budget;
process reap and log-thread join also take time. The second setting is the
container supervisor's per-service wait (integer 8–300s). The last setting
stops the manager before signalling the repeater, keeping radio services
available during normal plugin drain. With all settings omitted existing
five-second plugin grace, eight-second container wait and parallel signalling
remain. Manager shutdown retires crash supervision before stopping installed
plugins only when the policy is enabled; otherwise it stops the plugins first
and retires crash supervision afterwards.

Size the container wait for every managed process that might be running,
including manually started services. Each sequential stop needs its SIGTERM
grace, up to two seconds for SIGKILL reaping and two for log-thread joining;
the manager reserves another eight seconds. The planning allowance is
`plugin_count × (stop_timeout_seconds + 4) + 8` (32s, 56s and 80s for
one, two and three plugins at 20s). Configuration parsing checks a one-plugin
minimum; a larger live process count is not rejected at enable time. When the
configured container wait cannot fund the plugins enabled at startup, the
manager logs a warning naming the required seconds and the plugin count. At
shutdown the manager measures the actual process handles and shares the
available grace among them. This is best-effort when the outer grant is
insufficient even for fixed cleanup overhead.

The outer deadline must cover the whole ordered stop: the container supervisor
waits for the plugin manager and then for the repeater, each with the
container wait, so the deployment needs at least
`2 × container_shutdown_timeout_seconds + 10` seconds (90s for the example
above). The deployment declares that deadline in
`OPENHOP_STOP_TIMEOUT_SECONDS`; Compose sets it from the same variable that
configures `stop_grace_period`, and the systemd manager unit ships
`OPENHOP_MANAGER_STOP_TIMEOUT_SECONDS` alongside its `TimeoutStopSec`. The
repeater warns when the declared deadline is shorter than the budget it
needs, caps each container wait against the declared outer grant and reduces
per-plugin grace accordingly. An undersized grant is not a cleanup guarantee. Raise the matching outer timeout as well:
Compose `stop_grace_period`, the systemd `TimeoutStopSec` of the manager unit
and the repeater unit, and the HA add-on's `OPENHOP_ADDON_STOP_TIMEOUT`. The
HA Supervisor has an outer lifecycle budget of its own that must also be
qualified. Installations that do not enable the managed shutdown policy keep
their existing timeouts and defaults.

These are time allowances, not proof of successful application drain. Manager
shutdown visits installed plugins sequentially, and may include pending IPC
workers; size the outer deadline for all enabled services. A forcibly killed
manager cannot guarantee cleanup of independent plugin process groups. Before
qualifying whole-host shutdown, explicitly stop each managed plugin that
persists state while the repeater is alive, and confirm a clean exit. Do not treat an IPC timeout as cancellation;
inspect status before retrying. No plugin can select its own unbounded timeout
through its manifest.
