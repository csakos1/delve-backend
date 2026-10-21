# delve-backend

[![ci](https://github.com/csakos1/delve-backend/actions/workflows/ci.yml/badge.svg)](https://github.com/csakos1/delve-backend/actions/workflows/ci.yml)

Backend for **Delve**, a personal Android client for a self-hosted AI agent. It runs on the
same machine as the agent and is the only component the phone talks to.

- Client of the [Hermes Agent](https://github.com/NousResearch/hermes-agent) API server
  (pinned version, contract in [`delve-infra/docs/protocol.md`](https://github.com/csakos1/delve-infra/blob/main/docs/protocol.md)).
  Hermes is never imported or patched.
- Durable state in PostgreSQL: conversations, runs and their events, tool steps,
  approvals (including late approval after the Hermes timeout), cards, schedules.
- A single FIFO run queue (the model server runs one generation at a time).
- Per-conversation *thinking*: thinking conversations send a per-run `provider`/`model`
  route; the reasoning text is synced from the session messages after the run.
- Reachable only over Tailscale (`tailscale serve` → `127.0.0.1:8700`).

Infrastructure (model server unit, Hermes configuration fragments, sudoers helpers, protocol
fixtures) lives in [`delve-infra`](https://github.com/csakos1/delve-infra).

## Status

| Step | Content | State |
|---|---|---|
| 1 | Repository skeleton, config, logging, database schema (Alembic) | done |
| 2 | SSE parser, Hermes client, fake Hermes for tests | done |
| 3 | Run queue, state machine, minimal REST + WebSocket, device tokens | done |
| 4 | Tool steps, durable approvals, approval answers over WebSocket and REST | done |
| 5 | Approval expiry, late approval, reconciliation after a gateway restart | done |
| 6 | Token totals, context fill, per-run throughput | done |
| 7 | Pairing with a one-time code, FCM token, status for the app | done |
| 8 | Push notifications (FCM), per-device notification settings, list preview | done |
| 9 | Card ingest from the agent's `events.jsonl`, `GET /cards`, `card.new`, water-level push | done |
| 10 | System view (Hermes, llama-server, GPUs, queue), llama-server stop/start with a held queue | done |
| 11 | Schedules (cron, catch-up, run now) and the morning brief card; brief push | done |
| 12 | Observability: run, approval and token series, GPU sampling, retention cleanup | done |

## Development

Python ≥ 3.12.

```sh
python -m venv .venv && . .venv/bin/activate
pip install -e '.[dev]'
git config core.hooksPath .githooks     # secret / identifier guard, see below

ruff check . && ruff format --check .
mypy
pytest                                  # PostgreSQL tests are skipped without a database
```

### Database tests

Tests marked `pg` run against a real PostgreSQL ≥ 16 when `DELVE_TEST_DATABASE_URL` points
to an **empty, disposable** database (they create and drop the whole schema):

```sh
DELVE_TEST_DATABASE_URL=postgresql+asyncpg://delve_test:delve_test@127.0.0.1/delve_test pytest
```

### Migrations

```sh
alembic upgrade head            # uses DELVE_DATABASE_URL
alembic check                   # models and migrations must not drift (CI enforces this)
```

## Configuration

All settings come from the environment; in production from `/etc/delve/env`
(see [`deploy/delve.env.example`](deploy/delve.env.example)). The database uses peer
authentication over the unix socket, so there is no database password.

## Running

```sh
alembic upgrade head
delve-backend                               # or: delve-backend serve
delve-backend pair                          # one-time code for the app, e.g. 4821-0937
```

`delve-backend pair [--name NAME] [--ttl MINUTES]` prints an 8-digit code, valid once for
10 minutes by default; a new code replaces any unused one. The app sends it to
`POST /api/v1/devices/register` and gets its device token. Without the app,
`delve-backend token create --name pixel` prints a token directly. `delve-backend token
list` and `delve-backend token revoke <device-id>` manage devices (revoking also drops the
FCM token). Only SHA-256 hashes of codes and tokens are stored. These commands need only
`DELVE_DATABASE_URL`.

The installed service listens on `127.0.0.1:8700`; a development backend next to it
needs another port, e.g. `DELVE_BIND=127.0.0.1:8701` in its environment.

## Deployment

Production runs as the `delve` system user from `/opt/delve/current`, as a hardened system
unit (unit file, user and env helper in
[`delve-infra`](https://github.com/csakos1/delve-infra), installation steps 11–13).

```sh
sudo deploy/install.sh [REV]     # build /opt/delve/releases/<sha>, migrate, switch, restart
sudo delve pair                  # the CLI as `delve` with /etc/delve/env (deploy/delve)
```

A release is a venv built from `git archive REV` plus its migrations; the newest three are
kept besides the current one, and installing an older sha switches back to it (without
downgrading the schema).

## API

Every endpoint except `/api/v1/health`, `/api/v1/meta` and `/api/v1/devices/register`
needs `Authorization: Bearer <device token>`.

| Method | Path | |
|---|---|---|
| `POST` | `/api/v1/devices/register` | `{code, name?, app_version?}` → 201 `{device_id, name, token}`; 400 `invalid_code`, 429 after 10 failures a minute |
| `GET` | `/api/v1/devices/me` | `{id, name, created_at, last_seen_at, fcm, push, notify}`; `push`: the server can send pushes |
| `PATCH` | `/api/v1/devices/me` | `{notify: {approvals?, run_done?, water?, brief?}}` (booleans; left-out keys keep their value) → the device |
| `PUT` | `/api/v1/devices/me/fcm` | `{token}` (`null` clears) → 204 |
| `DELETE` | `/api/v1/devices/me` | unpair: revokes the calling token (and its FCM token) → 204 |
| `GET` | `/api/v1/status` | `{server_version, api_revision, revision, hermes, hermes_version, thinking_route}` |
| `GET` | `/api/v1/conversations?limit=&cursor=&archived=` | newest first, keyset cursor |
| `GET` | `/api/v1/conversations/{id}` | one conversation |
| `GET` | `/api/v1/conversations/{id}/messages?before=&limit=` | oldest first |
| `GET` | `/api/v1/conversations/{id}/usage?limit=` | totals, context, runs with `usage`/`perf` |
| `PATCH` | `/api/v1/conversations/{id}` | `{thinking?, archived?}` |
| `GET` | `/api/v1/approvals?status=pending,expired_in_hermes` | approval inbox |
| `POST` | `/api/v1/approvals/{request_id}` | `{choice: once\|session\|deny}` |
| `GET` | `/api/v1/cards?type=&since=&limit=` | cards newest first (`ts`); `since` with a timezone |
| `GET` | `/api/v1/runs/open` | the queue: active run first, then queued ones; each with `position` (runs ahead) |
| `GET` | `/api/v1/system` | the `system` view (below); one answer is reused for 2 s |
| `POST` | `/api/v1/system/llama` | `{action: start\|stop, force?}` → the `system` view; 409 `run_active` (stop without `force` while a run is active), 503 `helper_unavailable`, 502 `helper_failed` |
| `GET` | `/api/v1/schedules` | `{data: [schedule]}`, the morning brief first |
| `POST` | `/api/v1/schedules` | `{name, cron, prompt, conversation_mode?: new\|fixed, conversation_id?, thinking?, enabled?}` → 201 `schedule`; 422 `invalid_cron`, `too_frequent`, `invalid_name`, `invalid_prompt`, `conversation_required`, `conversation_not_found`; 409 `too_many` (50) |
| `PATCH` | `/api/v1/schedules/{id}` | any of the fields above → `schedule`; the brief takes only `cron` and `enabled` (422 `brief_fields`) |
| `DELETE` | `/api/v1/schedules/{id}` | → 204; the brief: 409 `brief_fixed` |
| `POST` | `/api/v1/schedules/{id}/run` | fire once now (timing unchanged) → `{schedule, run_id, card_id}` |
| `GET` | `/api/v1/stats?range=24h\|7d\|30d` | the Observability view (below); 422 for another range |
| `GET` | `/api/v1/meta` | no auth: `{server_version, api_revision, revision, python, …}` |
| `WS` | `/api/v1/ws` | see below |

### WebSocket

Frames are `{"type", "id"?, "data"}`. The first frame must be
`hello {device_token, last_seq?, epoch?}`; the answer is
`hello.ok {server_version, api_revision, revision, hermes, hermes_version, thinking_route,
epoch, seq, resync_required}`, followed by any
events after `last_seq`. Server events carry a global `seq`; the ring buffer keeps 10 000
events for up to an hour, and a new `epoch` after a restart means: reload over REST.

`hermes` is `ok`, `down` or `null` (not polled yet); `thinking_route` is `{ok, run_id, at}`
from the last thinking run (whether Hermes used the thinking provider), or `null`.

| Client frame | Reply |
|---|---|
| `chat.send {text, client_msg_id, conversation_id?, thinking?}` | `ack {conversation_id, message_id, run_id, replayed}` |
| `conversation.set {conversation_id, thinking?, archived?}` | `ack {conversation}` |
| `run.stop {run_id}` (queue id or Hermes id) | `ack {status}` |
| `approval.respond {request_id, choice}` | `ack {approval}` |
| `sync {conversation_id, after_message_id?}` | `ack {conversation, messages, has_more}` |
| `ping` | `pong` |

Server events: `conversation.created`, `conversation.updated`, `message.created`,
`message.delta`, `message.interim`, `message.updated`, `run.queued`, `run.started`,
`run.updated`, `run.finished`, `tool.started`, `tool.completed` (both `{step}`),
`approval.request`, `approval.updated`, `reasoning.final`, `context.updated`,
`health.status`, `card.new`, `system.updated`, `schedule.updated`, `schedule.deleted` (`{id}`), `error`, plus `keepalive` every 20 s. Close codes: 4400 protocol error, 4401 unauthorized,
4408 hello timeout or slow consumer.

A run is queued as `q_<hex>` and renamed to the Hermes `run_<hex>` when it starts
(`run.started` carries both). While a run streams, the assistant message holds the streamed
text; when it completes, `content` becomes the run's final output and the agent's interim
remarks ("I'll try …") move to `interim`. Assistant messages from REST and `sync` carry
their run's `tool_steps`, each with its `approval` if Hermes asked for one.

`api_revision` is the contract revision (below); `revision` is the git sha of the installed
release (`null` when not run from `deploy/install.sh`). After a resync, `GET /api/v1/runs/open`
tells which conversations have a run to stop or wait for.

### Contract

[`docs/contract.json`](docs/contract.json) lists every route with its query parameters, the
keys of every object (`conversation`, `message`, `run`, `open_run`, `approval`, `tool_step`),
every client frame and the exact key set of every server event. It is generated
(`python -m delve.contract > docs/contract.json`) and a test fails when it is stale. Server
events are also checked when they are published (`delve/protocol.py`): the test suite fails
on any mismatch, production logs it. A change a client can see bumps `API_REVISION`
(`delve/__init__.py`); the app requires a minimum revision.

### Tool steps and approvals

A step's `status` is `running`, `ok`, `error`, `cancelled` (stopped), `denied` (the user
said no), `timeout` (no answer before the Hermes approval timeout) or `blocked` (a deny
rule; no answer can change it). Approvals are stored with the exact command, its SHA-256,
an `action_class` (`pkg_upgrade`, `service_restart`, `delete_kill`, `other`), the Hermes
deadline and a longer expiry. The app can answer `once`, `session` or `deny`; `always` is
refused before anything reaches Hermes. If Hermes already gave up, the approval becomes
`expired_in_hermes` and can still be answered until `expires_at` (24 h for package
upgrades, 2 h for service restarts, 30 min otherwise):

- `deny` just records the answer;
- `once` or `session` queues a follow-up run in the same conversation
  (`origin: late_approval`, shown as a `system` message) that asks the agent to run exactly
  that command. If the follow-up asks for approval of a command with the same SHA-256, the
  backend answers `once` itself; anything else needs a fresh answer.

`expired_in_hermes → late_approved → followup_started → executed | superseded`, or
`cancelled` when the follow-up is stopped. Past `expires_at` an approval becomes `expired`.
The backend polls Hermes' `/health`; when it comes back after an outage, approvals still
pending for runs that no longer exist become `expired_in_hermes`. `health.status` events
report each change.

### Tokens and context

Every conversation carries `totals` (the sum of its runs' Hermes `usage`: `input_tokens`,
`output_tokens`, `cache_read_tokens`, `total_tokens`) and `context {tokens, max, pct, warn,
measured_at}`, where `warn` is `pct >= 75`. Input tokens add up over every llama call of
every run (a tool step means another call over the whole history), so `totals` is spend,
not size; `context` is how full the window is.

Context comes from llama-server's `/slots` (`n_prompt_tokens` includes the tokens decoded so
far). The slot is sampled every second while a run is active, published as
`context.updated {conversation_id, run_id, tokens, max, pct, warn, source, measured_at,
final}`, and the run's last call is stored when it ends. Hermes' session-title call starts
about 0.2 s after a run and overwrites the slot, so tasks seen before the run, newer tasks
smaller than the run's context, and anything after completion are ignored. A run too short
for any sample, with no tool steps, takes `input_tokens + output_tokens` (`source: usage`).
For a thinking run the value includes the reasoning tokens of the last call.

`runs.perf`: `sse_reconnects`, `first_delta_s` (from Hermes accepting the run to the first
text delta; a thinking run reasons first), `context_source`, `llama_calls_seen`, and from
`/metrics` differences `prompt_tokens` (prefilled, not cached), `predicted_tokens`,
`prefill_tps`, `gen_tps`. If llama-server cannot be reached, runs carry on without these.

Every conversation also has a `preview`: its latest non-empty user or assistant text,
whitespace collapsed, at most 200 characters (`null` before the first message).

### Cards

The agent's `agentcards` plugin and its no-agent scripts append cards to
`~agent/.hermes/cards/events.jsonl` (format: delve-infra, plan 5). The unit sees the
directory read-only at `/run/delve/cards` (delve-infra drop-in `cards.conf`, a bind mount;
the file is world-readable inside the agent's `0700` home). Every `DELVE_CARDS_POLL_S`
(2 s) the ingester reads the new complete lines from its stored offset
(`card_ingest_offsets`), checks each with the plugin's own validator
(`delve/cards/schemas.py`, vendored) and inserts the valid ones (`ON CONFLICT DO
NOTHING` on the card id) in one transaction with the new offset. A new card is published
as `card.new` (the `card` shape). A shorter file or a new inode starts over from 0; invalid
lines are logged and skipped. History is ingested too (the payload's time bounds are taken
relative to the card's own `ts`).

### System

`GET /api/v1/system` (and the `system.updated` event, after a stop/start and when the queue
is released) is one object:

| Key | Content |
|---|---|
| `checked_at` | when it was put together |
| `helper` | `{ok, error}`: the privileged helper answered every question (`error`: the first problem, e.g. `helper_unavailable`) |
| `hermes` | `{status: ok\|degraded\|down\|unknown, version, since, issues: [{check, status, detail}]}` - `degraded` and `issues` from `/health/detailed` (e.g. `disk`, `96% used`); `since`: the gateway unit's start |
| `llama` | `{status, intent, since, model, quant, ctx, slots, slots_busy, gen_tps}`; `status`: `running`, `loading` (unit active, `/health` 503), `starting`, `stopping`, `stopped`, `failed`, `down` (no helper and no answer), `unknown`; `intent`: `stopped` (stopped from here) or `starting` (started, model loading), else `null`; `since`: the unit's last start (running) or stop; `model`/`quant`/`ctx` from `/props` (file name only, never the path); `gen_tps`: the last completed run's generation speed |
| `queue` | `{active, queued}` runs |
| `gpus` | `[{index, name, temp_c, mem_used_mib, mem_total_mib, util_pct, power_w, power_limit_w}]` from `nvidia-smi`, `null` without the helper; a value the driver does not report is `null` |

The unit has no capabilities, no devices and its own user namespace, so the unit states,
`nvidia-smi` and starting/stopping llama-server go through the privileged helper of
delve-infra (`bin/delve-helper`, socket-activated on `DELVE_HELPER_SOCKET`,
`/run/delve-helper.sock`, group `delve`): one JSON line per connection, fixed verbs
`llama.status|start|stop`, `hermes.status`, `gpu.status` (and for the brief `host.status`,
`agent.incidents`). `sudo delve helper <verb>` asks it
the same way the server does. Without the helper the view still answers (`helper.ok:
false`, llama state from `/health`, no GPUs) and stop/start answer 503.

**Stopping** holds the run queue first, then: with an active run and no `force` → 409
(nothing changes); with `force` the active run is stopped (`cancelled`), then llama-server.
Queued runs - and messages sent meanwhile - **wait** instead of failing; the intent is
stored (`system_state['llama_intent']`) and holds the queue across a backend restart.
**Starting** keeps the queue held until llama `/health` answers 200
(`DELVE_LLAMA_START_WAIT_S`, 180 s, then it lets go anyway). A llama-server started
elsewhere (terminal, reboot) releases the queue within 15 s. A llama-server that dies
without being stopped from here is not held for: runs fail as before. The slot count is
kept (`system_state['llama_slots']`), so a stopped llama-server still shows it after a
backend restart; a failing `/slots` read is logged once (`llama /slots not readable`).

### Schedules and the morning brief

A schedule is a 5-field cron expression in **Europe/Budapest** (`cronsim` with
`zoneinfo`: on the autumn DST night a daily 02:30 fires once, in spring it fires at 03:00);
it may not fire more often than every 10 minutes. `description` is the English
explanation (`At 08:52, 12:52, and 20:52 every day`).

| Key | Content |
|---|---|
| `kind` | `prompt` (the prompt goes into the run queue, origin `schedule`) or `brief` (the built-in morning brief; exactly one, seeded `30 7 * * *` by migration 0007 and re-created if missing) |
| `conversation_mode`, `conversation_id`, `conversation_title` | `new`: a new conversation (origin `schedule`) for every run; `fixed`: always that conversation (unarchived when a run lands in it) |
| `thinking` | the schedule's own setting: it wins over the conversation's |
| `next_run_at` | `null` while disabled |
| `last_run_at`, `last_status`, `last_error`, `last_run_id` | the last firing: for a prompt the status of its latest run (`queued` … `completed`, `failed`, `cancelled`), or `failed` with `last_error` (`conversation_missing`) when it could not be queued; for the brief `ok` or `failed` |

Every `DELVE_SCHEDULE_TICK_S` (30 s; sooner when a schedule changes or the next one is
due) the enabled schedules whose time has passed fire once, then the next time is taken
from now - missed times in between are not repeated. A firing late by more than
`DELVE_SCHEDULE_CATCHUP_S` (3600 s: the backend was down) is skipped with a
`schedule missed` warning. Firing is idempotent: the run's `client_msg_id` and the brief's
card id derive from the schedule and the due time. A scheduled run waits in the queue like
any other run (also while llama-server is held) and always gets a `run_done` push.

The **brief** is written without the model straight into `cards` (`type: brief`,
`origin: backend`, `card.new`): `payload` = `{date, uptime_s, booted_at, open_incidents,
incidents: [{id, state, job, error, last_seen}] (≤ 5), updates_count, updates_checked_at,
water: {station, level_cm, lake_avg_cm, trend_cm_24h, lake_avg_trend_cm_24h, measured_at},
llama_state, hermes_state, missing}`. Uptime, boot and the agent's cron incidents come
from the helper (`host.status`, `agent.incidents`; an incident is open until acknowledged,
`resolved` included), the update count from the latest `updates` card of the agent's
`arch-updates` script, the water reading from the latest scheduled `water` card (both at
most 36 h old), the states from the system view. A source that cannot be read is `null`
and named in `missing`. `severity`: `alert` (llama-server failed/down or Hermes down),
`warn` (open incidents or something missing), else `info`.

### Observability

`GET /api/v1/stats?range=` (`24h` default, `7d`, `30d`) answers the app's Observability
screen. Buckets are in **Europe/Budapest** and the last one holds the current moment:
hours for `24h` (24), six hours at 00/06/12/18 for `7d` (28), days for `30d` (30, a DST day
is 23 or 25 hours); `from`/`to` are the outer edges, `bucket_s` the nominal size. Every
series is `{value, points: [{t, v}]}`, one point per bucket starting at `t`, `v: null`
where there is no data.

| Key | `points` | `value` |
|---|---|---|
| `generation` | mean `perf.gen_tps` of the completed runs that ended in the bucket | the newest such run's |
| `prefill` | mean `perf.prefill_tps`, only runs with `perf.prompt_tokens ≥ prefill_min_prompt_tokens` (500) | the newest such run's |
| `queue` | the most runs waiting at once (queued → started, or ended without starting) | queued now |
| `approval_latency` | median seconds from the request to the answer, by answer time; automatic late approvals are left out | median over the range |
| `cache_hit` | `Σ cache_read_tokens / Σ input_tokens` in %, completed runs | the same over the range |
| `tokens_per_day` | `days: [{date, tokens}]`, local days (7, or 30 for `30d`), `usage.total_tokens` of every run that ended that day | today's |
| `gpus[]` | `{index, name, temp, power}`: mean °C / W per GPU bucket (`gpu_bucket_s`: 10 min, 1 h, 6 h) | the newest sample |

The GPU series come from `gpu_samples`: every `DELVE_STATS_SAMPLE_S` (60 s; 0 turns it off)
the backend asks the helper for `gpu.status` and stores temperature, power, utilisation
and memory per GPU. `sampling` = `{interval_s, ok, error, last_at}` says whether that works
(a failure is logged once per kind). Once an hour the same loop deletes samples older than
`DELVE_GPU_RETENTION_DAYS` (30) and `push_log` rows older than
`DELVE_PUSH_LOG_RETENTION_DAYS` (90). Runs, approvals and messages are kept.

### Push notifications

With `FCM_SERVICE_ACCOUNT_FILE` (a Firebase service account key, `/etc/delve/fcm.json`,
`root:delve 0640`) the backend sends FCM HTTP v1 **data messages**; the app builds the
notification. Without it pushing is off and `GET /devices/me` says `push: false`. The unit
needs outbound HTTPS for this (delve-infra drop-in `delve-backend.service.d/fcm.conf`).

| `kind` | Fields (all strings) | Sent |
|---|---|---|
| `approval` | `request_id, conversation_id, title, action_class, command` (≤ 80 chars), `status` (`pending`/`expired_in_hermes`), `hermes_deadline, expires_at` | when an approval can be answered, and again when its status changes; high priority, collapse key = `request_id`, TTL until `expires_at` |
| `approval_resolved` | `request_id, status` | when a pushed approval stops being answerable (answered anywhere, expired, cancelled) |
| `card` | `card_id, type` (`water`), `severity, station, level_cm, lake_avg_cm, trend_cm_24h, lake_avg_trend_cm_24h, measured_at` | a new water card from the scheduled script (`origin: script`, at most an hour old), to devices with `notify.water`; high priority, collapse key `water` |
| `card` | `card_id, type` (`brief`), `severity, date, uptime_s, updates_count, open_incidents, llama_state, level_cm, lake_avg_cm` | a new morning brief (at most an hour old), to devices with `notify.brief`; high priority, collapse key `brief` |
| `run_done` | `conversation_id, run_id, title, status` (`completed`/`failed`), `preview` (≤ 120 chars) | for runs of at least `PUSH_RUN_DONE_MIN_S` (20 s; scheduled runs always; a shorter one is logged as `run_done not pushed: short run`); the app shows nothing while it is in the foreground; high priority |

Every kind goes with high priority: a normal-priority message waits while the phone
dozes and arrives only when it wakes (or the app is opened).

`title` is Hermes' title, else the conversation's first message. A new approval waits 1 s
before it is pushed (a follow-up run's approval is answered automatically within
milliseconds). Each device chooses with `notify` (`approvals`, `run_done`, `water`, `brief`); revoked devices and devices without an FCM token get nothing.
Whether the phone has a socket open does not matter: a phone locked a moment ago may
still look connected here for up to a minute (its close never arrived), so the log line
`push sent` only reports it (`socket`, live while a frame arrived within 60 s). An
`UNREGISTERED` answer clears the device's FCM token; 429/5xx are retried 3 times. Every
send is logged in `push_log`.

## Pre-commit hook

`.githooks/pre-commit` is the same guard as in `delve-infra`: it blocks staged content that
matches a local, untracked denylist (`~/.config/agent-infra/denylist.txt`), any 64-hex
string, and anything gitleaks flags.

## License

MIT
