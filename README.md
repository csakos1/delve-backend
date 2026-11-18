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
| 13 | Attachments: image and file uploads, images into the run, files through the agent's inbox | done |
| 14 | Watchers: the agent's watch cards, the judge between runs, hit pushes with the link, cron jobs through the helper; the missed morning brief written late | done |
| 15 | Notification mirroring, phone to desktop: the phone's events in, the desktop agent's long poll out, nothing stored; device roles (`app`, `desk`); the posting app's icon | done |
| 16 | Personal profile: a core in every agent run's instructions and topic files the agent reads on demand, kept per conversation; read and edited by the app; the part marked for the General chats | done |
| 17 | News (N1a): the agent's collector batches in, the feed list out; topics, feeds, duplicates by title, a day of releases in one row; the Dashboard block, the lists, read / saved / feedback; import / export | done |
| 18 | News (N1b): scoring, grouping and the Hungarian "why" between runs when llama is idle; push of the must-reads; the weekly | done |
| 19 | News (N1c): article text (robots, Google News links through the renderer) and Hungarian summaries | next |

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

`delve-backend token create --name desk --role desk` creates the desktop agent's token
(delve-infra `bin/delve-desk`, see "Notification mirroring"). A device's role says what
its token opens: `app` (every paired phone) the whole API below, `desk` only the two
routes under `/api/v1/desk/`. `token list` shows the role.

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
needs `Authorization: Bearer <device token>` - an app's token; the routes under `/api/v1/desk/` take
the desktop agent's instead.

| Method | Path | |
|---|---|---|
| `POST` | `/api/v1/devices/register` | `{code, name?, app_version?}` → 201 `{device_id, name, token}`; 400 `invalid_code`, 429 after 10 failures a minute |
| `GET` | `/api/v1/devices/me` | `{id, name, created_at, last_seen_at, fcm, push, notify}`; `push`: the server can send pushes |
| `PATCH` | `/api/v1/devices/me` | `{notify: {approvals?, run_done?, water?, brief?, watches?, system?, news?}}` (booleans; left-out keys keep their value) → the device |
| `PUT` | `/api/v1/devices/me/fcm` | `{token}` (`null` clears) → 204 |
| `DELETE` | `/api/v1/devices/me` | unpair: revokes the calling token (and its FCM token) → 204 |
| `GET` | `/api/v1/status` | `{server_version, api_revision, revision, hermes, hermes_version, thinking_route}` |
| `GET` | `/api/v1/conversations?limit=&cursor=&archived=` | newest first, keyset cursor |
| `GET` | `/api/v1/conversations/{id}` | one conversation |
| `GET` | `/api/v1/conversations/{id}/messages?before=&limit=` | oldest first |
| `GET` | `/api/v1/conversations/{id}/usage?limit=` | totals, context, runs with `usage`/`perf`/`model`; `last_model`: the model that answered last |
| `PATCH` | `/api/v1/conversations/{id}` | `{thinking?, archived?}` |
| `GET` | `/api/v1/approvals?status=pending,expired_in_hermes` | approval inbox |
| `POST` | `/api/v1/approvals/{request_id}` | `{choice: once\|session\|deny}` |
| `GET` | `/api/v1/cards?type=&since=&limit=` | cards newest first (`ts`); `since` with a timezone |
| `GET` | `/api/v1/runs/open` | the queue: active run first, then queued ones; each with `position` (runs ahead) |
| `GET` | `/api/v1/system` | the `system` view (below); one answer is reused for 2 s |
| `POST` | `/api/v1/system/llama` | `{action: start\|stop\|switch\|dismiss, force?, model?}` → the `system` view; `switch` takes `model` (a preset name from `llama.models`), `dismiss` forgets a failed switch; 409 `run_active` (stop or switch without `force` while a run is active), `already_active`, `model_not_usable`, `switch_in_progress`; 404 `unknown_model`; 503 `helper_unavailable`, 502 `helper_failed` |
| `GET` | `/api/v1/schedules` | `{data: [schedule]}`, the morning brief first |
| `POST` | `/api/v1/schedules` | `{name, cron, prompt, conversation_mode?: new\|fixed, conversation_id?, thinking?, enabled?}` → 201 `schedule`; 422 `invalid_cron`, `too_frequent`, `invalid_name`, `invalid_prompt`, `conversation_required`, `conversation_not_found`; 409 `too_many` (50) |
| `PATCH` | `/api/v1/schedules/{id}` | any of the fields above → `schedule`; the brief takes only `cron` and `enabled` (422 `brief_fields`) |
| `DELETE` | `/api/v1/schedules/{id}` | → 204; the brief: 409 `brief_fixed` |
| `POST` | `/api/v1/schedules/{id}/run` | fire once now (timing unchanged) → `{schedule, run_id, card_id}` |
| `GET` | `/api/v1/stats?range=24h\|7d\|30d` | the Observability view (below); 422 for another range |
| `POST` | `/api/v1/attachments?kind=image\|file&filename=` | the raw bytes as the body, the type in `Content-Type` → 201 `attachment`; 413 `too_large`, 415 `unsupported_image`, 422 `empty` |
| `GET` | `/api/v1/attachments/{id}` | the bytes (images `inline`, files `attachment`); 404 `attachment_not_found`, 410 `attachment_gone` |
| `POST` | `/api/v1/attachments/{id}/copy` | a new, unsent upload with the same content ("send again") → 201 `attachment`; 410 `attachment_gone` |
| `GET` | `/api/v1/watches` | `{data: [watch]}` (not deleted ones), by name |
| `GET` | `/api/v1/watches/{slug}` | `watch`; 404 `watch_not_found` |
| `GET` | `/api/v1/watches/{slug}/items?verdict=&before=&limit=` | `{data: [watch_item], next_before}`, newest first; `verdict`: comma list of `pending`, `match`, `reject`, `mechanical`, `error`, or `hits` (= `match,mechanical`); 422 `invalid_verdict` |
| `POST` | `/api/v1/watches/{slug}/pause\|resume\|run` | the watcher's cron job through the helper → `watch`; 409 `watch_job_missing`, 503 `helper_unavailable`, 502 `helper_failed` |
| `POST` | `/api/v1/mirror/phone` | `{events: [{op: post\|remove, key, app?, title?, text?, category?, hidden?, icon?}]}` (1-32 events, in order) → `{accepted, desk: {connected, last_seen_s}}`; see "Notification mirroring" |
| `GET` | `/api/v1/mirror` | `{desk: {connected, last_seen_s}}`: is the desktop agent fetching |
| `GET` | `/api/v1/desk/next?wait=` | the desktop agent's token: `{commands: [...]}`, waits up to `wait` seconds (0-30, default 25) for the first one |
| `GET` | `/api/v1/desk/icon/{icon}` | the desktop agent's token: an app's icon (`image/png`) by the name a `notify.show` carries; 404 `icon_not_found` once it is no longer kept |
| `GET` | `/api/v1/persona` | `persona`: the personal profile as it is now; see "Personal profile" |
| `PUT` | `/api/v1/persona/profile` | `{sections: [{title, text, general?}]}` (at most 12, a text of at most 4000 characters) replaces the core → `persona`; 503 `persona_no_store`, `persona_not_writable` |
| `PUT` | `/api/v1/persona/topics/{name}` | `{title, summary?, text, general?}` creates or replaces a topic file (`name`: lowercase letters, digits, `-`) → `persona`; 409 `too_many_topics` |
| `DELETE` | `/api/v1/persona/topics/{name}` | → `persona`; 404 `topic_not_found` |
| `GET` | `/api/v1/news` | `news_digest`: the Dashboard's News block; see "News" |
| `GET` | `/api/v1/news/items?topic=&saved=&weekly=&before=&limit=` | `{data: [news_item], next_before}`: the lead stories, newest first; `saved=true`: the saved ones; `weekly=true`: the best unopened ones of the last 7 days (one page, best first) |
| `GET` | `/api/v1/news/items/{id}` | `news_story`: a story with its other sources (a duplicate's id opens its story); 404 `news_item_not_found` |
| `PATCH` | `/api/v1/news/items/{id}` | `{read?, saved?, feedback?: more\|less\|null}` → `news_story` |
| `GET` | `/api/v1/news/topics` | `{data: [news_topic]}` by position |
| `POST` | `/api/v1/news/topics` | `{id, name, description?, position?, enabled?}` → 201 `news_topic`; 409 `news_topic_exists`, `too_many_topics` |
| `PATCH` | `/api/v1/news/topics/{id}` | `{name?, description?, position?, enabled?}` → `news_topic` |
| `DELETE` | `/api/v1/news/topics/{id}` | 204; its feeds go too, its stories stay without a topic |
| `GET` | `/api/v1/news/sources` | `{data: [news_source]}`: the feeds and what each answered last |
| `POST` | `/api/v1/news/sources` | `{url, topic, id?, kind?, window_h?, every_min?, enabled?}` → 201 `news_source`; 409 `news_source_exists`, `too_many_sources` |
| `PATCH` | `/api/v1/news/sources/{id}` | `{enabled?, topic?, window_h?, every_min?}` → `news_source` ("Hide this source": `enabled: false`) |
| `DELETE` | `/api/v1/news/sources/{id}` | 204 |
| `GET` | `/api/v1/news/settings` | `news_settings`: `{setup, digest_at, push, push_max}` |
| `PATCH` | `/api/v1/news/settings` | any of them → `news_settings` |
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
| `chat.send {text, client_msg_id, conversation_id?, thinking?, attachment_ids?}` | `ack {conversation_id, message_id, run_id, replayed}`; with attachments `text` may be empty |
| `conversation.set {conversation_id, thinking?, archived?}` | `ack {conversation}` |
| `run.stop {run_id}` (queue id or Hermes id) | `ack {status}` |
| `approval.respond {request_id, choice}` | `ack {approval}` |
| `sync {conversation_id, after_message_id?}` | `ack {conversation, messages, has_more}` |
| `ping` | `pong` |

Server events: `conversation.created`, `conversation.updated`, `message.created`,
`message.delta`, `message.interim`, `message.updated`, `run.queued`, `run.started`,
`run.updated`, `run.finished`, `tool.started`, `tool.completed` (both `{step}`),
`approval.request`, `approval.updated`, `reasoning.final`, `context.updated`,
`health.status`, `card.new`, `system.updated`, `schedule.updated`, `schedule.deleted` (`{id}`),
`watch.updated`, `watch.item`, `news.updated` (`{new, updated_at}`), `error`, plus `keepalive` every 20 s. Close codes: 4400 protocol error, 4401 unauthorized,
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
far); `max` is the slot's window but at most `DELVE_AGENT_CONTEXT` (65536, Hermes'
`context_length`): llama-server may run with a larger window than the agent works with,
and Hermes compresses by its own. The slot is sampled every second while a run is active, published as
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
relative to the card's own `ts`). Newly inserted cards also go to the watchers (below) in
the same transaction. Migration 0010 drops the stored offset once, so the `watch_*` cards
written before it (then skipped as an unknown type) are read again.

### System

`GET /api/v1/system` (and the `system.updated` event, after a stop/start and when the queue
is released) is one object:

| Key | Content |
|---|---|
| `checked_at` | when it was put together |
| `helper` | `{ok, error}`: the privileged helper answered every question (`error`: the first problem, e.g. `helper_unavailable`) |
| `hermes` | `{status: ok\|degraded\|down\|unknown, version, since, issues: [{check, status, detail}]}` - `degraded` and `issues` from `/health/detailed` (e.g. `disk`, `96% used`); `since`: the gateway unit's start |
| `llama` | `{status, intent, since, model, quant, ctx, slots, slots_busy, gen_tps, preset, vision, thinking, models, switch}` (the last five: **Models** below; `switch`: `{state, from, to, by, at, error, elapsed_s}` or `null`); `status`: `running`, `loading` (unit active, `/health` 503), `starting`, `stopping`, `stopped`, `failed`, `down` (no helper and no answer), `unknown`; `intent`: `stopped` (stopped from here) or `starting` (started, model loading), else `null`; `since`: the unit's last start (running) or stop; `model`/`quant`/`ctx` from `/props` (file name only, never the path); `gen_tps`: the last completed run's generation speed |
| `queue` | `{active, queued}` runs |
| `gpus` | `[{index, name, temp_c, mem_used_mib, mem_total_mib, util_pct, power_w, power_limit_w}]` from `nvidia-smi`, `null` without the helper; a value the driver does not report is `null` |

The unit has no capabilities, no devices and its own user namespace, so the unit states,
`nvidia-smi` and starting/stopping llama-server go through the privileged helper of
delve-infra (`bin/delve-helper`, socket-activated on `DELVE_HELPER_SOCKET`,
`/run/delve-helper.sock`, group `delve`): one JSON line per connection, fixed verbs
`llama.status|start|stop`, `llama.models`, `llama.select <preset>`, `hermes.status`,
`gpu.status` (and for the brief `host.status`,
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

**Models.** The loaded model is a state of the machine: one preset file per model on the
host (`delve-infra` `llama/presets/`), and the helper's `llama.models` / `llama.select`
list them and move the pointer llama-server loads from. The view's `llama` carries
`preset` (the selected one), `models` (`llama_model[]`; `null` when the helper cannot list
them), `vision` and `thinking` (what the loaded - or, while stopped, the selected - model
can do) and `switch`. **Switching** (`action: switch`) while llama-server runs is stop,
select, start with the queue held: messages sent meanwhile wait and run on the new model;
an active run is refused with 409 unless `force`, which stops it first. `switch.state` is
`switching` until `/health` answers. A preset that keeps exiting while it loads
(`DELVE_LLAMA_SWITCH_RESTARTS`, 3 failed starts in a row - one or two can be a passing
lack of GPU memory) or is not up in `DELVE_LLAMA_START_WAIT_S` makes the switch fail: the
previous preset is selected and started again, `switch.state` becomes `failed`
(`error`: `exited` or `timeout`) and stays until `action: dismiss` or the next switch,
also across a backend restart (`system_state['llama_switch']`). If the previous one does
not come up either, the state is `down` and the queue stays held as after a stop, until
llama-server is healthy again. Both ends are pushed (`system`, below). While llama-server
is **stopped**, `switch` only moves the selection: Start loads it. Every run stores the
model that was loaded when it started (`run.model`, e.g. `Qwen3.8-27B · IQ4_XS`; the
preset's label replaces the name from the file). A message with an image is refused with
`model_no_vision` while the loaded (or selected) model has no projector, and a preset
marked as not thinking sends its runs the plain route whatever the chat's switch says.

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
llama_state, hermes_state, missing, late_by_s}`. Uptime, boot and the agent's cron incidents come
from the helper (`host.status`, `agent.incidents`; an incident is open until acknowledged,
`resolved` included), the update count from the latest `updates` card of the agent's
`arch-updates` script, the water reading from the latest scheduled `water` card (both at
most 36 h old), the states from the system view. A source that cannot be read is `null`
and named in `missing`. `severity`: `alert` (llama-server failed/down or Hermes down),
`warn` (open incidents or something missing), else `info`.

A **missed brief** (the machine was off at 07:30) is not skipped like other late
schedules: if it is still the same local day and at most 6 h late, it is written once the
backend has been up for 3 minutes (the agent's own catch-up runs - `arch-updates`, the
water reading - come first), with `late_by_s` set (else `null`).

### Watchers

The agent defines a watcher in the chat (its `watch_manage` tool, delve-infra) and runs it
as a no-agent Hermes cron job `watch:<slug>`; no model runs on a check. Every run writes
cards: `watch_def` (the definition, when it changed), `watch_candidate` (a new listing
that passed the watcher's mechanical filters; with `change: "available"` one announced
earlier that is no longer reserved) and `watch_run` (counts, or `status: error` with
`error_code`). The card ingest applies them in its own transaction (`watches`,
`watch_items`); `watch.updated` and `watch.item` follow.

| `verdict` | Meaning |
|---|---|
| `mechanical` | the watcher has no `criteria`, or its source is `cinemacity` (the bookable film is the hit): every new candidate is a hit; every other source, a new one too, is judged when it has `criteria` |
| `pending` | waits for the judge (or for its watcher's definition) |
| `match` / `reject` | the judge's answer against `criteria`, with a one-sentence `reason` |
| `error` | the judge failed 3 times |

**The judge** asks llama-server directly (`/v1/chat/completions`, thinking off,
`temperature 0`, a JSON schema `{match, reason}`, no tools); the listing is data, never
instructions. It rejects only what the listing says or clearly shows against the
condition; a listing that says nothing about it is a match (the user's choice: "not
mined" means no sign of mining). It runs in the run worker while no run is queued, one
item at a time, so chat runs go first and llama-server is never asked twice at once; a
held queue holds it too. After a failed call it waits 60 s.

A hit is pushed (`kind: watch`) when its card is at most 6 h old; older cards (a re-read
file, history) are judged but not pushed. A reserved hit (Hardverapró "jegelve",
`reserved: true`) is pushed with the mark, and once more (`change: available`) when the
mark is gone. `stop_after_hit` pauses the cron job after the first hit (`stopped_at`;
resume clears it). Every `DELVE_WATCH_RECONCILE_S` (300 s) and after a new definition the
backend reads the cron jobs (`agent.cron.list`): `job_id`, paused or not, `next_run_at`; a
watcher whose job has been gone for more than 2 minutes after its definition was deleted in
the chat (`state: deleted`, no longer listed). Hits are kept
`DELVE_WATCH_HIT_RETENTION_DAYS` (180), the rest `DELVE_WATCH_REJECT_RETENTION_DAYS` (30).

`watch`: `{slug, name, source, url, criteria, include, any, exclude, min_price, max_price,
fetch_details, stop_after_hit, schedule, state (active|paused|unknown|deleted), job_id,
next_run_at, last_check_at, last_status, last_error_code, last_error, last_items,
last_matched, errors_in_row, hits, hits_24h, pending, rejected, last_hit_at, stopped_at,
defined_at, updated_at}`. `watch_item`: `{id, slug, item_id, title, url, price,
price_value, location, posted, excerpt, reserved, verdict, reason, seen_at, judged_at,
notified_at, available_at}`.

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

### Attachments

Up to four per message. The app uploads each one first (`POST /api/v1/attachments`, one
file per request, no multipart), then lists the ids in `chat.send`; the send binds them to
its user message in the same transaction and fails as a whole otherwise:
`too_many_attachments`, `duplicate_attachment`, `attachment_not_found`,
`attachment_gone`, `attachment_used` (an id can be sent once; `…/copy` makes a new one),
`model_no_vision` (an image while the loaded model cannot read images; the upload stays).
User messages from REST, `sync` and `message.created` carry `attachments`
(`[attachment]`: `{id, kind, mime, filename, size_bytes, message_id, available, created_at}`);
other messages have an empty list.

| Kind | Accepted | Stored | How the agent gets it |
|---|---|---|---|
| `image` | `image/jpeg`, `image/png`, `image/webp`, checked against the bytes; at most 1.5 MiB (the app sends 1536 px JPEG 85) | `DELVE_ATTACHMENTS_DIR/<id>.<ext>` (`/var/lib/delve/attachments`) | in the run's input: one user message whose content is the text plus `image_url` parts with `data:` URLs (needs `model.supports_vision: true` in Hermes) |
| `file` | any type, at most 20 MiB | `DELVE_INBOX_DIR/<id>/<name>` (`/var/lib/delve-inbox`, 2750 `delve:agent`, delve-infra) | a line in the run's `instructions` with the path, type and size; the agent reads it with its own tools |

Four images stay under the Hermes API server's 10 MB request limit after base64. The
image is encoded once per run (a tool round reuses the KV cache). Hermes keeps only a
placeholder for an image in the session history, so every later run of a conversation
that had images gets a note in its `instructions`: the model no longer sees them and asks
for them again instead of guessing. An attachment that is gone when its run starts becomes
a line saying so. Uploads that are never sent are dropped after an hour; the files of sent
ones are deleted after `DELVE_ATTACHMENT_RETENTION_DAYS` (14) and the row stays with
`available: false`.

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
| `card` | `card_id, type` (`brief`), `severity, date, uptime_s, updates_count, open_incidents, llama_state, late_by_s, level_cm, lake_avg_cm` | a new morning brief (at most an hour old), to devices with `notify.brief`; high priority, collapse key `brief` |
| `watch` | `change` (`hit`/`available`), `slug, name, item` (the `watch_item` id), `item_id, title` (≤ 120), `price, reason` (≤ 240), `url, reserved` (`true`/`false`) | a watcher's hit, and a reserved hit that became available, to devices with `notify.watches` (on by default); high priority, collapse key `watch:<slug>:<item_id>`, TTL 24 h |
| `news` | `item` (the `news_item` id), `title` (≤ 120), `publisher, topic` (its name), `why` (≤ 240), `url, score` | a must-read story (see "News"), to devices with `notify.news` (off by default until N2); high priority, collapse key `news:<item>`, TTL 12 h |
| `system` | `event` (`switch_failed`/`switch_down`), `to`, `back` (model names as shown; `back` empty when there was nothing to go back to), `at` | a model switch that did not work: the previous model is loaded again (`switch_failed`) or llama-server is not running (`switch_down`), to devices with `notify.system` (on by default); high priority, collapse key `llama-switch`, TTL 6 h |
| `run_done` | `conversation_id, run_id, title, status` (`completed`/`failed`), `preview` (≤ 120 chars) | for runs of at least `PUSH_RUN_DONE_MIN_S` (20 s; scheduled runs always; a shorter one is logged as `run_done not pushed: short run`); the app shows nothing while it is in the foreground; high priority |

Every kind goes with high priority: a normal-priority message waits while the phone
dozes and arrives only when it wakes (or the app is opened).

`title` is Hermes' title, else the conversation's first message. A new approval waits 1 s
before it is pushed (a follow-up run's approval is answered automatically within
milliseconds). Each device chooses with `notify` (`approvals`, `run_done`, `water`, `brief`, `watches`, `system`, `news`); revoked devices and devices without an FCM token get nothing.
Whether the phone has a socket open does not matter: a phone locked a moment ago may
still look connected here for up to a minute (its close never arrived), so the log line
`push sent` only reports it (`socket`, live while a frame arrived within 60 s). An
`UNREGISTERED` answer clears the device's FCM token; 429/5xx are retried 3 times. Every
send is logged in `push_log`.

### Notification mirroring

The phone's notifications on the desktop (DA2). The app runs a notification listener
and sends what it saw, for the apps the user chose there, to `POST /api/v1/mirror/phone`:
`post` for a new or changed notification (`key`: the listener's key; `app`: the app's
name as shown; `title`, `text` ≤ 256 / 2000 characters; `category`: Android's, such as
`msg` or `email`; `hidden: true` when Android kept the content from the listener, as it
does with one-time codes; `icon`: the posting app's icon, a PNG of at most 32 KiB as
base64, API 12) and `remove` when it went away on the phone. What is not chosen on the
phone never leaves it.

The desktop agent (delve-infra `bin/delve-desk`, a user service in the operator's
graphical session) asks `GET /api/v1/desk/next` in a loop and gets commands:

| Command | Fields | |
|---|---|---|
| `notify.show` | `id, app, title, body, category, hidden, icon` | show, or replace the one shown under the same `id` |
| `notify.close` | `id` | close it; an unknown `id` is nothing to do |

`id` is 16 hex characters derived from the device and the key with a secret of this
process, so the agent never sees a key and an `id` means nothing after a restart.
`icon` is "" or 32 hex characters of the hash of the app's icon; the agent fetches the
picture once with `GET /api/v1/desk/icon/{icon}` and keeps its own copy. The backend
holds the newest 64 icons in memory (an icon is the app's, the same for each of its
notifications); after a restart the phone's next post of an app brings its icon again.

The backend only passes things on. Nothing of a notification is stored or logged:
commands wait in memory, at most 200 of them and each for at most 60 seconds, and a
command is gone once a poll took it (at most once). An agent that was away longer starts
with what arrives from then on. A newer `post` of a key replaces the one still waiting; a
`remove` drops the waiting `post` and still sends the close, because an earlier post of
the key may be on the desktop. When the agent asks again while its previous request is
still waiting here, the older request ends with an empty list: commands go to the newest
poll only. `desk.connected` is true while the agent asked within the last 65 seconds.
A waiting poll ends within a second when the server is told to stop, so the agent's open
request does not hold up a restart.

The agent's token is a device with the role `desk`
(`sudo delve token create --name desk --role desk`); it opens these two routes. The
app's routes and the WebSocket answer it like an unknown token.

### Personal profile

What the agent knows about the person it works for (P1, API 13), so a chat does not start
with explaining who is asking. It lives in a directory outside every repository
(`DELVE_PERSONA_DIR`, `/var/lib/delve-persona`, 2750 `delve:agent` from delve-infra; the
agent reads it through its group and cannot write it):

| File | Content | Reaches the model |
|---|---|---|
| `profile.md` | the core: `## Title` sections of short facts; `## Title [general]` marks a section for the General chats; HTML comments are for the editor | whole, at the start of every agent run's `instructions`, closed by a line that says to use the facts only where the question needs them |
| `topics/<name>.md` | more detail on one subject; a front matter of `title`, `summary` (one line) and `general: yes\|no` | one line of the index after the core (`- <path>: <title> - <summary>`); the agent reads the file with `read_file` when the question needs it |

Without the directory, or with an empty one, runs go as before.

**A conversation keeps the profile it started with** (`conversations.persona`, the text
itself; `''` for a chat that started without one, as every chat from before this did).
Hermes puts a run's `instructions` at the end of the system prompt, before the
conversation: the same text in every run of a chat stays in llama-server's prompt cache,
and a different one would make it read the whole chat again. An edit therefore applies to
the chats started after it. Scheduled runs and late approvals get it like any run. The
notes about attachments come after it. Measured with a 525-token profile on a 10 267-token
system prompt: 37 tokens processed for the next message of a chat, 432 for a new chat.

`persona` (the answer of all four routes): `available` (the directory exists),
`sections: [{title, text, general}]`, `topics: [{name, title, summary, text, general}]`,
`block` (what a chat started now gets) with `block_chars` and `revision` (12 hex
characters of its hash, null without a profile), and `general_text`: the sections and
topic files marked `general`, and nothing else - the app adds it to the system prompt of a
General chat, which goes to a cloud model. What is not marked never leaves this machine.

The files can also be edited by hand (as `delve`, or with `sudo`); the backend reads them
when a chat starts. Nothing of the profile is logged.

### News

Stories from the web for one reader (N1, API 14). The backend never fetches the web
(delve-infra D-58): an hourly no-agent cron job of the agent's user reads the feeds and
hands the new items back as batch files. The two meet in two places:

| Where | Written by | Content |
|---|---|---|
| `DELVE_NEWS_DIR/sources.json` (`/var/lib/delve-news`, 2750 `delve:agent`) | the backend, whenever a topic or feed changes and hourly | `{v: 1, sources: [{id, url, kind, window_h, every_min}]}`: the enabled feeds of the enabled topics |
| `DELVE_NEWS_BATCH_DIR/batch-*.json` (the agent's `~/.hermes/cards/news`, read-only here) | the collector, one per run (it removes them after 3 days) | `{v: 1, at, sources: [{id, status, http, items, new, error_code, error}], items: [{key, source, url, title, publisher, published, summary, gn, kind}]}` |

Every `DELVE_NEWS_POLL_S` the batches not read yet are taken in (each once,
`news_batches`; a broken one is recorded with its error and never retried). An item of a
feed that is gone or hidden by now is dropped. A story is stored under the collector's key
(a hash of its normalised URL); one whose folded title (lower case, no accents, letters and
digits) a lead story of the last 72 hours has already is stored as its duplicate - the
lists show the lead with `sources` (how many feeds carried it), the story view lists the
others. Titles shorter than 12 characters never match. The releases of a `release` feed
(GitHub `releases.atom`) are one row per feed and day: `"llama.cpp: 10 releases (b12468 -
b12477)"`, `releases: [{tag, url, published_at}]` newest first. Each feed's last report is
kept on it (`last_status`: `ok`, `not_modified`, `skipped` - rate limited or out of time -
or `error` with `errors_in_row`); `failing` in the digest counts the feeds that failed 3
times in a row. Stories older than `DELVE_NEWS_RETENTION_DAYS` go, saved ones stay.

`news_digest`: `top` (at most 4 unread lead stories of the last 24 hours, the best score
first, then the newest - one per topic while other topics have one to show), `new` (how many unread there are), `updated_at` (the last batch),
`state` (`empty` before the first batch, else `ok`), `sources`, `failing`, `weekly`
(`{count}` on Mondays from `digest_at` on - the Dashboard's "Last week" row - else null).
`summary_hu` stays null until N1c.

**The model steps (N1b)** run between runs (`RunManager.side_work`, after a watcher's
verdict) with direct llama calls - no tools, no thinking, the items marked as data - and
only when llama-server has been idle for `DELVE_NEWS_IDLE_S` (180 s): it has one slot, and
a coding session that talks to it directly (opencode, VS Code) would lose its prompt cache
to a News request. Idle = the slot is not processing and the token counters of `/metrics`
did not move (our own requests do not count). One step at a time, the first that has work:

1. **score** - up to 12 unscored lead stories of the last 48 hours, newest first: `<item>
   <score 0-9> <topic number>` per line against the enabled topics' descriptions and
   `setup`; the topic the model picks replaces the feed's. An item left out three times
   gets 0. A release row is not asked: it gets 6 (good to know) by rule.
2. **group** - up to 16 stories scored 5 or more in the last 36 hours that were not
   compared yet (once there are 8, or the oldest waited 20 minutes), with up to 24 others
   of those days - first the ones whose titles share words with the new ones (names,
   numbers; rarer words count more), then the ones of the same topics: the items about *the
   same specific event* (also across Hungarian and English) become one story under its
   earliest item (`dup_of`, the others listed in the story view), with the best score of
   the group. Stories kept apart once are not merged by a later answer alone. Release rows
   are not grouped.
3. **why** - up to 5 stories scored 5 or more after grouping, best first: the topic, a
   Hungarian "why" of at most 12 words, and `setup` (a release row is always `true`: the
   reader's own tools).

A failed call (llama down, no usable line) counts a try and pauses the steps for a minute.
A step that waits for llama (in use, or unavailable) logs `news waiting` after half an hour
and then every half hour.

After a topic's description changed, `sudo delve news rescore [--hours 1-48]` sends the
stories of those hours through the three steps again (from their feed's topic; merged
duplicates, pushes, read / saved / feedback stay).

**Push** (`news`): a story with score 9 and a why, from the last 12 hours, not opened, at
most `push_max` a day, between `digest_at` and 22:00 (later ones wait for the morning) - to
the devices with `notify.news`, which is off until the app has its channel (N2).

**What the operator reads is personal** - the topics with their descriptions (what the
model will score against), the feeds, and `setup` (a line about his own machines, for
marking the stories that touch them). None of it is in a repository: it is imported once
and edited in the app.

```sh
sudo delve news import < news.json      # topics, sources, settings; --replace drops the rest
sudo delve news export > news.json      # the same format, for a backup
```

The format is `deploy/news.example.json` (an invented reader). The import checks
everything before it writes anything and writes `sources.json` at the end.

## Pre-commit hook

`.githooks/pre-commit` is the same guard as in `delve-infra`: it blocks staged content that
matches a local, untracked denylist (`~/.config/agent-infra/denylist.txt`), any 64-hex
string, and anything gitleaks flags.

## License

MIT
