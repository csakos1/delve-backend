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
| `GET` | `/api/v1/devices/me` | `{id, name, created_at, last_seen_at, fcm}` |
| `PUT` | `/api/v1/devices/me/fcm` | `{token}` (`null` clears) → 204 |
| `GET` | `/api/v1/status` | `{server_version, hermes, hermes_version, thinking_route}` |
| `GET` | `/api/v1/conversations?limit=&cursor=&archived=` | newest first, keyset cursor |
| `GET` | `/api/v1/conversations/{id}` | one conversation |
| `GET` | `/api/v1/conversations/{id}/messages?before=&limit=` | oldest first |
| `GET` | `/api/v1/conversations/{id}/usage?limit=` | totals, context, runs with `usage`/`perf` |
| `PATCH` | `/api/v1/conversations/{id}` | `{thinking?, archived?}` |
| `GET` | `/api/v1/approvals?status=pending,expired_in_hermes` | approval inbox |
| `POST` | `/api/v1/approvals/{request_id}` | `{choice: once\|session\|deny}` |
| `WS` | `/api/v1/ws` | see below |

### WebSocket

Frames are `{"type", "id"?, "data"}`. The first frame must be
`hello {device_token, last_seq?, epoch?}`; the answer is
`hello.ok {server_version, hermes, hermes_version, thinking_route, epoch, seq,
resync_required}`, followed by any
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
`health.status`, `error`, plus `keepalive` every 20 s. Close codes: 4400 protocol error, 4401 unauthorized,
4408 hello timeout or slow consumer.

A run is queued as `q_<hex>` and renamed to the Hermes `run_<hex>` when it starts
(`run.started` carries both). While a run streams, the assistant message holds the streamed
text; when it completes, `content` becomes the run's final output and the agent's interim
remarks ("I'll try …") move to `interim`. Assistant messages from REST and `sync` carry
their run's `tool_steps`, each with its `approval` if Hermes asked for one.

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

## Pre-commit hook

`.githooks/pre-commit` is the same guard as in `delve-infra`: it blocks staged content that
matches a local, untracked denylist (`~/.config/agent-infra/denylist.txt`), any 64-hex
string, and anything gitleaks flags.

## License

MIT
