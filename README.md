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
| 6 | Token and context usage | next |

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
delve-backend token create --name pixel     # prints a device token once
delve-backend                               # or: delve-backend serve
```

`delve-backend token list` and `delve-backend token revoke <device-id>` manage devices.
Only the SHA-256 of a token is stored. The token commands need only `DELVE_DATABASE_URL`.

## API

Every endpoint except `/api/v1/health` and `/api/v1/meta` needs
`Authorization: Bearer <device token>`.

| Method | Path | |
|---|---|---|
| `GET` | `/api/v1/conversations?limit=&cursor=&archived=` | newest first, keyset cursor |
| `GET` | `/api/v1/conversations/{id}` | one conversation |
| `GET` | `/api/v1/conversations/{id}/messages?before=&limit=` | oldest first |
| `PATCH` | `/api/v1/conversations/{id}` | `{thinking?, archived?}` |
| `GET` | `/api/v1/approvals?status=pending,expired_in_hermes` | approval inbox |
| `POST` | `/api/v1/approvals/{request_id}` | `{choice: once\|session\|deny}` |
| `WS` | `/api/v1/ws` | see below |

### WebSocket

Frames are `{"type", "id"?, "data"}`. The first frame must be
`hello {device_token, last_seq?, epoch?}`; the answer is
`hello.ok {server_version, hermes_version, epoch, seq, resync_required}`, followed by any
events after `last_seq`. Server events carry a global `seq`; the ring buffer keeps 10 000
events for up to an hour, and a new `epoch` after a restart means: reload over REST.

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
`approval.request`, `approval.updated`, `reasoning.final`, `health.status`, `error`, plus `keepalive` every 20 s. Close codes: 4400 protocol error, 4401 unauthorized,
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

## Pre-commit hook

`.githooks/pre-commit` is the same guard as in `delve-infra`: it blocks staged content that
matches a local, untracked denylist (`~/.config/agent-infra/denylist.txt`), any 64-hex
string, and anything gitleaks flags.

## License

MIT
