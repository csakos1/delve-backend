# Vendored fixtures

Copied unchanged from [`delve-infra`](https://github.com/csakos1/delve-infra)
`tests/fixtures/{protocol,runs,toolsets,cards,llama}` at commit `41a931b`.

Recorded against Hermes Agent v0.22.2 and sanitized there. When fixtures are re-recorded
after a Hermes update, update them in `delve-infra` first, then copy them here and bump the
commit above in the same change.

`llama/slots-run.jsonl` (a `/slots` + `/metrics` series over three live runs) was added at
`delve-infra` commit `318000e`.

`system/llama-props.json` and `system/hermes-health-detailed.json` (B3) are copied from
`delve-infra` `tests/fixtures/system/`. `system/helper-responses.json` is what the helper
answers for those samples: the `data` of `bin/delve-helper`'s `parse_show` / `parse_gpus`
run over `llama-show-*.txt`, `hermes-show-running.txt` and `gpu-*.csv` there, keyed
`<verb>/<state>`. Regenerate it when the helper's output changes.

`system/helper-responses.json` also holds `host.status/ok` and `agent.incidents/{closed,
detected,none}` (B4): `host_status()` over `proc-*.txt` and `parse_incidents()` over
`incidents-*.txt` of `delve-infra` `tests/fixtures/system/` (`detected` is that repository's
synthetic sample).

Recorded for W2 (watchers) on the live machine, 2026-10-28, not vendored from
`delve-infra`: `watches/cards.jsonl` (the first real `watch_def` / `watch_run` cards of
three watchers), `watches/cron-list.json` (the helper's `agent.cron.list` answer),
`llama/chat-judge.json` (one judge call: `/v1/chat/completions`, thinking off, JSON schema)
and `protocol/failed-partial.event.json` (the `run.failed` payload of a run that hit the
output length limit: no `error`, the reason in `output`, `partial: true` - N73). The model
path is redacted.
