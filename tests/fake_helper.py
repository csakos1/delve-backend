"""A fake of the privileged helper's socket (`delve-infra/bin/delve-helper`).

A threaded unix-socket server, so it answers both the TestClient's event loop and the
test's own. Answers come from `fixtures/system/helper-responses.json`; `llama.stop` and
`llama.start` flip the state (and, if given, the fake llama's /health).
"""

from __future__ import annotations

import json
import socketserver
import tempfile
import threading
from pathlib import Path
from typing import Any

from .fake_hermes import load_json
from .fake_llama import FakeLlama


class FakeHelper:
    def __init__(self, llama: FakeLlama | None = None) -> None:
        self.dir = Path(tempfile.mkdtemp(prefix="dh"))  # short: sun_path is 108 bytes
        self.path = self.dir / "helper.sock"
        self.responses: dict[str, Any] = load_json("system/helper-responses.json")
        self.state = "running"
        self.calls: list[str] = []
        self.fail: dict[str, tuple[str, str]] = {}  # verb -> (error code, message)
        self.silent: set[str] = set()  # verbs that never answer
        self.llama = llama
        self.start_to = "loading"  # the fake llama's /health right after llama.start
        self.incidents = "closed"  # agent.incidents answer: closed | detected | none
        self.jobs: list[dict[str, Any]] = []  # agent.cron.list (W2)
        self._server: socketserver.ThreadingUnixStreamServer | None = None

    def answer(self, verb: str, arg: str | None = None) -> dict[str, Any]:
        self.calls.append(verb if arg is None else f"{verb} {arg}")
        if verb in self.fail:
            code, msg = self.fail[verb]
            return {"ok": False, "error": code, "message": msg}
        if verb == "llama.stop":
            self.state = "stopped"
            if self.llama is not None:
                self.llama.health = "gone"
        elif verb == "llama.start":
            self.state = "running"
            if self.llama is not None:
                self.llama.health = self.start_to
        if verb in ("llama.status", "llama.stop", "llama.start"):
            data = self.responses[f"llama.status/{self.state}"]
        elif verb == "hermes.status":
            data = self.responses["hermes.status/running"]
        elif verb == "gpu.status":
            data = self.responses[f"gpu.status/{self.state}"]
        elif verb == "host.status":
            data = self.responses["host.status/ok"]
        elif verb == "agent.incidents":
            data = self.responses[f"agent.incidents/{self.incidents}"]
        elif verb == "agent.cron.list":
            data = {"jobs": [dict(j) for j in self.jobs]}
        elif verb in ("agent.cron.pause", "agent.cron.resume", "agent.cron.run"):
            job = next((j for j in self.jobs if j["id"] == arg), None)
            if job is None:
                return {"ok": False, "error": "failed", "message": "Job not found"}
            if verb != "agent.cron.run":
                paused = verb.endswith("pause")
                job.update(state="paused" if paused else "scheduled", enabled=not paused)
            data = {"job_id": arg, "action": verb.rsplit(".", 1)[1]}
        else:
            return {"ok": False, "error": "unknown_verb", "message": "unknown verb"}
        return {"ok": True, "verb": verb, "data": data}

    def start(self) -> FakeHelper:
        fake = self

        class Handler(socketserver.StreamRequestHandler):
            def handle(self) -> None:
                line = self.rfile.readline(2048)
                try:
                    req = json.loads(line)
                    verb, arg = req["verb"], req.get("arg")
                except (ValueError, KeyError, TypeError):
                    out = {"ok": False, "error": "bad_request", "message": "bad"}
                else:
                    if verb in fake.silent:
                        fake.calls.append(verb)
                        threading.Event().wait(5)
                        return
                    out = fake.answer(verb, arg)
                self.wfile.write(json.dumps(out).encode() + b"\n")

        self._server = socketserver.ThreadingUnixStreamServer(str(self.path), Handler)
        self._server.daemon_threads = True
        threading.Thread(target=self._server.serve_forever, daemon=True).start()
        return self

    def stop(self) -> None:
        if self._server is not None:
            self._server.shutdown()
            self._server.server_close()
        self.path.unlink(missing_ok=True)
