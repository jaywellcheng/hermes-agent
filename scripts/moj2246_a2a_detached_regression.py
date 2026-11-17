#!/usr/bin/env python3
"""MOJ-2246 real-link regression.

Exercise the actual A2A HTTP adapter with a slow turn:
1. send ``message/stream`` and disconnect after reading the first SSE frame;
2. poll ``tasks/get`` until the detached finalizer settles the task;
3. compare the persisted conversation agent entry with the terminal artifact.
"""
from __future__ import annotations

import asyncio
import http.client
import json
import socket
import sys
import time
import urllib.error
import urllib.request
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from plugins.platforms.a2a import protocol


def free_port() -> int:
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


def make_adapter(port: int):
    from gateway.config import PlatformConfig
    from plugins.platforms.a2a.adapter import A2AAdapter

    import os
    os.environ["A2A_PORT"] = str(port)
    adapter = A2AAdapter(PlatformConfig(enabled=True))

    async def slow_handle(event):
        await asyncio.sleep(5.2)
        await adapter.send(event.source.chat_id, "DETACHED-ARTIFACT-OK", metadata={"notify": True})

    adapter.handle_message = slow_handle
    adapter._message_handler = object()
    return adapter


def rpc_body(context_id: str) -> dict:
    return {
        "jsonrpc": "2.0",
        "id": "moj2246",
        "method": "message/stream",
        "params": {
            "message": protocol.text_message(protocol.ROLE_USER, "slow real-link turn", context_id=context_id),
            "metadata": {"issue_id": "MOJ-2246", "round": "real-link", "trace_id": "detached-finalization", "private": "drop-me"},
        },
    }


def read_first_frame(conn: http.client.HTTPConnection, body: dict) -> dict:
    payload = json.dumps(body).encode()
    conn.request("POST", "/", payload, {"Content-Type": "application/json"})
    response = conn.getresponse()
    if response.status != 200:
        raise RuntimeError(f"message/stream failed: {response.status} {response.read().decode(errors='replace')}")
    while True:
        line = response.fp.readline()
        if not line:
            raise RuntimeError("SSE closed before first frame")
        text = line.decode().strip()
        if not text.startswith("data: "):
            continue
        raw = json.loads(text[6:])
        result = raw.get("result", raw)
        if "task" in result:
            return result["task"]
        raise RuntimeError(f"first frame was not a Task: {result}")


def tasks_get(base_port: int, task_id: str) -> dict:
    body = {
        "jsonrpc": "2.0",
        "id": "poll",
        "method": "tasks/get",
        "params": {"taskId": task_id},
    }
    request = urllib.request.Request(
        f"http://127.0.0.1:{base_port}/",
        data=json.dumps(body).encode(),
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    with urllib.request.urlopen(request, timeout=3) as response:
        result = json.load(response)["result"]
    if "error" in result:
        raise RuntimeError(result["error"])
    return result


async def run() -> int:
    port = free_port()
    context_id = f"moj2246-real-{int(time.time())}"
    adapter = make_adapter(port)
    if not await adapter.connect():
        raise RuntimeError("adapter failed to connect")
    try:
        conn = http.client.HTTPConnection("127.0.0.1", port, timeout=10)
        try:
            started = time.monotonic()
            task = read_first_frame(conn, rpc_body(context_id))
            first_frame_seconds = time.monotonic() - started
        finally:
            conn.close()
        task_id = task["id"]
        print(f"FIRST_FRAME task_id={task_id} context_id={context_id} state={task['status']['state']} seconds={first_frame_seconds:.6f}")
        deadline = time.monotonic() + 25
        stored = None
        await asyncio.sleep(6.2)  # let the turn finish; poll slowly to stay under rate limiting
        while time.monotonic() < deadline:
            await asyncio.sleep(2.0)
            try:
                candidate = tasks_get(port, task_id)
            except urllib.error.HTTPError as error:
                if error.code != 429:
                    raise
                continue
            state = candidate.get("status", {}).get("state")
            print(f"POLL state={state} keys={sorted(candidate)}")
            if state in {"TASK_STATE_COMPLETED", "TASK_STATE_FAILED", "TASK_STATE_REJECTED"}:
                stored = candidate
                break
        if stored is None:
            rec = adapter.tasks.get(task_id)
            raise RuntimeError(f"task {task_id} did not settle within 25s; store={rec}")
        artifact = next(p for p in stored.get("artifacts", []) if p.get("parts"))
        artifact_text = artifact["parts"][0]["text"]
        state = stored["status"]["state"]
        entries = protocol.load_conversation(context_id, limit=0)
        agent_entries = [e for e in entries if e.get("role") == "agent"]
        latest_agent = agent_entries[-1]["text"] if agent_entries else ""
        print(f"TERMINAL state={state} artifact={artifact_text!r}")
        print(f"CONVERSATION agent={latest_agent!r} entries={len(entries)}")
        print(f"METADATA {stored.get('metadata')}")
        passed = (state == "TASK_STATE_COMPLETED" and first_frame_seconds <= 2.0
                 and artifact_text == latest_agent == "DETACHED-ARTIFACT-OK"
                 and stored.get("metadata") == {"issue_id": "MOJ-2246", "round": "real-link", "trace_id": "detached-finalization"})
        print(f"RESULT {'PASS' if passed else 'FAIL'}")
        return 0 if passed else 1
    finally:
        await adapter.disconnect()


if __name__ == "__main__":
    raise SystemExit(asyncio.run(run()))
