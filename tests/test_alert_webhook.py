"""Real loopback HTTP, no external webhook or database."""

import asyncio
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

from spine import alerts


class _Cursor:
    async def __aenter__(self):
        return self

    async def __aexit__(self, *args):
        return None

    async def execute(self, *args):
        return None


class _Connection:
    def cursor(self, **kwargs):
        return _Cursor()


async def test_webhook_keeps_event_loop_responsive_and_closes_response(monkeypatch):
    received = []

    class Handler(BaseHTTPRequestHandler):
        def do_POST(self):
            received.append(self.rfile.read(int(self.headers["Content-Length"])))
            time.sleep(0.15)
            self.send_response(200)
            self.end_headers()

        def log_message(self, *args):
            pass

    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    responses = []
    original_open = alerts.urllib.request.urlopen

    def tracked_open(*args, **kwargs):
        response = original_open(*args, **kwargs)
        responses.append(response)
        return response

    monkeypatch.setattr(alerts.urllib.request, "urlopen", tracked_open)
    monkeypatch.setenv("ALERT_WEBHOOK_URL", f"http://127.0.0.1:{server.server_port}")
    ticks = 0
    done = asyncio.Event()

    async def ticker():
        nonlocal ticks
        while not done.is_set():
            ticks += 1
            await asyncio.sleep(0.01)

    task = asyncio.create_task(ticker())
    try:
        await alerts._raise_alert(
            _Connection(), kind="failure_rate", job_id=None, variant="test", detail={}
        )
        assert ticks >= 3
        assert len(received) == 1
        assert len(responses) == 1
        assert responses[0].closed
    finally:
        done.set()
        await task
        for response in responses:
            response.close()
        await asyncio.to_thread(server.shutdown)
        server.server_close()
        thread.join(timeout=1)
