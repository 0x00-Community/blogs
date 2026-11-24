#!/usr/bin/env python3
"""Behaviour tests for the five relays (reuses bench.py's build).

  basic    A and B exchange a text and a 100KB binary message, then A drops its TCP
           without a close frame. What does B get?
  lobby    A connects and drops while waiting. Then B and C connect: are they paired?
  early    A sends a message before anyone is there. Does B get it after pairing?
  loss     A and B each send 500 x 64KB at the same time. Once A has received all of
           B's messages and flushed its own, A drops its TCP without a close frame.
           How many of A's 500 does B get? Five rounds.
"""

import argparse
import asyncio
import os
import subprocess
import tempfile
from pathlib import Path

from websockets.asyncio.client import connect
from websockets.exceptions import ConnectionClosed

from bench import Relay, build


class Server:
    def __init__(self, relay: Relay, binary: Path, port: int):
        self.argv, self.port = relay.argv(binary, port), port

    async def __aenter__(self):
        self.p = subprocess.Popen(self.argv, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        for _ in range(50):
            try:
                _, w = await asyncio.open_connection("127.0.0.1", self.port)
                w.close()
                return self
            except OSError:
                await asyncio.sleep(0.1)
        raise RuntimeError("server did not start")

    async def __aexit__(self, *_):
        self.p.kill()
        self.p.wait()

    async def client(self):
        return await connect(f"ws://127.0.0.1:{self.port}/ws", compression=None, max_size=None, close_timeout=1)


def drop(ws) -> None:
    """Close the TCP connection without a WebSocket close frame."""
    ws.transport.close()


async def until_flushed(ws) -> None:
    while ws.transport.get_write_buffer_size():
        await asyncio.sleep(0.01)


async def basic(s: Server) -> str:
    a = await s.client()
    b = await s.client()
    await a.send("hi")
    await b.send(os.urandom(100_000))
    if await asyncio.wait_for(b.recv(), 3) != "hi" or len(await asyncio.wait_for(a.recv(), 3)) != 100_000:
        return "relay failed"
    drop(a)
    try:
        await asyncio.wait_for(b.recv(), 5)
        return "B got an unexpected message"
    except ConnectionClosed as e:
        code = e.rcvd.code if e.rcvd else 1006
        return f"ok, B closed with {code}"
    except TimeoutError:
        return "B never disconnected"


async def lobby(s: Server) -> str:
    a = await s.client()
    await asyncio.sleep(0.3)
    drop(a)
    await asyncio.sleep(1.5)  # the std relay probes its lobby once a second
    b = await s.client()
    c = await s.client()
    await b.send("x")
    try:
        return "ok, B and C paired" if await asyncio.wait_for(c.recv(), 3) == "x" else "wrong message"
    except ConnectionClosed:
        return "C was disconnected"
    except TimeoutError:
        return "C got nothing"


async def early(s: Server) -> str:
    a = await s.client()
    await a.send("early")
    await asyncio.sleep(0.3)
    b = await s.client()
    try:
        return "ok" if await asyncio.wait_for(b.recv(), 3) == "early" else "wrong message"
    except (ConnectionClosed, TimeoutError) as e:
        return f"lost ({type(e).__name__})"


async def loss(s: Server, n: int = 500) -> int:
    a = await s.client()
    b = await s.client()
    payload = os.urandom(64 * 1024)

    async def a_side():
        async def send():
            for _ in range(n):
                await a.send(payload)
            await until_flushed(a)

        async def recv():
            for _ in range(n):
                await a.recv()

        await asyncio.gather(send(), recv())
        drop(a)

    async def b_side() -> int:
        async def send():
            try:
                for _ in range(n):
                    await b.send(payload)
            except ConnectionClosed:
                pass

        got = 0

        async def recv():
            nonlocal got
            try:
                while got < n:
                    await b.recv()
                    got += 1
            except ConnectionClosed:
                pass

        await asyncio.gather(send(), recv())
        return got

    _, got = await asyncio.wait_for(asyncio.gather(a_side(), b_side()), 30)
    await b.close()
    return got


async def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--port", type=int, default=18081)
    ap.add_argument("--build-dir", type=Path, default=Path(tempfile.gettempdir()) / "relay-bench")
    ap.add_argument("--rounds", type=int, default=5)
    args = ap.parse_args()
    bins = build(args.build_dir, args.port)

    print("| relay | basic | lobby | early | loss: messages B got of 500, per round |")
    print("| --- | --- | --- | --- | --- |")
    for r in Relay:
        row = []
        for test in (basic, lobby, early):
            async with Server(r, bins[r], args.port) as s:
                try:
                    row.append(await test(s))
                except Exception as e:  # noqa: BLE001 - report, do not stop
                    row.append(f"error: {type(e).__name__}")
        rounds = []
        for _ in range(args.rounds):
            async with Server(r, bins[r], args.port) as s:
                try:
                    rounds.append(str(await loss(s)))
                except Exception as e:  # noqa: BLE001
                    rounds.append(type(e).__name__)
        print(f"| `{r.value}` | {' | '.join(row)} | {', '.join(rounds)} |", flush=True)


if __name__ == "__main__":
    asyncio.run(main())
