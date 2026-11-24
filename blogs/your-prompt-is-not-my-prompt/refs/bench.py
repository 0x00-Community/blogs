#!/usr/bin/env python3
"""Stress-test the five relays: peak RSS, median RSS and CPU of the server process.

Builds relay.go, relay_tokio.rs, relay_std.rs, relay_single.rs and relay_jw238.rs
from this folder,
then for each scenario and each relay:
  1. starts the server,
  2. connects 2 * pairs clients and waits for them to be paired,
  3. every client sends `msgs` binary messages of `size` bytes and expects
     the same number back from its partner (full duplex, both at once),
  4. samples the server's RSS and CPU time from /proc while traffic flows.

Needs the `websockets` package (pip install websockets). Linux only (/proc).
"""

import argparse
import asyncio
import os
import shutil
import statistics
import subprocess
import sys
import tempfile
import time
from dataclasses import dataclass
from enum import Enum
from pathlib import Path

from websockets.asyncio.client import ClientConnection, connect
from websockets.exceptions import ConnectionClosed

HERE = Path(__file__).resolve().parent
CLK_TCK = os.sysconf("SC_CLK_TCK")
PAGE_KIB = os.sysconf("SC_PAGE_SIZE") // 1024

CARGO_TOML = """[package]
name = "relay-bench"
version = "0.1.0"
edition = "2024"

[dependencies]
futures-util = "0.3"
libc = "0.2"
tokio = { version = "1", features = ["full"] }
tokio-tungstenite = "0.30"
tungstenite = "0.30"

# relay_std.rs and relay_single.rs were written as edition 2021 crates
[[bin]]
name = "relay_std"
path = "src/bin/relay_std.rs"
edition = "2021"

[[bin]]
name = "relay_single"
path = "src/bin/relay_single.rs"
edition = "2021"

[profile.release]
debug = false
"""


class Relay(Enum):
    GO = "relay.go"
    TOKIO = "relay_tokio.rs"
    STD = "relay_std.rs"
    SINGLE = "relay_single.rs"
    JW238 = "relay_jw238.rs"

    @property
    def stem(self) -> str:
        return Path(self.value).stem

    @property
    def rust(self) -> bool:
        return self is not Relay.GO

    def source(self, port: int) -> str:
        """Source with a hard-coded listen address rewritten to the bench port."""
        src = (HERE / self.value).read_text()
        for addr in ("0.0.0.0:8080", "0.0.0.0:9000"):
            src = src.replace(f'"{addr}"', f'"127.0.0.1:{port}"')
        return src

    def argv(self, binary: Path, port: int) -> list[str]:
        """Command line; relays that take the address as an argument get it here."""
        match self:
            case Relay.GO:
                return [str(binary), "-addr", f"127.0.0.1:{port}"]
            case Relay.STD | Relay.SINGLE:
                return [str(binary), f"127.0.0.1:{port}"]
            case _:
                return [str(binary)]


@dataclass
class Scenario:
    name: str
    pairs: int
    msgs: int
    size: int


SCENARIOS = {
    "many": Scenario("many connections", pairs=1000, msgs=20, size=1024),
    "bulk": Scenario("bulk transfer", pairs=50, msgs=200, size=64 * 1024),
}


# ---------------------------------------------------------------- build


def build(build_dir: Path, port: int) -> dict[Relay, Path]:
    """Build every relay with its port rewritten; returns the binary of each."""
    rust = build_dir / "rust"
    (rust / "src" / "bin").mkdir(parents=True, exist_ok=True)
    (rust / "Cargo.toml").write_text(CARGO_TOML)
    (rust / "src" / "lib.rs").write_text("")
    for r in filter(lambda r: r.rust, Relay):
        (rust / "src" / "bin" / r.value).write_text(r.source(port))
    subprocess.run(["cargo", "build", "--release", "-q"], cwd=rust, check=True)

    go = build_dir / "go"
    go.mkdir(parents=True, exist_ok=True)
    (go / "main.go").write_text(Relay.GO.source(port))
    if not (go / "go.mod").exists():
        subprocess.run(["go", "mod", "init", "relaybench"], cwd=go, check=True, capture_output=True)
        subprocess.run(["go", "get", "github.com/gorilla/websocket@v1.5.3"], cwd=go, check=True, capture_output=True)
    subprocess.run(["go", "build", "-o", "relay_go", "."], cwd=go, check=True)

    bins = {r: rust / "target" / "release" / r.stem for r in Relay if r.rust}
    bins[Relay.GO] = go / "relay_go"
    return bins


# ---------------------------------------------------------------- client


class Client:
    """One benchmark client on top of the `websockets` library."""

    def __init__(self, ws: ClientConnection):
        self.ws = ws

    @classmethod
    async def connect(cls, port: int) -> "Client":
        ws = await connect(f"ws://127.0.0.1:{port}/ws", compression=None, max_size=None, close_timeout=1)
        return cls(ws)

    async def send(self, payload: bytes, msgs: int) -> None:
        for _ in range(msgs):
            await self.ws.send(payload)

    async def recv(self, msgs: int) -> None:
        for got in range(msgs):
            try:
                await self.ws.recv()
            except ConnectionClosed as e:
                raise ConnectionError(f"closed by relay after {got}/{msgs} messages") from e

    async def run(self, payload: bytes, msgs: int) -> None:
        await asyncio.gather(self.send(payload, msgs), self.recv(msgs))

    async def close(self) -> None:
        await self.ws.close()


# ---------------------------------------------------------------- measuring


class Proc:
    """RSS and CPU time of one process, read from /proc."""

    def __init__(self, pid: int):
        self.pid = pid

    def rss_kib(self) -> int:
        return int(Path(f"/proc/{self.pid}/statm").read_text().split()[1]) * PAGE_KIB

    def cpu_s(self) -> float:
        fields = Path(f"/proc/{self.pid}/stat").read_text().rsplit(")", 1)[1].split()
        return (int(fields[11]) + int(fields[12])) / CLK_TCK  # utime + stime

    def threads(self) -> int:
        return len(os.listdir(f"/proc/{self.pid}/task"))


@dataclass
class Result:
    relay: Relay
    ok: bool
    idle_kib: int
    peak_kib: int
    median_kib: int
    cpu_s: float
    wall_s: float
    threads: int
    error: str = ""

    @property
    def cpu_pct(self) -> float:
        return 100 * self.cpu_s / self.wall_s if self.wall_s else 0.0


async def sample(proc: Proc, out: list[int], stop: asyncio.Event, every: float) -> None:
    while not stop.is_set():
        out.append(proc.rss_kib())
        try:
            await asyncio.wait_for(stop.wait(), every)
        except TimeoutError:
            pass


async def bench_one(relay: Relay, binary: Path, port: int, sc: Scenario, timeout: float) -> Result:
    server = subprocess.Popen(relay.argv(binary, port), stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    proc = Proc(server.pid)
    clients: list[Client] = []
    try:
        # Wait for the listener with a bare TCP probe: a websocket probe would sit in
        # the lobby as a dead peer and shift every later pairing by one.
        for _ in range(50):
            try:
                _, w = await asyncio.open_connection("127.0.0.1", port)
                w.close()
                break
            except OSError:
                await asyncio.sleep(0.1)
        # Connect sequentially, so the relays' lobbies pair neighbours.
        for _ in range(2 * sc.pairs):
            clients.append(await Client.connect(port))
        await asyncio.sleep(1.0)  # let the server finish pairing before measuring idle RSS
        idle = proc.rss_kib()

        samples: list[int] = []
        stop = asyncio.Event()
        sampler = asyncio.create_task(sample(proc, samples, stop, 0.01))
        payload = os.urandom(sc.size)
        cpu0, t0 = proc.cpu_s(), time.perf_counter()
        error = ""
        try:
            await asyncio.wait_for(asyncio.gather(*(c.run(payload, sc.msgs) for c in clients)), timeout)
        except (TimeoutError, ConnectionError) as e:
            error = f"{type(e).__name__}: {e}"
        wall, cpu = time.perf_counter() - t0, proc.cpu_s() - cpu0
        threads = proc.threads()
        stop.set()
        await sampler
        return Result(relay, not error, idle, max(samples), int(statistics.median(samples)), cpu, wall, threads, error)
    finally:
        server.kill()
        server.wait()
        await asyncio.gather(*(c.close() for c in clients), return_exceptions=True)


# ---------------------------------------------------------------- report


def mib(kib: int) -> str:
    return f"{kib / 1024:.1f}"


def report(sc: Scenario, results: list[Result]) -> str:
    total = 2 * sc.pairs * sc.msgs * sc.size / (1 << 20)
    lines = [
        f"### {sc.name}: {sc.pairs} pairs, {sc.msgs} x {sc.size} B each way per client ({total:.0f} MiB relayed)",
        "",
        "| relay | idle RSS MiB | peak RSS MiB | median RSS MiB | CPU s | CPU % | wall s | threads | ok |",
        "| --- | --- | --- | --- | --- | --- | --- | --- | --- |",
    ]
    for r in results:
        lines.append(
            f"| `{r.relay.value}` | {mib(r.idle_kib)} | {mib(r.peak_kib)} | {mib(r.median_kib)} "
            f"| {r.cpu_s:.2f} | {r.cpu_pct:.0f} | {r.wall_s:.2f} | {r.threads} | {'yes' if r.ok else r.error} |"
        )
    return "\n".join(lines)


async def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--port", type=int, default=18080)
    ap.add_argument("--build-dir", type=Path, default=Path(tempfile.gettempdir()) / "relay-bench")
    ap.add_argument("--scenario", choices=[*SCENARIOS, "all"], default="all")
    ap.add_argument("--relay", choices=[r.stem for r in Relay], action="append")
    ap.add_argument("--timeout", type=float, default=60)
    args = ap.parse_args()

    for tool in ("cargo", "go"):
        if not shutil.which(tool):
            sys.exit(f"{tool} not found in PATH")
    bins = build(args.build_dir, args.port)
    relays = [r for r in Relay if not args.relay or r.stem in args.relay]
    scenarios = SCENARIOS.values() if args.scenario == "all" else [SCENARIOS[args.scenario]]
    for sc in scenarios:
        results = []
        for r in relays:
            print(f"[{sc.name}] {r.value} ...", file=sys.stderr, flush=True)
            results.append(await bench_one(r, bins[r], args.port, sc, args.timeout))
        print(report(sc, results) + "\n", flush=True)


if __name__ == "__main__":
    asyncio.run(main())
