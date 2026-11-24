//! std threads only: no tokio, no poll(2). One acceptor thread plus a fixed pool of
//! workers (one per CPU). std cannot tell us which socket is ready, so every thread
//! scans its non-blocking sockets and backs off (sleeps) while nothing moves.
//!
//! The five lifetimes:
//!   'rt1, 'rt2     the two `Ws` owned by a `Pair`
//!   'flow1, 'flow2 the two `Flow` fields of that `Pair`
//!   'pipe          the `Pair` itself, from the moment it enters a worker until it is closed
//! They are no longer borrows on a stack: they are entries in a worker's Vec.

use std::io::ErrorKind::WouldBlock;
use std::mem;
use std::net::{TcpListener, TcpStream};
use std::sync::mpsc::{channel, Receiver, Sender, TryRecvError};
use std::thread;
use std::time::Duration;

use tungstenite::handshake::server::{NoCallback, ServerHandshake};
use tungstenite::handshake::{HandshakeError, MidHandshake};
use tungstenite::{accept, Error, Message, WebSocket};

type Ws = WebSocket<TcpStream>;
type Handshake = MidHandshake<ServerHandshake<TcpStream, NoCallback>>;

/// Sleeps longer and longer while a scan moves nothing; the price of not having poll(2).
/// The cap is also the worst extra latency a message can see.
struct Backoff(Duration);

impl Backoff {
    const MIN: Duration = Duration::from_micros(50);
    const MAX: Duration = Duration::from_millis(5);

    fn new() -> Self {
        Backoff(Duration::ZERO)
    }

    fn after(&mut self, moved: bool) {
        if moved {
            self.0 = Duration::ZERO;
            return;
        }
        self.0 = (self.0 * 2).clamp(Self::MIN, Self::MAX);
        thread::sleep(self.0);
    }
}

fn would_block(e: &Error) -> bool {
    matches!(e, Error::Io(e) if e.kind() == WouldBlock)
}

/// One direction. Holds at most one message that has not been flushed yet.
enum Flow {
    Read,
    Write(Message),
    Flush,
}

enum Step {
    Moved,
    Blocked,
    Ended,
}

impl Flow {
    /// One atomic step: moves at most one message from src to dst.
    fn step(&mut self, src: &mut Ws, dst: &mut Ws) -> Step {
        loop {
            match mem::replace(self, Flow::Read) {
                Flow::Read => match src.read() {
                    Ok(m) if m.is_text() || m.is_binary() => *self = Flow::Write(m),
                    Ok(Message::Close(_)) => return Step::Ended,
                    Ok(_) => {} // Ping/Pong are answered by tungstenite itself
                    Err(e) if would_block(&e) => return Step::Blocked,
                    Err(_) => return Step::Ended,
                },
                Flow::Write(m) => match dst.write(m) {
                    // On WouldBlock the frame is kept in tungstenite's write buffer.
                    Ok(()) => *self = Flow::Flush,
                    Err(e) if would_block(&e) => *self = Flow::Flush,
                    Err(_) => return Step::Ended,
                },
                Flow::Flush => match dst.flush() {
                    Ok(()) => return Step::Moved,
                    Err(e) if would_block(&e) => {
                        *self = Flow::Flush;
                        return Step::Blocked;
                    }
                    Err(_) => return Step::Ended,
                },
            }
        }
    }
}

/// The pipe: owns both connections and both flows.
struct Pair {
    a: Ws,
    b: Ws,
    a2b: Flow,
    b2a: Flow,
}

impl Pair {
    /// Most messages one pair may move per scan, so a busy pair cannot starve the others.
    const BUDGET: usize = 64;

    fn new(a: Ws, b: Ws) -> Self {
        Pair { a, b, a2b: Flow::Read, b2a: Flow::Read }
    }

    /// Ping-pong: one step of a2b, one step of b2a, until both are blocked or the budget is spent.
    /// None once the pipe has ended, otherwise whether anything moved.
    fn relay(&mut self) -> Option<bool> {
        let (mut moved, mut idle, mut a2b) = (0, 0, true);
        while idle < 2 && moved < Self::BUDGET {
            let step = match a2b {
                true => self.a2b.step(&mut self.a, &mut self.b),
                false => self.b2a.step(&mut self.b, &mut self.a),
            };
            match step {
                Step::Ended => return None,
                Step::Moved => (moved, idle) = (moved + 1, 0),
                Step::Blocked => idle += 1,
            }
            a2b = !a2b;
        }
        Some(moved > 0)
    }

    /// Delivers what the flows still hold, then closes both sides. Blocking from here on,
    /// with a timeout, so a dead or stalled peer cannot hold the worker for long.
    fn close(mut self) {
        for ws in [&self.a, &self.b] {
            let _ = ws.get_ref().set_nonblocking(false);
            let _ = ws.get_ref().set_write_timeout(Some(Duration::from_secs(1)));
        }
        if let Flow::Write(m) = mem::replace(&mut self.a2b, Flow::Read) {
            let _ = self.b.write(m);
        }
        if let Flow::Write(m) = mem::replace(&mut self.b2a, Flow::Read) {
            let _ = self.a.write(m);
        }
        for ws in [&mut self.a, &mut self.b] {
            let _ = ws.close(None);
            let _ = ws.flush();
        }
    }
}

/// A worker owns many pairs and scans them all, round and round.
fn worker(rx: Receiver<Pair>) {
    let mut pairs: Vec<Pair> = Vec::new();
    let mut backoff = Backoff::new();
    loop {
        // Nothing to do: block on the channel instead of spinning.
        if pairs.is_empty() {
            match rx.recv() {
                Ok(p) => pairs.push(p),
                Err(_) => return,
            }
        }
        loop {
            match rx.try_recv() {
                Ok(p) => pairs.push(p),
                Err(TryRecvError::Empty) => break,
                Err(TryRecvError::Disconnected) => return,
            }
        }
        let mut moved = false;
        let mut i = 0;
        while i < pairs.len() {
            match pairs[i].relay() {
                None => pairs.swap_remove(i).close(),
                Some(m) => {
                    moved |= m;
                    i += 1;
                }
            }
        }
        backoff.after(moved);
    }
}

/// Alive unless the peer has sent FIN or reset. peek() leaves any pending data unread.
fn alive(ws: &Ws) -> bool {
    match ws.get_ref().peek(&mut [0u8; 1]) {
        Ok(n) => n > 0,
        Err(e) => e.kind() == WouldBlock,
    }
}

/// The acceptor: accepts, finishes handshakes, pairs neighbours, deals pairs out to the workers.
fn main() -> std::io::Result<()> {
    let listener = TcpListener::bind("0.0.0.0:8080")?;
    listener.set_nonblocking(true)?;
    let workers: Vec<Sender<Pair>> = (0..thread::available_parallelism()?.get())
        .map(|_| {
            let (tx, rx) = channel();
            thread::spawn(move || worker(rx));
            tx
        })
        .collect();
    let mut next = 0;
    let mut handshakes: Vec<Handshake> = Vec::new();
    let mut lobby: Option<Ws> = None;
    let mut backoff = Backoff::new();
    loop {
        let mut moved = false;
        let mut arrived: Vec<Ws> = Vec::new();
        loop {
            match listener.accept() {
                Ok((stream, _)) => {
                    moved = true;
                    stream.set_nonblocking(true)?;
                    match accept(stream) {
                        Ok(ws) => arrived.push(ws),
                        Err(HandshakeError::Interrupted(mid)) => handshakes.push(mid),
                        Err(HandshakeError::Failure(_)) => {}
                    }
                }
                Err(e) if e.kind() == WouldBlock => break,
                Err(e) => return Err(e),
            }
        }
        for mid in mem::take(&mut handshakes) {
            match mid.handshake() {
                Ok(ws) => arrived.push(ws),
                Err(HandshakeError::Interrupted(mid)) => handshakes.push(mid),
                Err(HandshakeError::Failure(_)) => {}
            }
        }
        // A waiter that hung up leaves the lobby instead of being paired.
        if lobby.as_ref().is_some_and(|ws| !alive(ws)) {
            lobby = None;
        }
        for ws in arrived {
            moved = true;
            match lobby.take() {
                Some(waiting) => {
                    let _ = workers[next].send(Pair::new(waiting, ws));
                    next = (next + 1) % workers.len();
                }
                None => lobby = Some(ws),
            }
        }
        backoff.after(moved);
    }
}
