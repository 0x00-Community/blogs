//! One thread, no tokio: a poll(2) reactor, a minimal executor, one Future per pair,
//! and the two directions take turns (ping-pong).
//!
//! Five lifetimes, each one named in the code:
//!   'rt1   async runtime 1: side A, its socket plus the readiness that wakes it
//!   'rt2   async runtime 2: side B, its socket plus the readiness that wakes it
//!   'flow1 the flow of rt1 (A -> B), including the message it may still be holding
//!   'flow2 the flow of rt2 (B -> A), including the message it may still be holding
//!   'pipe  the bridge between rt1 and rt2: the Future returned by `relay`
//! The only hard constraint is that 'pipe ends no later than any of the other four.
//! The reactor is part of each runtime, not a lifetime of its own.

use std::cell::RefCell;
use std::collections::HashMap;
use std::future::{poll_fn, Future};
use std::io::ErrorKind::WouldBlock;
use std::mem;
use std::net::{TcpListener, TcpStream};
use std::os::fd::{AsRawFd, RawFd};
use std::pin::Pin;
use std::rc::Rc;
use std::sync::atomic::{AtomicBool, Ordering::Relaxed};
use std::sync::Arc;
use std::task::{ready, Context, Poll, Wake, Waker};

use libc::{pollfd, POLLERR, POLLHUP, POLLIN, POLLOUT, POLLRDHUP};
use tungstenite::handshake::HandshakeError;
use tungstenite::{accept, Error, Message, WebSocket};

type Ws = WebSocket<TcpStream>;

// ---------------------------------------------------------------- reactor and executor

/// Which task waits for which fd and event. One poll(2) call serves them all.
#[derive(Default)]
struct Reactor {
    interest: RefCell<HashMap<(RawFd, i16), Waker>>,
}

impl Reactor {
    fn want(&self, fd: RawFd, events: i16, cx: &Context<'_>) {
        self.interest.borrow_mut().insert((fd, events), cx.waker().clone());
    }

    /// Blocks in poll(2) until the listener or a registered fd is ready, then wakes those tasks.
    fn wait(&self, listener: RawFd) {
        let mut fds: Vec<pollfd> = self.interest.borrow().keys()
            .map(|&(fd, events)| pollfd { fd, events, revents: 0 })
            .chain([pollfd { fd: listener, events: POLLIN, revents: 0 }])
            .collect();
        // SAFETY: `fds` is a valid array of pollfd that poll(2) may write to.
        if unsafe { libc::poll(fds.as_mut_ptr(), fds.len() as libc::nfds_t, -1) } <= 0 {
            return;
        }
        let mut interest = self.interest.borrow_mut();
        for p in fds.iter().filter(|p| p.revents != 0) {
            if let Some(w) = interest.remove(&(p.fd, p.events)) {
                w.wake();
            }
        }
    }
}

/// Wakers must be Send + Sync, so even on one thread the flag is an atomic.
struct Woken(AtomicBool);

impl Wake for Woken {
    fn wake(self: Arc<Self>) {
        self.0.store(true, Relaxed);
    }
}

/// A task borrows the reactor and the lobby for 'r: no 'static anywhere.
struct Task<'r> {
    fut: Pin<Box<dyn Future<Output = ()> + 'r>>,
    woken: Arc<Woken>,
}

impl<'r> Task<'r> {
    fn new(fut: impl Future<Output = ()> + 'r) -> Self {
        Task { fut: Box::pin(fut), woken: Arc::new(Woken(AtomicBool::new(true))) }
    }

    fn is_woken(&self) -> bool {
        self.woken.0.load(Relaxed)
    }

    /// Polls the task if it has been woken; true once it has finished.
    fn poll_if_woken(&mut self) -> bool {
        if !self.woken.0.swap(false, Relaxed) {
            return false;
        }
        let waker = Waker::from(self.woken.clone());
        self.fut.as_mut().poll(&mut Context::from_waker(&waker)).is_ready()
    }
}

// ---------------------------------------------------------------- connections

/// A websocket on a non-blocking socket, plus the reactor that wakes us when it is ready.
struct Conn<'r> {
    ws: Ws,
    reactor: &'r Reactor,
}

impl<'r> Conn<'r> {
    fn would_block(e: &Error) -> bool {
        matches!(e, Error::Io(e) if e.kind() == WouldBlock)
    }

    fn fd(&self) -> RawFd {
        self.ws.get_ref().as_raw_fd()
    }

    fn want(&self, events: i16, cx: &Context<'_>) {
        self.reactor.want(self.fd(), events, cx);
    }

    /// Has the peer hung up? Asks poll(2) for POLLRDHUP only, so pending data stays unread.
    fn hung_up(&self) -> bool {
        let mut p = pollfd { fd: self.fd(), events: POLLRDHUP, revents: 0 };
        // SAFETY: one valid pollfd, zero timeout.
        let ready = unsafe { libc::poll(&mut p, 1, 0) };
        ready > 0 && p.revents & (POLLRDHUP | POLLHUP | POLLERR) != 0
    }

    /// Ready(None): the connection is gone.
    fn poll_read(&mut self, cx: &mut Context<'_>) -> Poll<Option<Message>> {
        match self.ws.read() {
            Ok(m) => Poll::Ready(Some(m)),
            Err(e) if Self::would_block(&e) => {
                self.want(POLLIN, cx);
                Poll::Pending
            }
            Err(_) => Poll::Ready(None),
        }
    }

    /// Ready(true): flushed. Ready(false): the connection is gone.
    fn poll_flush(&mut self, cx: &mut Context<'_>) -> Poll<bool> {
        match self.ws.flush() {
            Ok(()) => Poll::Ready(true),
            Err(e) if Self::would_block(&e) => {
                self.want(POLLOUT, cx);
                Poll::Pending
            }
            Err(_) => Poll::Ready(false),
        }
    }

    async fn close(&mut self) {
        let _ = self.ws.close(None);
        poll_fn(|cx| self.poll_flush(cx)).await;
    }
}

// ---------------------------------------------------------------- flow and pipe

/// One direction. Holds at most one message that has not been flushed yet.
enum Flow {
    Read,
    Write(Message),
    Flush,
}

#[derive(Clone, Copy)]
enum Turn {
    A2B,
    B2A,
}

impl Turn {
    fn flip(self) -> Self {
        match self {
            Turn::A2B => Turn::B2A,
            Turn::B2A => Turn::A2B,
        }
    }
}

impl Flow {
    /// One atomic step: moves at most one message.
    /// Ready(Some): moved one. Ready(None): this direction has ended. Pending: stuck for now.
    fn poll_step(&mut self, cx: &mut Context<'_>, src: &mut Conn<'_>, dst: &mut Conn<'_>) -> Poll<Option<()>> {
        loop {
            match mem::replace(self, Flow::Read) {
                Flow::Read => match ready!(src.poll_read(cx)) {
                    Some(m) if m.is_text() || m.is_binary() => *self = Flow::Write(m),
                    Some(Message::Close(_)) | None => return Poll::Ready(None),
                    Some(_) => {} // Ping/Pong are answered by tungstenite itself
                },
                Flow::Write(m) => match dst.ws.write(m) {
                    // On WouldBlock the frame is kept in tungstenite's write buffer.
                    Ok(()) => *self = Flow::Flush,
                    Err(e) if Conn::would_block(&e) => *self = Flow::Flush,
                    Err(_) => return Poll::Ready(None),
                },
                Flow::Flush => {
                    return match dst.poll_flush(cx) {
                        Poll::Ready(true) => Poll::Ready(Some(())),
                        Poll::Ready(false) => Poll::Ready(None),
                        Poll::Pending => {
                            *self = Flow::Flush;
                            Poll::Pending
                        }
                    };
                }
            }
        }
    }

    /// The flow outlives the pipe, so a message it still holds is not lost with the pipe:
    /// hand it to `dst` if `dst` is still there.
    async fn drain(self, dst: &mut Conn<'_>) {
        match self {
            Flow::Read => return,
            Flow::Write(m) => {
                let _ = dst.ws.write(m);
            }
            Flow::Flush => {}
        }
        poll_fn(|cx| dst.poll_flush(cx)).await;
    }
}

/// The pipe: the bridge between rt1 and rt2. Borrows both runtimes and both flows; the returned
/// Future is 'pipe, and 'rt1: 'pipe etc. say that the bridge can never outlive any of them.
///
/// Ping-pong: one step of flow1, one step of flow2. If one side is stuck, switch to the other;
/// only when both are stuck do we go back to the event loop.
fn relay<'pipe, 'rt1: 'pipe, 'rt2: 'pipe, 'flow1: 'pipe, 'flow2: 'pipe>(
    rt1: &'rt1 mut Conn<'_>,
    rt2: &'rt2 mut Conn<'_>,
    flow1: &'flow1 mut Flow,
    flow2: &'flow2 mut Flow,
) -> impl Future<Output = ()> + 'pipe {
    let mut turn = Turn::A2B;
    poll_fn(move |cx| {
        let mut idle = 0;
        while idle < 2 {
            let step = match turn {
                Turn::A2B => flow1.poll_step(cx, rt1, rt2),
                Turn::B2A => flow2.poll_step(cx, rt2, rt1),
            };
            match step {
                Poll::Ready(None) => return Poll::Ready(()), // either direction ends, so the pipe ends
                Poll::Ready(Some(())) => idle = 0,
                Poll::Pending => idle += 1,
            }
            turn = turn.flip();
        }
        Poll::Pending
    })
}

// ---------------------------------------------------------------- lobby and rt

/// Where the second connection drops itself for the first one to pick up.
#[derive(Default)]
struct Mailbox {
    peer: Option<Ws>,
    waker: Option<Waker>,
}

type Lobby = RefCell<Option<Rc<RefCell<Mailbox>>>>;

async fn handshake(stream: TcpStream, reactor: &Reactor) -> Option<Ws> {
    let fd = stream.as_raw_fd();
    let mut mid = match accept(stream) {
        Ok(ws) => return Some(ws),
        Err(HandshakeError::Interrupted(mid)) => Some(mid),
        Err(HandshakeError::Failure(_)) => return None,
    };
    poll_fn(|cx| {
        let Some(m) = mid.take() else { return Poll::Ready(None) };
        match m.handshake() {
            Ok(ws) => Poll::Ready(Some(ws)),
            Err(HandshakeError::Interrupted(m)) => {
                reactor.want(fd, POLLIN, cx);
                mid = Some(m);
                Poll::Pending
            }
            Err(HandshakeError::Failure(_)) => Poll::Ready(None),
        }
    })
    .await
}

/// The task of the connection that arrives first. It owns 'rt1 from the handshake,
/// receives 'rt2 from the second connection's task, and creates 'flow1, 'flow2 and 'pipe.
async fn rt<'r>(stream: TcpStream, reactor: &'r Reactor, lobby: &'r Lobby) {
    let Some(ws) = handshake(stream, reactor).await else { return };
    let mut a = Conn { ws, reactor };
    loop {
        // Someone is waiting: hand ourselves over (we become their 'rt2), and this task is done.
        let waiting = lobby.borrow_mut().take();
        if let Some(mailbox) = waiting {
            let mut m = mailbox.borrow_mut();
            m.peer = Some(a.ws);
            if let Some(w) = m.waker.take() {
                w.wake();
            }
            return;
        }
        let mailbox = Rc::new(RefCell::new(Mailbox::default()));
        *lobby.borrow_mut() = Some(mailbox.clone());

        // Wait for a peer while watching only for a hang-up:
        // whatever A sends meanwhile stays unread in the socket, nothing is discarded.
        let peer = poll_fn(|cx| {
            if a.hung_up() {
                return Poll::Ready(None);
            }
            let mut m = mailbox.borrow_mut();
            match m.peer.take() {
                Some(b) => Poll::Ready(Some(b)),
                None => {
                    m.waker = Some(cx.waker().clone());
                    a.want(POLLRDHUP, cx);
                    Poll::Pending
                }
            }
        })
        .await;

        let Some(b) = peer else {
            // A is gone. Leave the lobby; if someone handed us a connection meanwhile, carry it back.
            if lobby.borrow().as_ref().is_some_and(|m| Rc::ptr_eq(m, &mailbox)) {
                lobby.borrow_mut().take();
            }
            match mailbox.borrow_mut().peer.take() {
                Some(b) => a = Conn { ws: b, reactor },
                None => return,
            }
            continue;
        };

        let mut b = Conn { ws: b, reactor };
        let (mut flow1, mut flow2) = (Flow::Read, Flow::Read);
        relay(&mut a, &mut b, &mut flow1, &mut flow2).await; // 'pipe ends here
        // 'flow1 and 'flow2 end here: deliver what they still hold.
        flow1.drain(&mut b).await;
        flow2.drain(&mut a).await;
        // 'rt1 and 'rt2 end here. The broken side errors out at once;
        // the healthy side is flushed first, then gets a Close frame.
        a.close().await;
        b.close().await;
        return;
    }
}

fn main() -> std::io::Result<()> {
    let listener = TcpListener::bind("0.0.0.0:8080")?;
    listener.set_nonblocking(true)?;
    let (reactor, lobby) = (Reactor::default(), Lobby::default());
    let mut tasks: Vec<Task<'_>> = Vec::new();
    loop {
        loop {
            match listener.accept() {
                Ok((stream, _)) => {
                    stream.set_nonblocking(true)?;
                    tasks.push(Task::new(rt(stream, &reactor, &lobby)));
                }
                Err(e) if e.kind() == WouldBlock => break,
                Err(e) => return Err(e),
            }
        }
        // A task may wake another one (the mailbox), so run until nobody is woken.
        while tasks.iter().any(Task::is_woken) {
            tasks.retain_mut(|t| !t.poll_if_woken());
        }
        reactor.wait(listener.as_raw_fd());
    }
}
