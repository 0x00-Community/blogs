// Cargo.toml:
//   [dependencies]
//   tokio = { version = "1", features = ["rt-multi-thread", "macros", "net", "sync", "time"] }
//   tokio-tungstenite = "0.28"
//   futures-util = { version = "0.3", default-features = false, features = ["sink", "std"] }

//! WebSocket 配对中转服务器。
//!
//! 任务结构：
//!   accept 循环 ──spawn──> 握手任务(每连接一个) ──mpsc──> Matchmaker 任务(唯一)
//!   Matchmaker 凑齐两个 ──spawn──> Session 任务(每对一个，内部 select! 两个方向)

use std::time::Duration;

use futures_util::{SinkExt, StreamExt, stream::SplitSink, stream::SplitStream};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::mpsc;
use tokio_tungstenite::{WebSocketStream, accept_async, tungstenite::Message};

type Ws = WebSocketStream<TcpStream>;

const LISTEN_ADDR: &str = "0.0.0.0:9000";
const HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(10);
const CLOSE_TIMEOUT: Duration = Duration::from_secs(3);
/// 等待配对期间最多缓存的消息数，超出视为滥用直接断开。
const MAX_PENDING: usize = 1024;

/// 一个已完成握手的客户端，以及它在等待配对期间发来的消息。
struct Peer {
    ws: Ws,
    pending: Vec<Message>,
}

/// 等待中的客户端在 select! 里可能发生的事件。
enum Event {
    Arrived(Option<Ws>),
    Waiter(Option<Result<Message, tokio_tungstenite::tungstenite::Error>>),
}

impl From<Ws> for Peer {
    fn from(ws: Ws) -> Self {
        Self { ws, pending: Vec::new() }
    }
}

impl Peer {
    /// 处理等待者发来的一条帧。返回 false 表示它已断开 / 出错 / 超限，应丢弃。
    fn absorb(&mut self, frame: Option<Result<Message, tokio_tungstenite::tungstenite::Error>>) -> bool {
        match frame {
            Some(Ok(msg @ (Message::Text(_) | Message::Binary(_)))) => {
                self.pending.push(msg);
                self.pending.len() <= MAX_PENDING
            }
            // Ping/Pong 由 tungstenite 自动应答，无需处理
            Some(Ok(Message::Ping(_) | Message::Pong(_) | Message::Frame(_))) => true,
            Some(Ok(Message::Close(_))) | Some(Err(_)) | None => false,
        }
    }
}

/// 唯一的配对者：独占“等待槽”，因此不需要任何锁。
struct Matchmaker {
    rx: mpsc::Receiver<Ws>,
    waiting: Option<Peer>,
    next_id: u64,
}

impl Matchmaker {
    async fn run(mut self) {
        loop {
            let Some(mut waiter) = self.waiting.take() else {
                // 槽空：只等新连接
                match self.rx.recv().await {
                    Some(ws) => self.waiting = Some(ws.into()),
                    None => return,
                }
                continue;
            };
            // 槽满：同时等“新连接到来”和“等待者自己的动静”(断开要及时发现)
            let event = tokio::select! {
                ws = self.rx.recv() => Event::Arrived(ws),
                frame = waiter.ws.next() => Event::Waiter(frame),
            };
            match event {
                Event::Arrived(Some(ws)) => {
                    let id = self.next_id;
                    self.next_id += 1;
                    tokio::spawn(Session { id, a: waiter, b: ws.into() }.run());
                }
                Event::Arrived(None) => return,
                Event::Waiter(frame) => {
                    if waiter.absorb(frame) {
                        self.waiting = Some(waiter);
                    } else {
                        eprintln!("waiting client left before pairing");
                        // waiter 在此被 drop，TCP 关闭
                    }
                }
            }
        }
    }
}

/// 单向转发结束的原因。
#[derive(Debug)]
enum End {
    CloseFrame,
    Eof,
    ReadError,
    WriteError,
}

/// 一对已配对的客户端。
struct Session {
    id: u64,
    a: Peer,
    b: Peer,
}

impl Session {
    async fn run(self) {
        let Self { id, a, b } = self;
        eprintln!("session {id}: paired");
        let (mut a_tx, a_rx) = a.ws.split();
        let (mut b_tx, b_rx) = b.ws.split();

        // 两个方向在同一个任务里并发推进；任一方向结束，select! 立刻返回，
        // 另一个方向的 future 被 drop（取消），不会遗留任何协程。
        let reason = tokio::select! {
            r = Self::pump(a.pending, a_rx, &mut b_tx) => format!("A->B ended: {r:?}"),
            r = Self::pump(b.pending, b_rx, &mut a_tx) => format!("B->A ended: {r:?}"),
        };
        eprintln!("session {id}: {reason}");

        // 给双方都发 Close 帧并尽力完成关闭握手；对端不配合也有超时兜底。
        let _ = tokio::time::timeout(CLOSE_TIMEOUT, async {
            let _ = tokio::join!(a_tx.close(), b_tx.close());
        })
        .await;
        // a_tx/b_tx 在此 drop，两条 TCP 连接随之释放
        eprintln!("session {id}: closed");
    }

    /// 单向泵：先补发等待期缓存，再逐条原样转发。返回结束原因。
    async fn pump(
        pending: Vec<Message>,
        mut from: SplitStream<Ws>,
        to: &mut SplitSink<Ws, Message>,
    ) -> End {
        for msg in pending {
            if to.send(msg).await.is_err() {
                return End::WriteError;
            }
        }
        while let Some(frame) = from.next().await {
            match frame {
                Ok(msg @ (Message::Text(_) | Message::Binary(_))) => {
                    // send 会等待写出，天然形成背压：慢的一方会拖慢快的一方的读取
                    if to.send(msg).await.is_err() {
                        return End::WriteError;
                    }
                }
                Ok(Message::Close(_)) => return End::CloseFrame,
                Ok(_) => {} // Ping/Pong 属于逐跳控制帧，由库自动处理，不转发
                Err(_) => return End::ReadError,
            }
        }
        End::Eof
    }
}

#[tokio::main]
async fn main() -> std::io::Result<()> {
    let listener = TcpListener::bind(LISTEN_ADDR).await?;
    eprintln!("listening on {LISTEN_ADDR}");

    let (tx, rx) = mpsc::channel::<Ws>(64);
    tokio::spawn(Matchmaker { rx, waiting: None, next_id: 0 }.run());

    loop {
        let (tcp, addr) = listener.accept().await?;
        let tx = tx.clone();
        // 握手放到独立任务，慢客户端不会阻塞 accept；超时则直接丢弃
        tokio::spawn(async move {
            match tokio::time::timeout(HANDSHAKE_TIMEOUT, accept_async(tcp)).await {
                Ok(Ok(ws)) => {
                    let _ = tx.send(ws).await;
                }
                Ok(Err(e)) => eprintln!("{addr}: handshake failed: {e}"),
                Err(_) => eprintln!("{addr}: handshake timed out"),
            }
        });
    }
}
