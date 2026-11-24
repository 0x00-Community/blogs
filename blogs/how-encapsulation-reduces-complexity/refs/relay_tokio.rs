use futures_util::StreamExt;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::mpsc;
use tokio_tungstenite::{accept_async, WebSocketStream};

type Ws = WebSocketStream<TcpStream>;

async fn relay(a: Ws, b: Ws) {
    let (a_tx, a_rx) = a.split();
    let (b_tx, b_rx) = b.split();
    tokio::select! {
        _ = a_rx.forward(b_tx) => {}
        _ = b_rx.forward(a_tx) => {}
    }
}

#[tokio::main]
async fn main() -> std::io::Result<()> {
    let listener = TcpListener::bind("0.0.0.0:8080").await?;
    let (tx, mut rx) = mpsc::channel::<Ws>(16);

    tokio::spawn(async move {
        while let (Some(a), Some(b)) = (rx.recv().await, rx.recv().await) {
            tokio::spawn(relay(a, b));
        }
    });

    loop {
        let (stream, _) = listener.accept().await?;
        let tx = tx.clone();
        tokio::spawn(async move {
            if let Ok(ws) = accept_async(stream).await {
                let _ = tx.send(ws).await;
            }
        });
    }
}
