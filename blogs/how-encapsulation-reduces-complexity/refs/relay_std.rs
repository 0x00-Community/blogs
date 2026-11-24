//! WebSocket 1:1 中转服务器，只用标准库。
//!
//! 线程模型：
//!   main 线程         : accept 循环，每个新连接 spawn 一个握手线程
//!   握手线程(每连接)  : 完成 HTTP Upgrade 后把 TcpStream 通过 mpsc 交给 Lobby
//!   Lobby 线程(1 个)  : 配对。持有最多一个等待者；定期探活，死掉的等待者直接丢弃
//!   Pair 线程(每对)   : thread::scope 里开两个 pump 线程(A->B, B->A)，scope 结束 = 两个方向都已退出
//!
//! 断开：任一 pump 退出时 shutdown(Both) 两个 socket，另一个 pump 阻塞中的 read 立即返回 EOF/错误而退出；
//! scope 保证两个线程都 join 后才 drop 两个 TcpStream，不留悬挂连接或线程。

use std::io::{self, BufReader, Read, Write};
use std::net::{Shutdown, TcpListener, TcpStream};
use std::sync::mpsc::{self, Receiver, RecvTimeoutError};
use std::sync::Mutex;
use std::thread;
use std::time::Duration;

const MAX_PAYLOAD: u64 = 16 << 20;
const MAX_HEADER: usize = 8 << 10;
const WS_GUID: &str = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11";
const PROBE_INTERVAL: Duration = Duration::from_secs(1);
const HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(10);
const WRITE_TIMEOUT: Duration = Duration::from_secs(30);

// ---------------------------------------------------------------- SHA-1 / Base64

struct Sha1 {
    h: [u32; 5],
}

impl Sha1 {
    fn digest(data: &[u8]) -> [u8; 20] {
        let mut s = Sha1 { h: [0x67452301, 0xEFCDAB89, 0x98BADCFE, 0x10325476, 0xC3D2E1F0] };
        let mut msg = data.to_vec();
        let bits = (data.len() as u64).wrapping_mul(8);
        msg.push(0x80);
        while msg.len() % 64 != 56 {
            msg.push(0);
        }
        msg.extend_from_slice(&bits.to_be_bytes());
        msg.chunks_exact(64).for_each(|c| s.block(c));
        let mut out = [0u8; 20];
        out.chunks_exact_mut(4).zip(s.h).for_each(|(o, h)| o.copy_from_slice(&h.to_be_bytes()));
        out
    }

    fn block(&mut self, chunk: &[u8]) {
        let mut w = [0u32; 80];
        for (i, c) in chunk.chunks_exact(4).enumerate() {
            w[i] = u32::from_be_bytes([c[0], c[1], c[2], c[3]]);
        }
        for i in 16..80 {
            w[i] = (w[i - 3] ^ w[i - 8] ^ w[i - 14] ^ w[i - 16]).rotate_left(1);
        }
        let [mut a, mut b, mut c, mut d, mut e] = self.h;
        for (i, wi) in w.iter().enumerate() {
            let (f, k) = match i {
                0..=19 => ((b & c) | (!b & d), 0x5A827999),
                20..=39 => (b ^ c ^ d, 0x6ED9EBA1),
                40..=59 => ((b & c) | (b & d) | (c & d), 0x8F1BBCDC),
                _ => (b ^ c ^ d, 0xCA62C1D6),
            };
            let t = a.rotate_left(5).wrapping_add(f).wrapping_add(e).wrapping_add(k).wrapping_add(*wi);
            (e, d, c, b, a) = (d, c, b.rotate_left(30), a, t);
        }
        for (h, v) in self.h.iter_mut().zip([a, b, c, d, e]) {
            *h = h.wrapping_add(v);
        }
    }
}

struct Base64<'a>(&'a [u8]);

impl From<Base64<'_>> for String {
    fn from(Base64(data): Base64<'_>) -> String {
        const T: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
        let mut out = String::with_capacity(data.len().div_ceil(3) * 4);
        for c in data.chunks(3) {
            let n = (u32::from(c[0]) << 16)
                | (u32::from(*c.get(1).unwrap_or(&0)) << 8)
                | u32::from(*c.get(2).unwrap_or(&0));
            for i in 0..4 {
                out.push(if i <= c.len() { T[(n >> (18 - 6 * i) & 63) as usize] as char } else { '=' });
            }
        }
        out
    }
}

// ---------------------------------------------------------------- Handshake

struct Handshake<'a>(&'a TcpStream);

impl Handshake<'_> {
    /// 逐字节读到 \r\n\r\n，避免把客户端后续的帧数据吞进缓冲区。
    fn accept(self) -> io::Result<()> {
        let mut sock = self.0;
        sock.set_read_timeout(Some(HANDSHAKE_TIMEOUT))?;
        let mut req = Vec::with_capacity(512);
        let mut byte = [0u8; 1];
        while !req.ends_with(b"\r\n\r\n") {
            if req.len() >= MAX_HEADER || sock.read(&mut byte)? == 0 {
                return Err(io::ErrorKind::InvalidData.into());
            }
            req.push(byte[0]);
        }
        sock.set_read_timeout(None)?;

        let req = std::str::from_utf8(&req).map_err(|_| io::Error::from(io::ErrorKind::InvalidData))?;
        let key = req
            .lines()
            .filter_map(|l| l.split_once(':'))
            .find(|(k, _)| k.trim().eq_ignore_ascii_case("sec-websocket-key"))
            .map(|(_, v)| v.trim())
            .ok_or(io::ErrorKind::InvalidData)?;

        let accept: String = Base64(&Sha1::digest(format!("{key}{WS_GUID}").as_bytes())).into();
        write!(
            sock,
            "HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Accept: {accept}\r\n\r\n"
        )
    }
}

// ---------------------------------------------------------------- Frame

#[derive(Clone, Copy, PartialEq, Eq)]
enum Opcode {
    Continuation = 0x0,
    Text = 0x1,
    Binary = 0x2,
    Close = 0x8,
    Ping = 0x9,
    Pong = 0xA,
}

impl TryFrom<u8> for Opcode {
    type Error = io::Error;
    fn try_from(v: u8) -> io::Result<Self> {
        Ok(match v {
            0x0 => Opcode::Continuation,
            0x1 => Opcode::Text,
            0x2 => Opcode::Binary,
            0x8 => Opcode::Close,
            0x9 => Opcode::Ping,
            0xA => Opcode::Pong,
            _ => return Err(io::ErrorKind::InvalidData.into()),
        })
    }
}

struct Frame {
    fin: bool,
    op: Opcode,
    payload: Vec<u8>,
}

impl Frame {
    /// 1001 Going Away：对端异常消失时发给存活方。
    fn going_away() -> Self {
        Frame { fin: true, op: Opcode::Close, payload: 1001u16.to_be_bytes().to_vec() }
    }

    fn read(r: &mut impl Read) -> io::Result<Self> {
        let mut h = [0u8; 2];
        r.read_exact(&mut h)?;
        let (fin, op, masked) = (h[0] & 0x80 != 0, Opcode::try_from(h[0] & 0x0F)?, h[1] & 0x80 != 0);
        let len = match h[1] & 0x7F {
            126 => {
                let mut b = [0u8; 2];
                r.read_exact(&mut b)?;
                u64::from(u16::from_be_bytes(b))
            }
            127 => {
                let mut b = [0u8; 8];
                r.read_exact(&mut b)?;
                u64::from_be_bytes(b)
            }
            n => u64::from(n),
        };
        // RFC 6455：客户端帧必须带掩码
        if !masked || len > MAX_PAYLOAD {
            return Err(io::ErrorKind::InvalidData.into());
        }
        let mut mask = [0u8; 4];
        r.read_exact(&mut mask)?;
        let mut payload = vec![0u8; usize::try_from(len).map_err(|_| io::Error::from(io::ErrorKind::InvalidData))?];
        r.read_exact(&mut payload)?;
        payload.iter_mut().zip(mask.iter().cycle()).for_each(|(b, m)| *b ^= m);
        Ok(Frame { fin, op, payload })
    }
}

impl From<&Frame> for Vec<u8> {
    /// 服务器 -> 客户端：不加掩码，头和负载拼成一块，单次 write_all。
    fn from(f: &Frame) -> Vec<u8> {
        let len = f.payload.len();
        let mut out = Vec::with_capacity(len + 10);
        out.push(u8::from(f.fin) << 7 | f.op as u8);
        match len {
            0..=125 => out.push(len as u8),
            126..=0xFFFF => {
                out.push(126);
                out.extend_from_slice(&(len as u16).to_be_bytes());
            }
            _ => {
                out.push(127);
                out.extend_from_slice(&(len as u64).to_be_bytes());
            }
        }
        out.extend_from_slice(&f.payload);
        out
    }
}

// ---------------------------------------------------------------- Peer / Pair

/// 一个已升级的连接。读只发生在自己的 pump 线程；
/// 写可能来自两个线程（对端 pump 转发数据 + 自己 pump 回 pong/close），用 wlock 保证帧不交错。
struct Peer {
    sock: TcpStream,
    wlock: Mutex<()>,
}

impl From<TcpStream> for Peer {
    fn from(sock: TcpStream) -> Self {
        Peer { sock, wlock: Mutex::new(()) }
    }
}

impl Peer {
    fn send(&self, f: &Frame) -> io::Result<()> {
        let buf: Vec<u8> = f.into();
        let _g = self.wlock.lock().unwrap_or_else(|e| e.into_inner());
        (&self.sock).write_all(&buf)
    }

    /// 不需要 wlock：shutdown 会让另一个线程阻塞中的 read/write 立刻失败返回。
    fn kill(&self) {
        let _ = self.sock.shutdown(Shutdown::Both);
    }

    /// 单向泵：读 self，写 to。返回时保证两端都已 shutdown。
    fn pump(&self, to: &Peer) {
        let mut r = BufReader::new(&self.sock);
        let farewell = loop {
            let Ok(f) = Frame::read(&mut r) else { break Frame::going_away() };
            let ok = match f.op {
                Opcode::Ping => self.send(&Frame { fin: true, op: Opcode::Pong, payload: f.payload }),
                Opcode::Pong => Ok(()),
                Opcode::Close => {
                    let _ = self.send(&f); // 回 close 完成本端关闭握手
                    break f; // 原样把 close（含状态码/原因）转给对端
                }
                Opcode::Text | Opcode::Binary | Opcode::Continuation => to.send(&f),
            };
            if ok.is_err() {
                break Frame::going_away();
            }
        };
        let _ = to.send(&farewell);
        self.kill();
        to.kill();
    }
}

struct Pair {
    a: Peer,
    b: Peer,
}

impl Pair {
    /// scope 返回 = 两个 pump 都已 join；随后 self drop，两个 fd 关闭。
    fn run(self) {
        thread::scope(|s| {
            s.spawn(|| self.a.pump(&self.b));
            s.spawn(|| self.b.pump(&self.a));
        });
    }
}

// ---------------------------------------------------------------- Lobby

struct Lobby {
    rx: Receiver<TcpStream>,
    waiting: Option<TcpStream>,
}

trait Alive {
    fn alive(&self) -> bool;
}

impl Alive for TcpStream {
    /// 非阻塞 peek：Ok(0) = 对端已 FIN；WouldBlock / Ok(n>0) = 仍在线。
    fn alive(&self) -> bool {
        if self.set_nonblocking(true).is_err() {
            return false;
        }
        let r = self.peek(&mut [0u8; 1]);
        let _ = self.set_nonblocking(false);
        match r {
            Ok(n) => n > 0,
            Err(e) => e.kind() == io::ErrorKind::WouldBlock,
        }
    }
}

impl Lobby {
    fn run(mut self) {
        loop {
            match self.rx.recv_timeout(PROBE_INTERVAL) {
                Ok(s) => match self.waiting.take().filter(TcpStream::alive) {
                    Some(w) => {
                        thread::spawn(move || Pair { a: w.into(), b: s.into() }.run());
                    }
                    None => self.waiting = Some(s),
                },
                // 等待者在配对前断开：定期探活，死了就 drop（关闭 fd）
                Err(RecvTimeoutError::Timeout) => {
                    self.waiting = self.waiting.take().filter(TcpStream::alive);
                }
                Err(RecvTimeoutError::Disconnected) => return,
            }
        }
    }
}

// ---------------------------------------------------------------- main

fn main() -> io::Result<()> {
    let addr = std::env::args().nth(1).unwrap_or_else(|| "0.0.0.0:9001".into());
    let listener = TcpListener::bind(&addr)?;
    eprintln!("ws-relay listening on {addr}");

    let (tx, rx) = mpsc::channel();
    thread::spawn(move || Lobby { rx, waiting: None }.run());

    for sock in listener.incoming().flatten() {
        let tx = tx.clone();
        thread::spawn(move || {
            let _ = sock.set_nodelay(true);
            let _ = sock.set_write_timeout(Some(WRITE_TIMEOUT));
            if Handshake(&sock).accept().is_ok() {
                let _ = tx.send(sock);
            }
        });
    }
    Ok(())
}
