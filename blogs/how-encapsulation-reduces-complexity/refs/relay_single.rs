//! 单线程 WebSocket 配对中转服务器：只用 std + poll(2)，没有 tokio、没有线程。
//!
//! 并发单元：只有一个——主线程里的 poll 事件循环。每个 TCP 连接是一个
//! `Conn` 状态机，存放在 slab（`Vec<Option<Conn>>`）里，用下标当 token。

use std::io::{self, ErrorKind, Read, Write};
use std::net::{Shutdown, TcpListener, TcpStream};
use std::os::fd::AsRawFd;
use std::time::{Duration, Instant};

const GUID: &str = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11";
const MAX_HEADER: usize = 8 << 10;
const MAX_FRAME: u64 = 16 << 20;
/// 发往对端的待写字节超过这个量就停止读本端（背压）。
const HIGH_WATER: usize = 1 << 20;
const HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(10);
/// 发出 Close 帧后最多等这么久把缓冲刷完，然后强制关闭。
const LINGER: Duration = Duration::from_secs(5);
const GOING_AWAY: [u8; 2] = 1001u16.to_be_bytes();
const PROTOCOL_ERROR: [u8; 2] = 1002u16.to_be_bytes();
const BAD_REQUEST: &[u8] = b"HTTP/1.1 400 Bad Request\r\nConnection: close\r\nContent-Length: 0\r\n\r\n";

mod sys {
    #[repr(C)]
    pub struct PollFd {
        pub fd: i32,
        pub events: i16,
        pub revents: i16,
    }
    pub const POLLIN: i16 = 0x001;
    pub const POLLOUT: i16 = 0x004;
    pub const POLLERR: i16 = 0x008;
    pub const POLLHUP: i16 = 0x010;
    pub const POLLNVAL: i16 = 0x020;
    extern "C" {
        pub fn poll(fds: *mut PollFd, nfds: std::ffi::c_ulong, timeout: i32) -> i32;
    }
}
use sys::{PollFd, POLLERR, POLLHUP, POLLIN, POLLNVAL, POLLOUT};

// ---------------------------------------------------------------- SHA-1 / Base64 / Accept

struct Sha1([u8; 20]);

impl From<&[u8]> for Sha1 {
    fn from(msg: &[u8]) -> Self {
        let mut h: [u32; 5] = [0x67452301, 0xEFCDAB89, 0x98BADCFE, 0x10325476, 0xC3D2E1F0];
        let bits = u64::try_from(msg.len()).unwrap_or(u64::MAX).wrapping_mul(8);
        let mut data = msg.to_vec();
        data.push(0x80);
        while data.len() % 64 != 56 {
            data.push(0);
        }
        data.extend_from_slice(&bits.to_be_bytes());
        for block in data.chunks_exact(64) {
            let mut w = [0u32; 80];
            for (i, word) in block.chunks_exact(4).enumerate() {
                w[i] = u32::from_be_bytes([word[0], word[1], word[2], word[3]]);
            }
            for i in 16..80 {
                w[i] = (w[i - 3] ^ w[i - 8] ^ w[i - 14] ^ w[i - 16]).rotate_left(1);
            }
            let [mut a, mut b, mut c, mut d, mut e] = h;
            for (i, &wi) in w.iter().enumerate() {
                let (f, k) = match i {
                    0..=19 => ((b & c) | (!b & d), 0x5A827999),
                    20..=39 => (b ^ c ^ d, 0x6ED9EBA1),
                    40..=59 => ((b & c) | (b & d) | (c & d), 0x8F1BBCDC),
                    _ => (b ^ c ^ d, 0xCA62C1D6),
                };
                let t = a.rotate_left(5).wrapping_add(f).wrapping_add(e).wrapping_add(k).wrapping_add(wi);
                (e, d, c, b, a) = (d, c, b.rotate_left(30), a, t);
            }
            for (hi, v) in h.iter_mut().zip([a, b, c, d, e]) {
                *hi = hi.wrapping_add(v);
            }
        }
        let mut out = [0u8; 20];
        for (dst, v) in out.chunks_exact_mut(4).zip(h) {
            dst.copy_from_slice(&v.to_be_bytes());
        }
        Sha1(out)
    }
}

struct Base64(String);

impl From<&[u8]> for Base64 {
    fn from(bytes: &[u8]) -> Self {
        const ALPHABET: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
        let mut s = String::with_capacity(bytes.len().div_ceil(3) * 4);
        for chunk in bytes.chunks(3) {
            let n = chunk.iter().enumerate().fold(0u32, |n, (i, &b)| n | u32::from(b) << (16 - 8 * i));
            for i in 0..4 {
                s.push(match i <= chunk.len() {
                    true => char::from(ALPHABET[(n >> (18 - 6 * i) & 63) as usize]),
                    false => '=',
                });
            }
        }
        Base64(s)
    }
}

/// Sec-WebSocket-Accept = base64(sha1(key + GUID))
struct AcceptKey(String);

impl From<&str> for AcceptKey {
    fn from(key: &str) -> Self {
        let digest = Sha1::from(format!("{key}{GUID}").as_bytes());
        AcceptKey(Base64::from(&digest.0[..]).0)
    }
}

// ---------------------------------------------------------------- HTTP Upgrade 握手

#[derive(Debug)]
struct BadRequest;

enum Header {
    Upgrade,
    Key,
    Other,
}

impl From<&str> for Header {
    fn from(name: &str) -> Self {
        match name {
            n if n.eq_ignore_ascii_case("upgrade") => Header::Upgrade,
            n if n.eq_ignore_ascii_case("sec-websocket-key") => Header::Key,
            _ => Header::Other,
        }
    }
}

struct Upgrade {
    accept: AcceptKey,
    /// 请求头占用的字节数（之后的字节可能已经是第一帧）。
    len: usize,
}

impl Upgrade {
    fn parse(buf: &[u8]) -> Result<Option<Self>, BadRequest> {
        let Some(end) = buf.windows(4).position(|w| w == b"\r\n\r\n") else {
            return match buf.len() > MAX_HEADER {
                true => Err(BadRequest),
                false => Ok(None),
            };
        };
        let head = std::str::from_utf8(&buf[..end]).map_err(|_| BadRequest)?;
        let mut lines = head.split("\r\n");
        lines.next().filter(|l| l.starts_with("GET ")).ok_or(BadRequest)?;
        let (mut key, mut upgrade) = (None, false);
        for line in lines {
            let (name, value) = line.split_once(':').ok_or(BadRequest)?;
            let value = value.trim();
            match Header::from(name.trim()) {
                Header::Upgrade => upgrade = value.eq_ignore_ascii_case("websocket"),
                Header::Key => key = Some(value),
                Header::Other => {}
            }
        }
        match (upgrade, key) {
            (true, Some(k)) => Ok(Some(Upgrade { accept: k.into(), len: end + 4 })),
            _ => Err(BadRequest),
        }
    }

    fn write_response(&self, out: &mut Vec<u8>) {
        out.extend_from_slice(b"HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Accept: ");
        out.extend_from_slice(self.accept.0.as_bytes());
        out.extend_from_slice(b"\r\n\r\n");
    }
}

// ---------------------------------------------------------------- 帧编解码

#[derive(Clone, Copy, PartialEq, Eq)]
enum Opcode {
    Continuation = 0x0,
    Text = 0x1,
    Binary = 0x2,
    Close = 0x8,
    Ping = 0x9,
    Pong = 0xA,
}

#[derive(Debug)]
struct ProtocolError;

impl TryFrom<u8> for Opcode {
    type Error = ProtocolError;
    fn try_from(v: u8) -> Result<Self, ProtocolError> {
        Ok(match v {
            0x0 => Opcode::Continuation,
            0x1 => Opcode::Text,
            0x2 => Opcode::Binary,
            0x8 => Opcode::Close,
            0x9 => Opcode::Ping,
            0xA => Opcode::Pong,
            _ => return Err(ProtocolError),
        })
    }
}

impl From<Opcode> for u8 {
    fn from(op: Opcode) -> u8 {
        op as u8
    }
}

impl Opcode {
    fn is_control(self) -> bool {
        u8::from(self) & 0x8 != 0
    }
}

/// 一帧；payload 借用自读缓冲（原地去掩码，零拷贝）。
struct Frame<'a> {
    fin: bool,
    op: Opcode,
    payload: &'a [u8],
}

impl<'a> Frame<'a> {
    /// 从客户端字节流解析一帧。`Ok(None)` = 数据不够；返回值里的 usize 是消耗的字节数。
    fn parse(buf: &'a mut [u8]) -> Result<Option<(Self, usize)>, ProtocolError> {
        let [b0, b1, ..] = *buf else { return Ok(None) };
        let (fin, op) = (b0 & 0x80 != 0, Opcode::try_from(b0 & 0x0F)?);
        // RSV 位必须为 0；客户端帧必须带掩码
        if b0 & 0x70 != 0 || b1 & 0x80 == 0 {
            return Err(ProtocolError);
        }
        let (len, off) = match b1 & 0x7F {
            126 if buf.len() >= 4 => (u64::from(u16::from_be_bytes([buf[2], buf[3]])), 4),
            127 if buf.len() >= 10 => (u64::from_be_bytes(buf[2..10].try_into().map_err(|_| ProtocolError)?), 10),
            126 | 127 => return Ok(None),
            n => (u64::from(n), 2),
        };
        if len > MAX_FRAME || (op.is_control() && (len > 125 || !fin)) {
            return Err(ProtocolError);
        }
        let len = usize::try_from(len).map_err(|_| ProtocolError)?;
        let total = off + 4 + len;
        if buf.len() < total {
            return Ok(None);
        }
        let mask = [buf[off], buf[off + 1], buf[off + 2], buf[off + 3]];
        let payload = &mut buf[off + 4..total];
        payload.iter_mut().zip(mask.iter().cycle()).for_each(|(b, m)| *b ^= m);
        Ok(Some((Frame { fin, op, payload }, total)))
    }

    /// 编码成服务端→客户端帧（不带掩码）。
    fn encode_into(&self, out: &mut Vec<u8>) {
        out.push(u8::from(self.fin) << 7 | u8::from(self.op));
        match self.payload.len() {
            n @ 0..=125 => out.push(n as u8),
            n @ 126..=0xFFFF => {
                out.push(126);
                out.extend_from_slice(&(n as u16).to_be_bytes());
            }
            n => {
                out.push(127);
                out.extend_from_slice(&(n as u64).to_be_bytes());
            }
        }
        out.extend_from_slice(self.payload);
    }
}

// ---------------------------------------------------------------- 连接状态机

#[derive(Clone, Copy)]
enum Phase {
    /// 等 HTTP Upgrade 请求。
    Handshake { deadline: Instant },
    /// 已升级，正常转发。
    Open,
    /// 已排队 Close 帧，刷完写缓冲（或超时）后释放。
    Closing { deadline: Instant },
}

struct Conn {
    stream: TcpStream,
    phase: Phase,
    rbuf: Vec<u8>,
    wbuf: Vec<u8>,
    /// 尚未配对时收到的数据帧（已编码好），配对时整体移交给对端的 wbuf。
    held: Vec<u8>,
    peer: Option<usize>,
}

impl From<TcpStream> for Conn {
    fn from(stream: TcpStream) -> Self {
        Conn {
            stream,
            phase: Phase::Handshake { deadline: Instant::now() + HANDSHAKE_TIMEOUT },
            rbuf: Vec::new(),
            wbuf: Vec::new(),
            held: Vec::new(),
            peer: None,
        }
    }
}

impl Conn {
    fn deadline(&self) -> Option<Instant> {
        match self.phase {
            Phase::Handshake { deadline } | Phase::Closing { deadline } => Some(deadline),
            Phase::Open => None,
        }
    }
}

/// pump 停下的原因。
enum Stop {
    /// 帧读完了或背压满了。
    Idle,
    /// 需要关闭这一对，附带 Close 帧 payload。
    Close(Vec<u8>),
}

// ---------------------------------------------------------------- 中转服务器

struct Relay {
    listener: TcpListener,
    conns: Vec<Option<Conn>>,
    free: Vec<usize>,
    /// 已完成握手、正在等伙伴的连接（最多一个）。
    waiting: Option<usize>,
}

impl From<TcpListener> for Relay {
    fn from(listener: TcpListener) -> Self {
        Relay { listener, conns: Vec::new(), free: Vec::new(), waiting: None }
    }
}

impl Relay {
    fn conn(&mut self, t: usize) -> Option<&mut Conn> {
        self.conns.get_mut(t).and_then(Option::as_mut)
    }

    /// 本连接产生的转发数据要写到哪：已配对 → 对端 wbuf；未配对 → 自己的 held。
    fn out_len(&self, t: usize) -> usize {
        let Some(c) = self.conns[t].as_ref() else { return 0 };
        match (c.phase, c.peer) {
            (Phase::Open, Some(p)) => self.conns[p].as_ref().map_or(0, |p| p.wbuf.len()),
            (Phase::Open, None) => c.held.len(),
            _ => 0,
        }
    }

    fn outbox(&mut self, t: usize) -> &mut Vec<u8> {
        match self.conns[t].as_ref().and_then(|c| c.peer) {
            Some(p) => &mut self.conns[p].as_mut().expect("peer alive").wbuf,
            None => &mut self.conns[t].as_mut().expect("conn alive").held,
        }
    }

    fn run(&mut self) -> io::Result<()> {
        let mut fds: Vec<PollFd> = Vec::new();
        let mut tokens: Vec<usize> = Vec::new();
        loop {
            fds.clear();
            tokens.clear();
            fds.push(PollFd { fd: self.listener.as_raw_fd(), events: POLLIN, revents: 0 });
            for (t, c) in self.conns.iter().enumerate() {
                let Some(c) = c else { continue };
                let mut events = 0;
                if self.out_len(t) < HIGH_WATER {
                    events |= POLLIN;
                }
                if !c.wbuf.is_empty() {
                    events |= POLLOUT;
                }
                fds.push(PollFd { fd: c.stream.as_raw_fd(), events, revents: 0 });
                tokens.push(t);
            }
            let timeout = self.poll_timeout();
            // SAFETY: fds 是有效的 PollFd 数组，长度与 nfds 一致。
            let n = unsafe { sys::poll(fds.as_mut_ptr(), fds.len() as std::ffi::c_ulong, timeout) };
            if n < 0 {
                match io::Error::last_os_error() {
                    e if e.kind() == ErrorKind::Interrupted => continue,
                    e => return Err(e),
                }
            }
            for (fd, &t) in fds[1..].iter().zip(&tokens) {
                let r = fd.revents;
                if r == 0 || self.conn(t).is_none() {
                    continue; // 本轮已被对端连带关闭
                }
                if r & (POLLERR | POLLNVAL) != 0 || (r & POLLHUP != 0 && r & POLLIN == 0) {
                    self.kill(t);
                    continue;
                }
                if r & POLLIN != 0 {
                    self.on_readable(t);
                }
                if r & POLLOUT != 0 {
                    self.flush(t);
                }
            }
            // 最后再 accept：保证本轮被释放的槽位不会在遍历中途被复用
            if fds[0].revents & POLLIN != 0 {
                self.accept_all();
            }
            self.expire(Instant::now());
        }
    }

    fn poll_timeout(&self) -> i32 {
        let now = Instant::now();
        self.conns
            .iter()
            .flatten()
            .filter_map(Conn::deadline)
            .min()
            .map_or(-1, |d| i32::try_from(d.saturating_duration_since(now).as_millis() + 1).unwrap_or(i32::MAX))
    }

    fn accept_all(&mut self) {
        loop {
            match self.listener.accept() {
                Ok((stream, _)) => {
                    if stream.set_nonblocking(true).is_err() {
                        continue;
                    }
                    let _ = stream.set_nodelay(true);
                    let conn = Some(Conn::from(stream));
                    match self.free.pop() {
                        Some(t) => self.conns[t] = conn,
                        None => self.conns.push(conn),
                    }
                }
                Err(e) if e.kind() == ErrorKind::Interrupted => continue,
                Err(_) => return, // WouldBlock 或 EMFILE 等：下一轮再说
            }
        }
    }

    fn on_readable(&mut self, t: usize) {
        let mut chunk = [0u8; 16 << 10];
        let Some(c) = self.conn(t) else { return };
        match c.stream.read(&mut chunk) {
            Ok(0) => return self.kill(t),
            Ok(n) => c.rbuf.extend_from_slice(&chunk[..n]),
            Err(e) if matches!(e.kind(), ErrorKind::WouldBlock | ErrorKind::Interrupted) => return,
            Err(_) => return self.kill(t),
        }
        match c.phase {
            Phase::Handshake { .. } => self.handshake(t),
            Phase::Open => self.pump(t),
            Phase::Closing { .. } => c.rbuf.clear(), // 关闭中：读到的东西全丢弃
        }
    }

    fn handshake(&mut self, t: usize) {
        let Some(c) = self.conn(t) else { return };
        match Upgrade::parse(&c.rbuf) {
            Ok(None) => {}
            Ok(Some(up)) => {
                c.rbuf.drain(..up.len);
                up.write_response(&mut c.wbuf);
                c.phase = Phase::Open;
                self.pair(t);
            }
            Err(BadRequest) => {
                c.rbuf.clear();
                c.wbuf.extend_from_slice(BAD_REQUEST);
                c.phase = Phase::Closing { deadline: Instant::now() + LINGER };
            }
        }
    }

    fn pair(&mut self, t: usize) {
        match self.waiting.take() {
            Some(w) if w != t => {
                let Some(wc) = self.conn(w) else { return self.pair(t) };
                wc.peer = Some(t);
                let held = std::mem::take(&mut wc.held);
                let Some(tc) = self.conn(t) else { return };
                tc.peer = Some(w);
                tc.wbuf.extend_from_slice(&held);
                self.pump(w);
                self.pump(t);
            }
            _ => {
                self.waiting = Some(t);
                self.pump(t);
            }
        }
    }

    /// 解析 t 的读缓冲里所有完整帧并分发；受背压限制，没处理完的留在 rbuf。
    /// 只往缓冲里写，不直接 flush（由下一轮 poll 的 POLLOUT 驱动），避免递归。
    fn pump(&mut self, t: usize) {
        let Some(c) = self.conn(t) else { return };
        if !matches!(c.phase, Phase::Open) {
            return;
        }
        let mut rbuf = std::mem::take(&mut c.rbuf);
        let mut pos = 0;
        let stop = loop {
            if self.out_len(t) >= HIGH_WATER {
                break Stop::Idle;
            }
            match Frame::parse(&mut rbuf[pos..]) {
                Ok(None) => break Stop::Idle,
                Err(ProtocolError) => break Stop::Close(PROTOCOL_ERROR.to_vec()),
                Ok(Some((frame, used))) => {
                    pos += used;
                    match frame.op {
                        Opcode::Text | Opcode::Binary | Opcode::Continuation => frame.encode_into(self.outbox(t)),
                        Opcode::Ping => {
                            let pong = Frame { fin: true, op: Opcode::Pong, payload: frame.payload };
                            pong.encode_into(&mut self.conn(t).expect("conn alive").wbuf);
                        }
                        Opcode::Pong => {}
                        Opcode::Close => break Stop::Close(frame.payload.to_vec()),
                    }
                }
            }
        };
        rbuf.drain(..pos);
        if let Some(c) = self.conn(t) {
            c.rbuf = rbuf;
        }
        if let Stop::Close(payload) = stop {
            self.close_pair(t, &payload);
        }
    }

    fn flush(&mut self, t: usize) {
        let Some(c) = self.conn(t) else { return };
        let was_full = c.wbuf.len() >= HIGH_WATER;
        while !c.wbuf.is_empty() {
            match c.stream.write(&c.wbuf) {
                Ok(0) => return self.kill(t),
                Ok(n) => drop(c.wbuf.drain(..n)),
                Err(e) if e.kind() == ErrorKind::WouldBlock => break,
                Err(e) if e.kind() == ErrorKind::Interrupted => continue,
                Err(_) => return self.kill(t),
            }
        }
        match (c.phase, c.wbuf.is_empty(), c.peer) {
            (Phase::Closing { .. }, true, _) => self.release(t),
            // 背压解除：对端之前积压在 rbuf 里的帧可以继续处理了
            (Phase::Open, _, Some(p)) if was_full && c.wbuf.len() < HIGH_WATER => self.pump(p),
            _ => {}
        }
    }

    /// 优雅关闭：给 t 及其对端都排队一个 Close 帧，进入 Closing。
    fn close_pair(&mut self, t: usize, payload: &[u8]) {
        if let Some(p) = self.conn(t).and_then(|c| c.peer.take()) {
            self.begin_close(p, payload);
        }
        self.begin_close(t, payload);
    }

    fn begin_close(&mut self, t: usize, payload: &[u8]) {
        if self.waiting == Some(t) {
            self.waiting = None;
        }
        let Some(c) = self.conn(t) else { return };
        if matches!(c.phase, Phase::Closing { .. }) {
            return;
        }
        c.peer = None;
        c.rbuf.clear();
        c.held.clear();
        Frame { fin: true, op: Opcode::Close, payload }.encode_into(&mut c.wbuf);
        c.phase = Phase::Closing { deadline: Instant::now() + LINGER };
    }

    /// 硬断开：t 的 socket 已经坏了（EOF/错误/超时），立即释放，并让对端优雅关闭。
    fn kill(&mut self, t: usize) {
        let peer = self.conn(t).and_then(|c| c.peer.take());
        self.release(t);
        if let Some(p) = peer {
            if let Some(pc) = self.conn(p) {
                pc.peer = None;
            }
            self.begin_close(p, &GOING_AWAY);
        }
    }

    /// 释放槽位：drop TcpStream 即 close(fd)。
    fn release(&mut self, t: usize) {
        if self.waiting == Some(t) {
            self.waiting = None;
        }
        if let Some(c) = self.conns.get_mut(t).and_then(Option::take) {
            let _ = c.stream.shutdown(Shutdown::Both);
            self.free.push(t);
        }
    }

    fn expire(&mut self, now: Instant) {
        for t in 0..self.conns.len() {
            match self.conns[t].as_ref().map(|c| c.phase) {
                Some(Phase::Handshake { deadline }) if deadline <= now => self.kill(t),
                Some(Phase::Closing { deadline }) if deadline <= now => self.release(t),
                _ => {}
            }
        }
    }
}

fn main() -> io::Result<()> {
    let addr = std::env::args().nth(1).unwrap_or_else(|| "0.0.0.0:9000".into());
    let listener = TcpListener::bind(&addr)?;
    listener.set_nonblocking(true)?;
    eprintln!("ws relay listening on {addr}");
    Relay::from(listener).run()
}

#[cfg(test)]
mod tests {
    use super::*;

    /// RFC 6455 §1.3 的示例向量。
    #[test]
    fn accept_key_rfc6455() {
        assert_eq!(AcceptKey::from("dGhlIHNhbXBsZSBub25jZQ==").0, "s3pPLMBiTxaQ9kYGzzhZRbK+xOo=");
    }
}
