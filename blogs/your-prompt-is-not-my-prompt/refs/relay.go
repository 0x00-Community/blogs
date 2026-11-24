// Command relay is a WebSocket relay: clients are paired two by two, and every
// message from one side is forwarded unchanged to the other, in both
// directions at once. When either side goes away, the other is closed too,
// and every goroutine belonging to the pair exits.
package main

import (
	"flag"
	"log"
	"net/http"
	"sync"
	"time"

	"github.com/gorilla/websocket"
)

const (
	writeWait  = 10 * time.Second  // max time for a single write to a peer
	pongWait   = 60 * time.Second  // peer must answer a ping within this
	pingPeriod = pongWait * 9 / 10 // how often we ping
	maxMsgSize = 1 << 20           // per-message read limit
	closeGrace = 1 * time.Second   // time to send a close frame on teardown
)

// message is one WebSocket data frame, kept with its type (text/binary) so it
// can be forwarded verbatim.
type message struct {
	typ  int
	data []byte
}

// client owns one WebSocket connection and the two goroutines bound to it:
//   - readLoop: the only reader of conn; pushes frames into in.
//   - pingLoop: keepalive; detects half-open TCP connections.
//
// Writes to conn are done by exactly one goroutine at a time: the pump that
// forwards the peer's messages (plus WriteControl, which gorilla allows
// concurrently with everything else).
type client struct {
	conn *websocket.Conn
	in   chan message  // unbuffered: backpressure flows to the TCP socket
	dead chan struct{} // closed when readLoop exits (peer gone or we closed)
	quit chan struct{} // closed by shutdown(); tells our goroutines to stop
	once sync.Once
	wg   sync.WaitGroup // readLoop + pingLoop

	// Pairing state, guarded by hub.mu.
	paired      bool
	sessionDone chan struct{} // closed after relay() returns, if paired
}

func newClient(conn *websocket.Conn) *client {
	c := &client{
		conn: conn,
		in:   make(chan message),
		dead: make(chan struct{}),
		quit: make(chan struct{}),

		sessionDone: make(chan struct{}),
	}
	c.wg.Add(2)
	go c.readLoop()
	go c.pingLoop()
	return c
}

func (c *client) readLoop() {
	defer c.wg.Done()
	defer close(c.dead)
	c.conn.SetReadLimit(maxMsgSize)
	c.conn.SetReadDeadline(time.Now().Add(pongWait))
	c.conn.SetPongHandler(func(string) error {
		return c.conn.SetReadDeadline(time.Now().Add(pongWait))
	})
	for {
		typ, data, err := c.conn.ReadMessage()
		if err != nil {
			return
		}
		select {
		case c.in <- message{typ, data}:
		case <-c.quit:
			return
		}
	}
}

func (c *client) pingLoop() {
	defer c.wg.Done()
	t := time.NewTicker(pingPeriod)
	defer t.Stop()
	for {
		select {
		case <-t.C:
			if err := c.conn.WriteControl(websocket.PingMessage, nil, time.Now().Add(writeWait)); err != nil {
				return
			}
		case <-c.quit:
			return
		}
	}
}

// shutdown is idempotent: it sends a best-effort close frame, closes the TCP
// connection (which unblocks any pending ReadMessage/WriteMessage), and
// signals our goroutines to exit.
func (c *client) shutdown() {
	c.once.Do(func() {
		close(c.quit)
		_ = c.conn.WriteControl(websocket.CloseMessage,
			websocket.FormatCloseMessage(websocket.CloseGoingAway, "peer disconnected"),
			time.Now().Add(closeGrace))
		_ = c.conn.Close()
	})
}

// shutdownAndWait closes the client and blocks until all its goroutines exit.
func (c *client) shutdownAndWait() {
	c.shutdown()
	c.wg.Wait()
}

// pump forwards src -> dst until either side fails.
func (src *client) pump(dst *client) {
	for {
		select {
		case m := <-src.in:
			dst.conn.SetWriteDeadline(time.Now().Add(writeWait))
			if err := dst.conn.WriteMessage(m.typ, m.data); err != nil {
				return
			}
		case <-src.dead: // src disconnected
			return
		case <-dst.dead: // dst disconnected (its reader saw the error)
			return
		}
	}
}

// relay runs a paired session and returns only after all four per-client
// goroutines and both pumps are gone.
func relay(a, b *client) {
	var wg sync.WaitGroup
	wg.Add(2)
	for _, p := range [][2]*client{{a, b}, {b, a}} {
		go func(src, dst *client) {
			defer wg.Done()
			src.pump(dst)
			// Whichever direction stops first tears down both sides;
			// that unblocks the other pump via the dead channels.
			a.shutdown()
			b.shutdown()
		}(p[0], p[1])
	}
	wg.Wait()
	a.shutdownAndWait()
	b.shutdownAndWait()
}

// hub pairs clients in arrival order. At most one client waits at a time.
type hub struct {
	mu      sync.Mutex
	waiting *client
}

func (h *hub) serve(c *client) {
	h.mu.Lock()
	peer := h.waiting
	h.waiting = nil
	if peer != nil && isClosed(peer.dead) {
		// The waiter vanished but its handler has not cleaned up yet.
		// Drop it (its own handler will shut it down) and wait ourselves.
		peer = nil
	}
	if peer != nil {
		peer.paired = true
		h.mu.Unlock()

		log.Printf("paired %s <-> %s", peer.conn.RemoteAddr(), c.conn.RemoteAddr())
		relay(peer, c)
		close(peer.sessionDone) // release the waiting client's handler
		log.Printf("closed %s <-> %s", peer.conn.RemoteAddr(), c.conn.RemoteAddr())
		return
	}
	h.waiting = c
	h.mu.Unlock()

	select {
	case <-c.sessionDone: // our session ran in the partner's handler and is over
	case <-c.dead: // we went away while waiting
		h.mu.Lock()
		if h.waiting == c {
			h.waiting = nil
		}
		paired := c.paired
		h.mu.Unlock()
		if paired {
			// Lost the race: a partner took us just now; relay sees c.dead,
			// tears down both sides, then closes sessionDone.
			<-c.sessionDone
		} else {
			c.shutdownAndWait()
		}
	}
}

func isClosed(ch <-chan struct{}) bool {
	select {
	case <-ch:
		return true
	default:
		return false
	}
}

var upgrader = websocket.Upgrader{
	ReadBufferSize:  4096,
	WriteBufferSize: 4096,
	CheckOrigin:     func(*http.Request) bool { return true },
}

func main() {
	addr := flag.String("addr", ":8080", "listen address")
	flag.Parse()

	h := &hub{}
	http.HandleFunc("/ws", func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			return // Upgrade already replied with an HTTP error
		}
		h.serve(newClient(conn))
	})
	log.Printf("listening on %s", *addr)
	log.Fatal(http.ListenAndServe(*addr, nil))
}
