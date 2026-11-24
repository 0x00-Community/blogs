package main

import (
	"io"
	"net/http"

	"github.com/gorilla/websocket"
)

var upgrader websocket.Upgrader

// lobby: the first connection waits here until a second one takes it.
var lobby = make(chan *websocket.Conn)

// flow: one direction. Streams each message from src to dst with io.Copy.
func flow(dst, src *websocket.Conn, done chan<- struct{}) {
	defer func() { done <- struct{}{} }()
	for {
		mt, r, err := src.NextReader()
		if err != nil {
			return
		}
		w, err := dst.NextWriter(mt)
		if err != nil {
			return
		}
		if _, err := io.Copy(w, r); err != nil {
			return
		}
		if err := w.Close(); err != nil {
			return
		}
	}
}

// relay: the pipe. Returns as soon as either direction ends,
// and closes both connections so the other flow unblocks.
func relay(a, b *websocket.Conn) {
	defer a.Close()
	defer b.Close()
	done := make(chan struct{}, 2) // buffered: the second flow must be able to exit
	go flow(b, a, done)
	go flow(a, b, done)
	<-done
}

func handle(w http.ResponseWriter, r *http.Request) {
	c, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		return
	}
	select {
	case peer := <-lobby:
		relay(peer, c)
	case lobby <- c:
	}
}

func main() {
	http.HandleFunc("/", handle)
	http.ListenAndServe(":8080", nil)
}
