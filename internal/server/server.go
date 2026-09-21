package server

import (
	"log"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/gorilla/websocket"
)

const (
	writeWait      = 10 * time.Second
	pongWait       = 60 * time.Second
	pingPeriod     = (pongWait * 9) / 10
	maxMessageSize = 1024 * 1024
)

var upgrader = websocket.Upgrader{
	ReadBufferSize:  1024,
	WriteBufferSize: 1024,

	// Suitable for a local CLI application.
	// Use proper origin validation in production.
	CheckOrigin: func(r *http.Request) bool {
		return true
	},
}

type client struct {
	hub  *hub
	conn *websocket.Conn
	send chan []byte
}

type hub struct {
	register   chan *client
	unregister chan *client
	broadcast  chan []byte
	shutdown   chan struct{}
	clients    map[*client]bool
}

func newHub() *hub {
	return &hub{
		register:   make(chan *client),
		unregister: make(chan *client),
		broadcast:  make(chan []byte),
		shutdown:   make(chan struct{}),
		clients:    make(map[*client]bool),
	}
}

func (h *hub) run() {
	for {
		select {
		case client := <-h.register:
			h.clients[client] = true
			log.Printf("client connected: %s", client.conn.RemoteAddr())

		case client := <-h.unregister:
			if _, exists := h.clients[client]; exists {
				delete(h.clients, client)
				close(client.send)

				log.Printf(
					"client disconnected: %s",
					client.conn.RemoteAddr(),
				)
			}

		case message := <-h.broadcast:
			for client := range h.clients {
				select {
				case client.send <- message:
				default:
					// Remove clients that cannot receive messages quickly enough.
					delete(h.clients, client)
					close(client.send)
					_ = client.conn.Close()
				}
			}

		case <-h.shutdown:
			for client := range h.clients {
				close(client.send)
				_ = client.conn.Close()
			}

			return
		}
	}
}

func (h *hub) registerClient(client *client) {
	select {
	case h.register <- client:
	case <-h.shutdown:
		_ = client.conn.Close()
	}
}

func (h *hub) unregisterClient(client *client) {
	select {
	case h.unregister <- client:
	case <-h.shutdown:
	}
}

func (h *hub) broadcastMessage(message []byte) {
	select {
	case h.broadcast <- message:
	case <-h.shutdown:
	}
}

func handleWebSocket(hub *hub, writer http.ResponseWriter, request *http.Request) {
	conn, err := upgrader.Upgrade(writer, request, nil)
	if err != nil {
		log.Printf("WebSocket upgrade error: %v", err)
		return
	}

	client := &client{
		hub:  hub,
		conn: conn,
		send: make(chan []byte, 256),
	}

	hub.registerClient(client)

	go client.writePump()
	go client.readPump()
}

func (c *client) readPump() {
	defer func() {
		c.hub.unregisterClient(c)
		_ = c.conn.Close()
	}()

	c.conn.SetReadLimit(maxMessageSize)
	_ = c.conn.SetReadDeadline(time.Now().Add(pongWait))

	c.conn.SetPongHandler(func(string) error {
		return c.conn.SetReadDeadline(time.Now().Add(pongWait))
	})

	for {
		messageType, message, err := c.conn.ReadMessage()
		if err != nil {
			break
		}

		if messageType != websocket.TextMessage {
			continue
		}

		message = []byte(strings.TrimSpace(string(message)))

		if len(message) == 0 {
			continue
		}

		formatted := []byte(
			"[" + c.conn.RemoteAddr().String() + "] " + string(message),
		)

		c.hub.broadcastMessage(formatted)
	}
}

func (c *client) writePump() {
	ticker := time.NewTicker(pingPeriod)

	defer func() {
		ticker.Stop()
		_ = c.conn.Close()
	}()

	for {
		select {
		case message, ok := <-c.send:
			_ = c.conn.SetWriteDeadline(time.Now().Add(writeWait))

			if !ok {
				_ = c.conn.WriteMessage(
					websocket.CloseMessage,
					[]byte{},
				)
				return
			}

			if err := c.conn.WriteMessage(
				websocket.TextMessage,
				message,
			); err != nil {
				return
			}

		case <-ticker.C:
			_ = c.conn.SetWriteDeadline(time.Now().Add(writeWait))

			if err := c.conn.WriteMessage(
				websocket.PingMessage,
				nil,
			); err != nil {
				return
			}
		}
	}
}

// Run starts the WebSocket server.
func Run(address string) error {
	hub := newHub()
	go hub.run()

	mux := http.NewServeMux()
	mux.HandleFunc("/ws", func(
		writer http.ResponseWriter,
		request *http.Request,
	) {
		handleWebSocket(hub, writer, request)
	})

	httpServer := &http.Server{
		Addr:    address,
		Handler: mux,
	}

	signalChannel := make(chan os.Signal, 1)
	signal.Notify(
		signalChannel,
		os.Interrupt,
		syscall.SIGTERM,
	)

	go func() {
		<-signalChannel

		log.Println("shutting down server...")

		close(hub.shutdown)
		_ = httpServer.Close()
	}()

	log.Printf("server listening on %s", address)
	log.Printf("WebSocket endpoint: ws://%s/ws", address)

	err := httpServer.ListenAndServe()

	if err == http.ErrServerClosed {
		return nil
	}

	return err
}
