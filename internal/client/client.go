package client

import (
	"bufio"
	"fmt"
	"log"
	"os"
	"strings"
	"time"

	"github.com/gorilla/websocket"
)

func Run(address string) error {
	url := fmt.Sprintf("ws://%s/ws", address)

	conn, _, err := websocket.DefaultDialer.Dial(url, nil)
	if err != nil {
		return fmt.Errorf("connect to server: %w", err)
	}

	defer conn.Close()

	log.Printf("connected to %s", url)
	log.Println("type a message and press Enter")
	log.Println("press Ctrl+C or Ctrl+D to exit")

	done := make(chan struct{})

	// Receive messages from the server.
	go func() {
		defer close(done)

		for {
			messageType, message, err := conn.ReadMessage()
			if err != nil {
				log.Printf("server disconnected: %v", err)
				return
			}

			if messageType == websocket.TextMessage {
				fmt.Println(string(message))
			}
		}
	}()

	// Read messages from stdin and send them to the server.
	scanner := bufio.NewScanner(os.Stdin)

	for scanner.Scan() {
		message := strings.TrimSpace(scanner.Text())

		if message == "" {
			continue
		}

		err := conn.WriteMessage(
			websocket.TextMessage,
			[]byte(message),
		)
		if err != nil {
			return fmt.Errorf("send message: %w", err)
		}
	}

	if err := scanner.Err(); err != nil {
		return fmt.Errorf("read stdin: %w", err)
	}

	_ = conn.WriteControl(
		websocket.CloseMessage,
		websocket.FormatCloseMessage(
			websocket.CloseNormalClosure,
			"",
		),
		time.Now().Add(time.Second),
	)

	select {
	case <-done:
	case <-time.After(time.Second):
	}

	return nil
}
