package main

import (
	"flag"
	"fmt"
	"os"

	"github.com/fiwon123/broadcast-server-go/internal/client"
	"github.com/fiwon123/broadcast-server-go/internal/server"
)

func main(){
	if len(os.Args) < 2 {
		printUsage()
		os.Exit(1)
	}

	switch os.Args[1] {
	case "start":
		startServer(os.Args[2:])
	case "connect":
		connectClient(os.Args[2:])
	default:
		printUsage()
		os.Exit(1)
	}
}


func startServer(args []string) {
	flags := flag.NewFlagSet("start", flag.ExitOnError)

	address := flags.String(
		"addr",
		":8080",
		"address for the server to listen on",
	)

	_ = flags.Parse(args)

	if err := server.Run(*address); err != nil {
		fmt.Fprintf(os.Stderr, "server error: %v\n", err)
		os.Exit(1)
	}
}

func connectClient(args []string) {
	flags := flag.NewFlagSet("connect", flag.ExitOnError)

	address := flags.String(
		"addr",
		"localhost:8080",
		"broadcast server address",
	)

	_ = flags.Parse(args)

	if err := client.Run(*address); err != nil {
		fmt.Fprintf(os.Stderr, "client error: %v\n", err)
		os.Exit(1)
	}
}

func printUsage() {
	fmt.Println(`Usage:
  broadcast-server start [options]
  broadcast-server connect [options]

Commands:
  start       Start the WebSocket broadcast server
  connect     Connect to the server as a client

Examples:
  broadcast-server start
  broadcast-server start -addr :9000
  broadcast-server connect
  broadcast-server connect -addr localhost:9000`)
}