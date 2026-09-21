.PHONY: client server

server:
	go run ./cmd/cli start

client:
	go run ./cmd/cli connect
