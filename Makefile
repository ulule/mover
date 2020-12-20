lint:
	golangci-lint run

build:
	go build -o mover ./cmd/mover/ 

test:
	go test ./... -v
