.PHONY: all test build clean example lint

all: test build

build:
	go build -v ./...

test:
	go test -v -race ./...

clean:
	go clean
	rm -rf ./data
	rm -rf ./testdata

example:
	go run ./examples/basic/main.go

lint:
	go vet ./...
	go fmt ./...

.DEFAULT_GOAL := all 