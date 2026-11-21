.PHONY: tidy run build podman-up podman-down podman-build

tidy:
	go mod tidy

build:
	mkdir -p bin
	go build -o bin/stocker-list ./cmd/server

run:
	go run ./cmd/server

podman-build:
	podman-compose -f podman-compose.yml build

podman-up:
	podman-compose -f podman-compose.yml up -d

podman-down:
	podman-compose -f podman-compose.yml down
