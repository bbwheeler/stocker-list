.PHONY: proto proto-protoc tidy run build podman-up podman-down podman-build quadlet-install quadlet-build quadlet-enable
 
tidy:
	go mod tidy
 
build:
    go build -o bin/stocker-list ./cmd/server
 
run:
    go run ./cmd/server
 
podman-build:
	podman-compose -f podman-compose.yml build
 
podman-up:
	podman-compose -f podman-compose.yml up -d
 
podman-down:
	podman-compose -f podman-compose.yml down
 
quadlet-install:
	mkdir -p ~/.config/containers/systemd
	mkdir -p ~/.config/stocker-list
	cp deploy/quadlet/stocker-list-rootless.container ~/.config/containers/systemd/stocker-list.container
	cp -n .env.podman ~/.config/stocker-list/.env.podman 2>/dev/null || true
	chmod 600 ~/.config/stocker-list/.env.podman
	systemctl --user daemon-reload
 
quadlet-build:
	podman build --no-cache -t git.wheeli.ca/brian/stocker-list:latest .
 
quadlet-enable: quadlet-build
	systemctl --user daemon-reload
	systemctl --user start stocker-list.service
