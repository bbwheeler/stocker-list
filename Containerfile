# Build stage
FROM docker.io/golang:1.25-bookworm AS build
WORKDIR /src

COPY . .

ENV PATH="$PATH:/root/go/bin"

RUN go mod tidy
RUN CGO_ENABLED=0 go build -o /out/stocker-list ./cmd/server

# Runtime stage
FROM gcr.io/distroless/static-debian12
COPY --from=build /out/stocker-list /stocker-list
EXPOSE 50051
ENTRYPOINT ["/stocker-list"]
