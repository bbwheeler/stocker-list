# Build stage
FROM docker.io/golang:1.25-bookworm AS build
WORKDIR /src

COPY . .

RUN go mod tidy
RUN CGO_ENABLED=0 go build -o /out/stocker-list ./cmd/server

# Runtime stage
FROM gcr.io/distroless/static-debian12
COPY --from=build /out/stocker-list /stocker-list
USER 65532:65532
ENTRYPOINT ["/stocker-list"]
