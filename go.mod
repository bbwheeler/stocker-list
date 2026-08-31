module github.com/anomalyco/stocker-list

go 1.25.0

require stocker-store v0.0.0

require google.golang.org/protobuf v1.36.12 // indirect

replace stocker-store v0.0.0 => git.wheeli.ca/brian/stocker-store v0.0.0-20260831021953-e273f709a428
