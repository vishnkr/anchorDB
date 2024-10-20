compile:
	protoc api/v1/*.proto \
	--go_out=. \
	--go-grpc_out=. \
	--go_opt=paths=source_relative \
	--go-grpc_opt=paths=source_relative \
	--experimental_allow_proto3_optional \
	--proto_path=.

build:
	go build -o bin/anchordb ./cmd/main.go

run:
	go run ./cmd/main.go

test:
	go test -race ./...