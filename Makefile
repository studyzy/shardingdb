resharding:
	go build -o bin/resharding cmd/resharding/main.go

ut:
	go test -v ./...