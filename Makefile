.PHONY: run build

rm:
	rm bin/mo-linux-amd64; rm ~/linkit/mo-linux-amd64

build:
	export GOOS=linux; export GOARCH=amd64; \
  go build -ldflags "-s -w" -o bin/mo-linux-amd64 ; cp bin/mo-linux-amd64 ~/linkit

