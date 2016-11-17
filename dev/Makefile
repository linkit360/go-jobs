.PHONY: run build

rm:
	rm bin/subscriptions-linux-amd64; rm ~/linkit/subscriptions-linux-amd64

build:
	export GOOS=linux; export GOARCH=amd64; \
  go build -ldflags "-s -w" -o bin/subscriptions-linux-amd64 ; cp bin/subscriptions-linux-amd64 ~/linkit

