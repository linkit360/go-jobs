.PHONY: run build

rm:
	rm bin/jobs-linux-amd64; rm ~/linkit/jobs-linux-amd64

build:
	export GOOS=linux; export GOARCH=amd64; \
  go build -ldflags "-s -w" -o bin/jobs-linux-amd64 ; cp bin/jobs-linux-amd64 ~/linkit ; cp dev/jobs.yml ~/linkit/

