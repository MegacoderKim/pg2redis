FROM golang:1.22-bookworm

WORKDIR /app
COPY go.mod main.go ./
RUN go get github.com/redis/go-redis/v9@v9.7.0 && go mod tidy && go build -o /go-redis-test .

CMD ["/go-redis-test"]
