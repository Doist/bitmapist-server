FROM golang:latest as builder
RUN CGO_ENABLED=0 GOOS=linux go install -a -installsuffix=nocgo std
WORKDIR /project
COPY go.sum go.mod ./
RUN go mod download
COPY . .
RUN go test ./...
RUN CGO_ENABLED=0 GOOS=linux go build -ldflags="-s -w" -installsuffix=nocgo -o=/tmp/bitmapist-server
FROM scratch
COPY --from=builder /tmp/bitmapist-server .
VOLUME /data
EXPOSE 6379
CMD ["./bitmapist-server", "-addr=:6379", "-db=/data/bitmapist.db", "-bak=/data/bitmapist.bak"]
