FROM golang:alpine AS builder
RUN apk add --update upx
WORKDIR /app
ENV GOPROXY=https://proxy.golang.org CGO_ENABLED=0
COPY go.mod go.sum ./
RUN go mod download
COPY . ./
RUN go build -ldflags='-s -w' -o bitmapist-server && upx --lzma bitmapist-server

FROM scratch
COPY --from=builder /app/bitmapist-server .
VOLUME /data
EXPOSE 6379
CMD ["./bitmapist-server", "-addr=:6379", "-db=/data/bitmapist.db", "-bak=/data/bitmapist.bak"]
