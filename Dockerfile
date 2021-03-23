FROM golang:alpine AS builder
WORKDIR /app
ENV GOPROXY=https://proxy.golang.org CGO_ENABLED=0
COPY go.mod go.sum ./
RUN go mod download
COPY . ./
RUN test -s ci-version.txt && \
    go build -trimpath -ldflags='-s -w' -o bitmapist-server \
        -ldflags="-X=main.explicitVersion=$(cat ci-version.txt)" || \
    go build -trimpath -ldflags='-s -w' -o bitmapist-server

FROM scratch
COPY --from=builder /app/bitmapist-server .
VOLUME /data
EXPOSE 6379
CMD ["./bitmapist-server", "-addr=:6379", "-db=/data/bitmapist.db", "-bak=/data/bitmapist.bak"]
