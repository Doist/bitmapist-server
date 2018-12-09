ARG GO_VERSION=1.11.2
ARG ALPINE_VERSION=3.8

FROM golang:${GO_VERSION}-alpine${ALPINE_VERSION} AS builder

# Install the Certificate-Authority certificates for the app to be able to make
# calls to HTTPS endpoints.
# Git is required for fetching the dependencies.
RUN apk add --no-cache git

# Set the working directory outside $GOPATH to enable the support for modules.
WORKDIR /src

# Fetch dependencies first; they are less susceptible to change on every build
# and will therefore be cached for speeding up the next build
COPY ./go.mod ./go.sum ./
RUN go mod download

# Import the code from the context.
COPY ./ ./

# Build the executable to `/app`. Mark the build as statically linked.
RUN CGO_ENABLED=0 go build \
    -installsuffix 'static' \
    -o /bitmapist-server .

# Final stage: the running container.
FROM alpine:${ALPINE_VERSION} AS final

# Create the user and group files that will be used in the running container to
# run the process as an unprivileged user.
RUN addgroup -S bitmapist && \
    adduser -D -S -h /data -G bitmapist bitmapist

# Import the compiled executable from the first stage.
COPY --from=builder /bitmapist-server /app/

EXPOSE 6379
VOLUME /data

# Perform any further action as an unprivileged user.
USER bitmapist:bitmapist

# Run the compiled binary.
CMD ["/app/bitmapist-server", "-addr=0.0.0.0:6379", "-bak=/data/bitmapist.bck", "-db=/data/bitmapist.db"]