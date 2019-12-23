# Standalone bitmapist server

This is a standalone server to be used with [bitmapist library](https://github.com/Doist/bitmapist4) instead of Redis.

Python bitmapist library relies on Redis bitmap operations and Redis stores bitmaps in plain byte arrays, which could become quite wasteful when you deal with big/sparse bitmaps. This standalone server implements a subset of Redis operations used by bitmapist library and relies on [compressed bitmaps representation](http://roaringbitmap.org) which saves a lot of memory. Another memory-saving technique used is only keeping *hot* dataset in memory.

Example on heavily used bitmapist setup:

Memory in use reported by Redis (matches RSS of the redis-server process): 129.48G.

With the same dataset migrated to standalone bitmapist server under the same load: RSS reported at about 300M.

## Installation

You'd need [Go](https://golang.org/dl/) to build the server. To build bitmapist server:

    cd bitmapist-server # directory you've cloned repository
    go build

Binary will be saved as `bitmapist-server`.

## Usage

    Usage of bitmapist-server:
      -addr string
            address to listen (default "localhost:6379")
      -bak string
            file to save backup to on SIGUSR1
      -db string
            path to database file (default "bitmapist.db")

Service mmaps its database so it is not safe to copy the database while the process is running. If you need to get a consistent snapshot without downtime, point `-bak` flag to a separate file; the process would save a consistent copy of its database to this file on USR1 signal.

You may need to migrate data from already running Redis instance; to do so, issue a special command over Redis protocol: `slurp host:port [db]` where `host`, `port` and `db` specify the address and optionally the number of the running Redis database to import. Note that all string keys would be imported from this Redis instance, it's expected that instance is only used for bitmapist data.

Special command `info keys` displays the total number of keys in the database and number of cached (hot) keys.

## Caveats

Service does not support multiple Redis databases, running `select` command with argument different from 0 would fail.
