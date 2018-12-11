# Standalone bitmapist server

This is a standalone server to be used with [bitmapist library](https://github.com/Doist/bitmapist) instead of redis.

Python bitmapist library relies on redis bitmap operations and redis stores bitmaps in plain byte arrays, that could become quite wasteful when you deal with big/sparse bitmaps. This standalone server implements subset of redis operations used by bitmapist library and relies on [compressed bitmaps representation](http://roaringbitmap.org) which saves a lot of memory. Another memory-saving technique used is only keeping *hot* dataset in memory.

Example on heavily used bitmapist setup:

Memory in use reported by redis (matches RSS of redis-server process): 129.48G.

With the same dataset migrated to standalone bitmapist server under the same load: RSS reported at about 300M.

## Installation

You'd need [Go](https://golang.org/dl/) in order to build bitmapist server. To build bitmapist server:

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

Service mmaps its database so it is not safe to copy database while process is running. If you need to get a consistent snapshot without downtime, point `-bak` flag to a separate file; process would save a consistent copy of its database to this file on USR1 signal.

You may need to migrate data from already running redis instance; to do so, issue a special command over redis protocol: `slurp host:port` where host:port specifies address of running redis instance filled with data. Note that all string keys would be imported from this redis instance, it's expected that instance is only used for bitmapist data.

Special command `info keys` displays total number of keys in database and number of cached (hot) keys.

## Caveats

Service does not support multiple redis databases, running `select` command with argument different from 0 would fail.
