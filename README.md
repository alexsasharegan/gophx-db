# gophx-db

Submission for the October 2018 Golang Phoenix database challenge.

## Branches

Performance benchmarks done with
[github.com/alexsasharegan/gophx-db-test](https://github.com/alexsasharegan/gophx-db-test).

Benchmark Settings:

- `-bench`
- `-procs=12` _(I have a 6 core duo machine)_
- `-time=60`

### `mutex`

Inspired from
[Chenxi Cai's _(@cchx0000)_](https://github.com/cchx0000/meetupphx) submission.

```txt
✅ All tests passed
    Clients    Duration    Messages Sent & Received    Messages/Second
         12        1m0s                  16,086,403            268,106
```

### `channels`

My own, original implementation.

```txt
✅ All tests passed
    Clients    Duration    Messages Sent & Received    Messages/Second
         12        1m0s                  12,854,464            214,241
```

### `evio`

My favorite implementation using
[Josh Baker's evio library](https://github.com/tidwall/evio).

```txt
✅ All tests passed
    Clients    Duration    Messages Sent & Received    Messages/Second
         12        1m0s                  11,560,414            192,673
```

## Golang Phoenix Requirements

### Details

Hey everyone! Super excited for this meetup. For October, we'll be building a
key/value database server. This is an adaptation from an interview challenge
from a well known company who writes a lot of Go.

We will spend the first portion of the evening going over our implementations
and helping others who haven't quite finished theirs or are looking to learn
from what others have done. We'll spend the last half of the evening presenting
our implementations and running some benchmarks.

You're free to use whatever tools are available to build this.

### Requirements

1. Listen on port `8888`
1. Handle the following commands: `GET`, `SET`, `DEL`, `QUIT`, `BEGIN`,
   `COMMIT`. All commands need to be terminated with a `CRLF` string (`"\r\n"`).
1. Must support transactions. Transactions should be a N+1 queries grouped
   together to operate on the data, segmented off from other transactions.

Non-transactional Command Specifications:

1. `GET` - key -> returns ; GET takes a string key and returns the value if it
   exists otherwise an empty string.
1. `SET` - key, value -> returns ; SET takes a string for a key and a string for
   a value and returns a string. The return string will be "OK" on success or
   contain an error.
1. `DEL` - key; DEL takes a string for the key and removes that entry. The
   return string will be "OK" on success or contain an error.
1. `QUIT` - kills the active connection

Transactional Command Specifications:

1. `BEGIN` - BEGIN indicates to the server that you want to use a transaction
   and to start one. All commands following will be scoped to that transaction.
1. `COMMIT` - COMMIT indicates that the transaction is complete and to finalize
   the transaction by committing any changes to the global data store.

### Examples

#### Regular interaction with the server

```text
SET key1 value1\r\n
GET key1\r\n
DEL key1\r\n
QUIT\r\n
```

In the example above, the expectation is that if "key1" doesn't exist, it is
created and the "value1" is assigned to it. If it already exists, "key1" is
updated with the new value.

#### Transactional interaction with the server

```text
BEGIN\r\n
SET key1 value1\r\n
GET key1\r\n
DEL key1\r\n
COMMIT\r\n
QUIT\r\n
```

In the example above, the expectation is that a new transaction is started which
will cause all of the queries below to operate on their own set of data. That
means that the same expectations apply from the first example however those
changes aren't seen outside of the scope of the transaction. If "key1" is set,
then deleted, another transaction could be performing operations on "key1" at
the same time however only the transaction that gets committed is seen.

Feel free to post any questions about this here or on twitter: @golangphoenix
