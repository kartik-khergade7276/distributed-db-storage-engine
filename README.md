# Distributed Database Storage Engine

This is a **minimal log-structured key–value storage engine** written in Java.
It is designed to be:

- Simple enough to read in an interview.
- Realistic enough to talk about segments, log-structured storage, indexing and compaction.
- Fully runnable with `mvn test` and a tiny CLI demo.

## Features

- Append-only **segment files** on disk.
- In-memory index: `key -> (segmentPath, offset)`.
- Simple **compaction** that rewrites the latest values for each key into a new segment.
- Tiny CLI to interactively `PUT`, `GET` and `COMPACT`.

## Requirements

- Java 17+
- Maven 3+

## How to Run the Tests

```bash
mvn test
```

You should see all tests in `StorageEngineTest` pass.

## How to Run the Demo

```bash
mvn compile
mvn exec:java -Dexec.mainClass="com.example.storage.Main"
```

Or, compile and run via `java` directly:

```bash
mvn package
java -cp target/classes com.example.storage.Main
```

A `data/` directory will be created in the working directory with segment files.

### Example Session

```text
=== Distributed-style Storage Engine Demo ===
> PUT user:1 Alice
OK
> PUT user:2 Bob
OK
> GET user:1
Alice
> COMPACT
Compaction completed.
> EXIT
Shutting down.
```

## Talking About This Project


- **Log-structured design**: writes are append-only, which is sequential I/O friendly.
- **Indexing**: an in-memory hash map keeps track of where the latest value lives.
- **Compaction**: periodically rewrite only *live* keys into a new segment to reclaim space.
- **Trade-offs**: 
  - Fast writes, simple durability story.
  - Reads are one disk seek + sequential read.
  - Compaction cost vs. disk usage.

 Extend it with:

- Bloom filters per segment.
- Separate index files.
- Checksums and corruption detection.
- Background compaction threads.
