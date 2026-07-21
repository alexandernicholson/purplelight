# Purplelight upgrade and performance notes

Updated: 2026-07-21

## Completed

- Pinned managed development Ruby 4.0.2; no Homebrew Ruby is used.
- Updated all compatible dependencies: MongoDB Ruby driver 2.24.1, BSON 5.2.0, zstd-ruby 2.0.6, red-arrow/red-parquet 25.0.0, Rake 13.4.2, RSpec 3.13.2, RuboCop 1.88.2, SimpleCov 1.0.2, StackProf 0.2.28, and current transitives. `diff-lcs` 2.0.0 remains blocked by RSpec's `< 2.0` constraint.
- Updated CI to MongoDB 7 and 8, Ruby 3.2 and 4.0, Apache Arrow 25, and `actions/checkout@v7.0.1`.
- Added 23 bounded microbenchmarks covering all eight runtime paths. The harness enforces 100% path registration, takes the median of five samples, measures allocations, and aborts an entire case after two seconds. The slowest observed timed sample was 0.340 seconds.
- Added CPU and object-allocation StackProf tasks plus generated flamegraph support.
- Added SimpleCov line and branch enforcement. The full suite verifies 100% line and branch coverage, including CLI and MongoDB integration behavior.
- Added focused behavioral coverage for queue backpressure, manifests, telemetry, partition planning, compression backends, rotations, Parquet row groups, resumability, concurrent JSONL writing, progress, and telemetry output.
- Made persistent MongoDB fixtures rerunnable by dropping static test collections before seeding.
- Persisted the exact Extended JSON partition plan in manifest version 2. Resume now reuses the original boundaries rather than replanning against a growing collection, preventing boundary replay and duplicate rows.

## Retained performance changes

- ByteQueue stores payloads and sizes in parallel arrays instead of allocating a pair per enqueue. Confirmed round-trip runs improved from 1,792,596.54 to 2,637,130.80–2,657,101.17 operations/second: **+47.1% to +48.2%**. Oversized single items are also admitted when the queue is empty, avoiding a producer deadlock.
- JSONL readers pass row and byte metadata with pre-encoded batches, removing a full `String#count("\n")` scan in the writer. The bounded accounting benchmark rose from 37,313.43 to more than 320,000 operations/second: **over +750%**.
- JSONL writers cache their thread-local telemetry sink after the first write. Two confirmation runs improved the pre-encoded path by **+3.2% to +6.1%** without a confirmed JSONL pipeline regression.
- Snapshot progress reporting no longer creates a mandatory two-second shutdown delay. Runs without a callback create no progress thread; callback runs use a condition variable and wake immediately on shutdown.
- Two JSONL writer threads now perform real concurrent serialization/compression and allocate globally unique part numbers. On the 20,000-document incompressible pipeline, throughput improved from 9.56 to 15.14 exports/second: **+58.4%**, with complete row-count verification.
- JSONL batching honors `batch_size` as well as the 1 MB byte ceiling, bounding latency and memory for small documents.
- CSV uses an allocation-conscious serializer instead of constructing one `CSV.generate_line` result per row. Confirmed batch throughput improved from 1,211.39 to 1,716.50–1,738.04 operations/second: **+41.7% to +43.5%**.
- Manifest configuration avoids redundant atomic rewrites. Confirmed progress runs improved from 1,343.24 to 1,753.49–1,764.93 operations/second: **+30.5% to +31.4%**.
- Parquet uses the red-parquet 25 API directly, applies compression through `Parquet::WriterProperties`, and drains row groups with an indexed buffer instead of repeated front deletion. Final runs reached 296.22–323.08 operations/second versus the initial 203.52: **at least +45.5%**.
- Compression defaults were selected from bounded comparisons: zstd level 3 for JSONL, zstd level 9 for CSV, and gzip level 1 for throughput-oriented fallback.
- CSV manifests count only rows actually written; pre-encoded strings are skipped without inflating progress.

## Rejected or unnecessary changes

- Compact manifest JSON was measured and reverted: it remained below the 3% retention threshold.
- A ring-buffer rewrite for ByteQueue was rejected. MRI's front-removal behavior kept the bulk-drain case stable, while the lower-allocation parallel-array representation supplied the retained gain.
- Alternative CSV gzip and zstd levels that did not produce a repeatable throughput/size improvement were rejected.
- In-place escaping for nested JSON in CSV was measured and reverted. Dedicated row serialization moved by +2.4% and +3.1% across two runs, so it did not repeatedly clear the 3% threshold.
- No replacement gems or external `alexandernicholson/gems` repository were needed. Current maintained dependencies met the compatibility and measured performance requirements.

## Follow-up suggestions

- Run the existing opt-in one-million-document load benchmark only on a dedicated, named production-like host; record CPU, storage, MongoDB topology, and compression ratio with every result. It intentionally remains outside the sub-two-second microbenchmark harness.
- Define schema evolution for CSV and Parquet. Columns are inferred from the first available batch, so fields first appearing in later batches are not added.
- Consider batching or journaling manifest checkpoints for high-latency filesystems. Current per-batch atomic rewrites prioritize resumability over maximum throughput.
- Define an optional ordering contract if consumers need global document order with multiple JSONL writer threads. Current parts are collision-free and complete, but independently scheduled.
- Track the 3% regression threshold on a pinned benchmark runner. Compression timings vary with host load and should not become a hard gate on shared CI workers.
- Recheck `diff-lcs` when RSpec permits 2.x, and continue testing both MongoDB 7 and 8 before future driver upgrades.
