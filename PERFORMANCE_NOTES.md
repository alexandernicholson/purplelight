# Purplelight upgrade and performance notes

Updated: 2026-07-23

## Completed

- Pinned managed development Ruby 4.0.2; no Homebrew Ruby is used.
- Updated all Ruby 3.2-compatible dependencies: MongoDB Ruby driver 2.24.1, BSON 5.2.0, zstd-ruby 2.0.6, red-arrow/red-parquet 25.0.0, Rake 13.4.2, RSpec 3.13.2, RuboCop 1.88.2, SimpleCov 1.0.2, StackProf 0.2.28, and current transitives. `parallel` remains on 1.28 because 2.x requires Ruby 3.3; `diff-lcs` 2.0 remains blocked by RSpec's `< 2.0` constraint.
- Updated CI to MongoDB 7 and 8, Ruby 3.2 and 4.0, Apache Arrow 25, and `actions/checkout@v7.0.1`.
- Added 55 bounded microbenchmarks covering all eight runtime paths, including 28 Parquet column types plus full-table, row-group, and projection reads. The harness enforces 100% path registration, takes the median of five samples, measures allocations, and aborts each timed sample or allocation pass after two seconds. The slowest observed timed sample in the expanded suite was 0.397 seconds.
- Added CPU and object-allocation StackProf tasks plus generated flamegraph support.
- Added SimpleCov line and branch enforcement. The full suite verifies 100% line and branch coverage, including CLI and MongoDB integration behavior.
- Added focused behavioral coverage for queue backpressure, manifests, telemetry, partition planning, compression backends, rotations, Parquet row groups, resumability, concurrent JSONL writing, progress, and telemetry output.
- Made persistent MongoDB fixtures rerunnable by dropping static test collections before seeding.
- Persisted the exact Extended JSON partition plan in manifest version 2. Resume now reuses the original boundaries rather than replanning against a growing collection, preventing boundary replay and duplicate rows.
- BSON Symbol values now use Purplelight's direct UTF-8 Arrow string builder for scalar columns and recursive list leaves. Cached row-group schemas therefore remain buildable instead of failing through red-arrow's unsupported `DictionaryDataType#build_array` path.
- Snapshot reader and writer failures now atomically close and discard the shared byte queue, immediately waking blocked producers and preserving the first worker exception for the caller.

## Retained performance changes

- ByteQueue stores payloads and sizes in parallel arrays instead of allocating a pair per enqueue. Confirmed round-trip runs improved from 1,792,596.54 to 2,637,130.80–2,657,101.17 operations/second: **+47.1% to +48.2%**. Oversized single items are also admitted when the queue is empty, avoiding a producer deadlock.
- JSONL readers pass row and byte metadata with pre-encoded batches, removing a full `String#count("\n")` scan in the writer. The bounded accounting benchmark rose from 37,313.43 to more than 320,000 operations/second: **over +750%**.
- JSONL writers cache their thread-local telemetry sink after the first write. Two confirmation runs improved the pre-encoded path by **+3.2% to +6.1%** without a confirmed JSONL pipeline regression.
- Snapshot progress reporting no longer creates a mandatory two-second shutdown delay. Runs without a callback create no progress thread; callback runs use a condition variable and wake immediately on shutdown.
- Two JSONL writer threads now perform real concurrent serialization/compression and allocate globally unique part numbers. On the 20,000-document incompressible pipeline, throughput improved from 9.56 to 15.14 exports/second: **+58.4%**, with complete row-count verification.
- JSONL batching honors `batch_size` as well as the 1 MB byte ceiling, bounding latency and memory for small documents.
- CSV uses an allocation-conscious serializer instead of constructing one `CSV.generate_line` result per row. Confirmed batch throughput improved from 1,211.39 to 1,716.50–1,738.04 operations/second: **+41.7% to +43.5%**.
- Manifest configuration avoids redundant atomic rewrites. Confirmed progress runs improved from 1,343.24 to 1,753.49–1,764.93 operations/second: **+30.5% to +31.4%**.
- Parquet now constructs Arrow record batches directly, caches the first row group's schema, packs primitive buffers without per-value GObject calls, preserves requested row-group boundaries through `Parquet::WriterProperties`, and keeps one native writer open per output part. The 200-document benchmark rose from 306.10 to 871.99–1,004.42 operations/second across three runs (**+184.9% to +228.1%**) while allocations fell from 12,281.0 to 2,263.2 per export (**-81.6%**). At 10,000 documents, median throughput rose from 7.23 to 46.71 operations/second (**+545.7%**) and allocations fell from 565,487 to 54,413 (**-90.4%**); output sizes remained unchanged for the compared row-group layouts.
- Parquet batches each BSON ObjectId column's raw 12-byte values before one hexadecimal conversion, instead of allocating a hexadecimal string and `String#unpack` result per document. Alternating matched runs improved the 200-document export from 918.40 to 982.90 operations/second (**+7.0%**) and cut allocations from 2,266.15 to 1,872.15 (**-17.4%**). The 10,000-document row-group export improved from 47.05 to 50.23 operations/second (**+6.8%**) and from 54,413 to 34,443 allocations (**-36.7%**); a single 10,000-row group improved **+8.3%** with **-39.3%** allocations. Schemas, values, row-group counts, and artifact sizes remained unchanged.
- Parquet now disables dictionary encoding for scalar columns unless the first row group proves a string has at most 16 distinct values and at least four repetitions per value. It preserves dictionary encoding for every list leaf, avoiding reader-specific list regressions. Seven interleaved Arrow-reader samples over 50,000 rows and 27 column types improved geometric-mean raw-column throughput by **21.4%**, full-table reads by **15.9%**, row-group streaming by **29.2%**, and projected primitive reads by **30.9%**; no individual column regressed by 3%, and the file shrank from 3,959,054 to 2,713,148 bytes (**-31.5%**). Independent DuckDB checks improved integer aggregates by **18.4%**, string aggregates by **10.2%**, low-cardinality filters by **13.4%**, range filters by **13.6%**, projections by **5.0%**, and full materialization by **3.6%**, while list aggregation remained unchanged.
- Compression defaults were selected from bounded comparisons: zstd level 3 for JSONL, zstd level 9 for CSV, and gzip level 1 for throughput-oriented fallback.
- CSV manifests count only rows actually written; pre-encoded strings are skipped without inflating progress.
- Cached integer schemas now reject later values outside their represented range instead of allowing Arrow's integer builder to wrap them silently.

## Rejected or unnecessary changes

- Compact manifest JSON was measured and reverted: it remained below the 3% retention threshold.
- A ring-buffer rewrite for ByteQueue was rejected. MRI's front-removal behavior kept the bulk-drain case stable, while the lower-allocation parallel-array representation supplied the retained gain.
- Alternative CSV gzip and zstd levels that did not produce a repeatable throughput/size improvement were rejected.
- In-place escaping for nested JSON in CSV was measured and reverted. Dedicated row serialization moved by +2.4% and +3.1% across two runs, so it did not repeatedly clear the 3% threshold.
- Blanket Parquet dictionary disabling was rejected despite strong aggregate gains: it regressed low-cardinality strings by 9.2%, integer lists by 6.7%, nested integer lists by 6.5%, and DuckDB list aggregation by about 10%. The retained adaptive policy preserves those dictionaries. Global dictionary-page limits, 20,000–50,000-row groups, and native batch/data-page changes were also rejected because they either stayed below 3% or regressed filtered, full-materialization, temporal, or list workloads.
- Preallocating string capacity, avoiding the integer `compact` copy, borrowing caller buffers, bulk-joining ObjectId bytes, direct native writer calls, caching writer properties, and skipping redundant directory creation each stayed below 3% or regressed and were rejected.
- Direct three-argument `RecordBatch` construction improved the 200-document case by 7.0% but only -0.6% to +1.9% on the 10,000-document cases, so it was rejected as a first-row-group-only gain that did not persist at scale.
- No replacement gems or external `alexandernicholson/gems` repository were needed. Current maintained dependencies met the compatibility and measured performance requirements.

## Follow-up suggestions

- Run the existing opt-in one-million-document load benchmark only on a dedicated, named production-like host; record CPU, storage, MongoDB topology, and compression ratio with every result. It intentionally remains outside the sub-two-second microbenchmark harness.
- Define schema evolution for CSV and Parquet. Columns are inferred from the first available batch, so fields first appearing in later batches are not added; an explicit Parquet schema option would also let callers avoid narrow first-row-group integer inference.
- Further Parquet gains likely require an upstream/native bulk-buffer API: after the retained rewrite, CPU samples were dominated by GObject-introspection constructor calls and the native Parquet writer rather than Ruby row transposition.
- A custom native Parquet read extension is not justified: Arrow and DuckDB already perform decoding natively, while profiling isolated file encoding—not Ruby dispatch—as the controllable bottleneck. Reconsider only if a concrete reader profile still spends more than 3% in Purplelight-owned Ruby code.
- Parquet columns containing literal dots retain Arrow's default dictionary behavior because the Ruby writer API exposes dot-delimited physical paths without an escaping mechanism. Revisit if red-parquet adds a structured `ColumnPath` binding.
- Nested Ruby hashes now dominate the remaining avoidable Parquet allocations because compatibility requires their existing `Hash#inspect` string representation. An explicit nested Struct/JSON encoding option could remove that cost without silently changing current files.
- Consider batching or journaling manifest checkpoints for high-latency filesystems. Current per-batch atomic rewrites prioritize resumability over maximum throughput.
- Define an optional ordering contract if consumers need global document order with multiple JSONL writer threads. Current parts are collision-free and complete, but independently scheduled.
- Track the 3% regression threshold on a pinned benchmark runner. Compression timings vary with host load and should not become a hard gate on shared CI workers.
- Recheck `diff-lcs` when RSpec permits 2.x, and continue testing both MongoDB 7 and 8 before future driver upgrades.
