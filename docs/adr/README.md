# Architecture Decision Records

Short records of each significant decision, written **when it was made**. They are the raw
material for report §3 and §5, and they are evidence in a viva that decisions were reasoned
about in advance rather than rationalised afterwards.

Format: context → decision → consequences → status. Deliberately terse; the full argument for
each lives in the corresponding `plan/` document, which each record links to.

| # | Decision | Status |
|---|---|---|
| [001](001-lambda-over-kappa.md) | Lambda architecture, not Kappa | Accepted |
| [002](002-vehicle-id-partition-key.md) | Partition Kafka by `vehicle_id` | Accepted |
| [003](003-spark-over-storm.md) | Spark Structured Streaming, not Storm | Accepted |
| [004](004-redis-speed-layer-store.md) | Redis as the speed-layer store | Accepted |
| [005](005-high-water-mark-merge.md) | Explicit high-water-mark for the serving merge | Accepted |
| [006](006-spark-local-mode-default.md) | Spark in local mode by default | Accepted |
