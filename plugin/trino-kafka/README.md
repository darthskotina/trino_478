# Kafka Connector Operator Notes

This README is the single source of truth for Kafka-related functionality
introduced or changed on this branch relative to `main`.

## Modes

The connector has two read modes:

- default mode
- committed-read mode

Default mode keeps the existing bounded-scan behavior:

- split planning is based on broker beginning and end offsets
- multiple splits per partition are allowed
- reads ignore Kafka consumer-group committed offsets
- no offsets are committed by reads
- the effective consumer group ID is `consumer_group_id` when set, otherwise
  `kafka.consumer-group-id`

Committed-read mode is opt-in and changes read semantics:

- the connector resumes from Kafka committed offsets for the effective group ID
- the connector plans at most one data split per partition
- the connector commits the next offset to read after source split exhaustion
- commit semantics are split-local; they are not whole-query success semantics

## Properties

Catalog properties used by committed-read mode:

- `kafka.committed-read-enabled`
- `kafka.committed-read-missing-offset-policy`

Session properties used by committed-read mode:

- `committed_read_enabled`
- `consumer_group_id`
- `committed_read_allow_offset_rewind`
- `committed_read_max_rows_per_partition`

Legacy compatibility property:

- `kafka.consumer-group-id`

Compatibility rules:

- `kafka.consumer-group-id` remains the legacy default-mode fallback
- `consumer_group_id` overrides `kafka.consumer-group-id` for default-mode
  consumer construction
- `kafka.consumer-group-id` is never used as a fallback committed-read group
- `consumer_group_id` is required when committed-read mode reads run

## Effective Settings

Committed-read mode resolution:

1. session `committed_read_enabled`, if set
2. otherwise catalog `kafka.committed-read-enabled`

Committed-read group ID resolution:

1. session `consumer_group_id`, if set
2. otherwise fail the query

Rules:

- committed-read mode can be enabled either by catalog default or by session
  override
- committed-read mode requires an explicit session group ID
- blank group IDs are invalid
- `kafka.consumer-group-id` is not a committed-read fallback
- one query has one effective committed-read group ID across all Kafka scans in
  that session
- separate sessions may use different committed-read group IDs concurrently in
  the same catalog
- a single shared session or JDBC connection must not be mutated concurrently by
  multiple clients expecting different group IDs

Example:

```sql
SET SESSION kafka.committed_read_enabled = true;
SET SESSION kafka.consumer_group_id = 'svc-orders-main';

SELECT *
FROM kafka.default.orders_topic;
```

To use a different committed-read group ID for another workload in the same
catalog, use a different session or change the session property before the next
query.

## Resume and Commit Semantics

Committed-read mode uses Kafka committed offsets as the resume point for each
topic-partition.

The connector:

- reads from the broker snapshot captured during split planning
- does not read messages appended after split planning
- commits only after the source split is locally exhausted
- commits the next offset to read, not the last emitted offset
- does not commit on early close or partial split consumption
- fails the query if the offset commit itself fails
- fails the query and suppresses commit if a planned offset becomes invalid
  during execution

Important consequence:

- a split may commit even if the overall query later fails elsewhere
- a small split may commit even when the query contains `LIMIT`, if Trino has
  already exhausted that split locally

This feature does not provide:

- exactly-once query semantics
- whole-query atomic offset commits
- commit-on-query-success semantics

## Per-Partition Batch Cap

Committed-read mode can optionally cap split planning with session property
`committed_read_max_rows_per_partition`.

Rules:

- the cap applies in committed-read mode only
- `0` means unlimited and preserves the previous behavior
- the cap is per selected partition, not global per topic or per query
- the cap narrows the planned offset window; it does not guarantee that the
  query returns exactly that many rows
- Kafka realities such as compaction or tombstones may cause fewer rows than
  the capped offset span
- default mode still uses `kafka.messages-per-split`; the new session property
  does not affect default-mode planning

Example:

- a two-partition query with `committed_read_max_rows_per_partition = 1000`
  may read up to `2000` source offsets total in one query
- each partition still commits its own next offset independently

## Predicate Rules In Committed-Read Mode

Committed-read mode preserves Kafka-like consumer-group resume semantics by
default. An additional session flag can opt a query into offset rewind
planning.

Committed-read mode therefore rejects:

- lower-bound predicates on `_partition_offset` by default
- lower-bound predicates on `_timestamp`

Committed-read mode allows `_timestamp` upper bounds only when they are safe for
committed progress:

- topics using `LogAppendTime`: allowed
- topics using `CreateTime`: rejected

Rationale:

- allowing lower bounds would turn group offsets into filter-relative
  checkpoints rather than normal consumer-group resume points
- rejecting unsupported bounds keeps committed offsets meaningful as "next
  unread offset for this group"

Optional rewind extension:

- session `committed_read_allow_offset_rewind = true` allows explicit
  `_partition_offset` lower bounds in committed-read mode
- session `committed_read_max_rows_per_partition` still applies after the
  explicit rewind start is chosen
- this applies to `_partition_offset` only in this iteration; `_timestamp`
  lower bounds remain rejected
- explicit offset windows become authoritative for planning, even when
  `kafka.committed-read-missing-offset-policy=LATEST`
- `_partition_offset = x` reads exactly `[x, x + 1)`
- `_partition_offset > x` starts at `x + 1`
- `_partition_offset >= x` starts at `x`
- if the explicit rewind window resolves to an empty range after broker-range
  clamping, Trino plans no split and commits nothing

Operational consequence:

- fully consumed rewind-enabled reads may move the stored Kafka group offset
  backward only to the end of the fully consumed capped window
- subsequent plain committed-read queries resume from that rebased offset
- rerunning the same rewind query may rewind again, because the explicit lower
  bound remains authoritative for that query
- `_partition_offset` is partition-local; rewind predicates apply to every
  selected partition unless `_partition_id` narrows the scope
- same-group concurrent queries remain unsafe and last-writer-wins, including
  backward offset movement
- the existing repeated-scan guard and the `retry_policy=NONE` requirement
  still apply unchanged

## Missing Or Invalid Committed Offsets

Catalog property `kafka.committed-read-missing-offset-policy` controls behavior
when:

- no committed offset exists for the effective group ID
- the committed offset is outside the current `[logStart, logEnd]` range

Supported values:

- `EARLIEST`
- `LATEST`
- `ERROR`

Default:

- `ERROR`

`LATEST` may produce a checkpoint-only split:

- the split emits no rows
- the split exists only to persist the resolved initial position
- the committed checkpoint target is clamped into the current broker snapshot
  `[logStart, logEnd]`
- the per-partition batch cap does not affect this checkpoint-only
  initialization path
- if pushed-down upper bounds fall below retained data, Trino persists the
  clamped empty-range checkpoint rather than the raw filtered end offset

## Offset Metadata Table Function

This branch also adds the `system.offsets` table function for Kafka.

Use it to inspect current broker offset bounds for a connector-visible Kafka
table without scanning topic rows:

```sql
SELECT partition_id, log_start_offset, log_end_offset, last_readable_offset
FROM TABLE(kafka.system.offsets(schema_name => 'default', table_name => 'orders'))
ORDER BY partition_id;
```

Arguments:

- `schema_name`: schema of the Kafka table exposed through Trino
- `table_name`: table name exposed through Trino
- `partition => <bigint>`: optional; narrows the lookup to one partition

Important argument rule:

- `schema_name` and `table_name` refer to the connector-visible table, not an
  arbitrary broker topic name
- if a Kafka table is exposed under an alias, call the function with that alias

Output columns:

- `partition_id`: Kafka partition ID
- `log_start_offset`: current broker beginning offset
- `log_end_offset`: current broker end offset
- `last_readable_offset`: `log_end_offset - 1` for a non-empty partition, or
  `NULL` when `log_start_offset = log_end_offset`

Properties:

- the result is a point-in-time metadata snapshot
- values can change immediately after lookup
- the function is metadata-only
- it calls Kafka metadata APIs such as `partitionsFor`, `beginningOffsets`,
  and `endOffsets`
- it does not read topic rows
- it does not decode Kafka messages
- runtime cost scales primarily with partition count, not message volume

Examples:

```sql
SELECT partition_id, log_start_offset, log_end_offset, last_readable_offset
FROM TABLE(kafka.system.offsets(schema_name => 'default', table_name => 'orders', partition => 2));
```

```sql
SELECT
    min(log_start_offset) AS topic_min_log_start_offset,
    max(log_end_offset) AS topic_max_log_end_offset
FROM TABLE(kafka.system.offsets(schema_name => 'default', table_name => 'orders'));
```

The function returns one row per partition. Add `ORDER BY` if deterministic
output ordering matters.

The function uses metadata-only Kafka consumers and does not require
`consumer_group_id`, even when committed-read mode is enabled for the session.

## Manual Offset Commit Procedure

This branch adds `kafka.system.commit_offsets` for explicit, user-driven Kafka
offset commits. The procedure resolves `(schema_name, table_name)` through the
connector table description supplier; it does not accept arbitrary topic names.

Argument forms:

- single partition: `partition => <bigint>, offset => <bigint>`
- multiple partitions: `offsets => MAP(ARRAY[partition...], ARRAY[offset...])`
- exactly one form must be supplied
- `allow_out_of_range => true` optionally bypasses broker-range validation

Group ID resolution:

- explicit `group_id` argument wins
- otherwise session `consumer_group_id` is used
- otherwise the procedure fails
- this is independent of `committed_read_enabled`; an explicit `group_id`
  works even when committed-read mode is enabled and no session group is set

Commit mechanics:

- the procedure uses a short-lived subscribe-mode Kafka consumer that joins the
  group
- this differs from the connector read path, which keeps manual `assign(...)`
  semantics
- broker ACLs may therefore differ from default-mode reads
- after assignment converges, the procedure pauses assigned partitions and
  commits the requested offsets

Validation:

- every requested partition must exist in the connector-visible topic
- offsets must be inside `[logStart, logEnd]` unless
  `allow_out_of_range => true`
- `logEnd` is valid and means the group is caught up

Concurrency:

- Kafka offset commits are last-writer-wins
- a concurrent member in the same group can prevent the procedure from owning
  all requested partitions
- in that case, the procedure fails fast after a bounded assignment wait

`allow_out_of_range` and committed-read policy:

- with `kafka.committed-read-missing-offset-policy=ERROR`, a subsequent
  committed-read query fails until the group offset is reset
- with `EARLIEST`, a subsequent committed-read query treats the invalid value
  as missing and reads from `logStart`
- with `LATEST`, a subsequent committed-read query produces a checkpoint-only
  split and overwrites the stored value with a clamped broker-range offset
- use `allow_out_of_range => true` only when the next consumer's policy and
  expectations are known

The procedure is idempotent: repeated calls with the same arguments produce the
same broker-side committed offset state.

Examples:

```sql
CALL kafka.system.commit_offsets(
    schema_name => 'default',
    table_name => 'orders',
    group_id => 'svc-orders-main',
    partition => 0,
    offset => 12345);
```

```sql
CALL kafka.system.commit_offsets(
    schema_name => 'default',
    table_name => 'orders',
    group_id => 'svc-orders-main',
    offsets => MAP(ARRAY[0, 1, 2], ARRAY[12345, 12001, 11990]));
```

```sql
CALL kafka.system.commit_offsets(
    schema_name => 'default',
    table_name => 'orders',
    group_id => 'svc-orders-main',
    partition => 0,
    offset => 999999999,
    allow_out_of_range => true);
```

## Concurrency And Safety

Committed-read mode keeps manual `assign(...)` semantics. It does not use Kafka
group membership, `subscribe(...)`, or rebalance coordination.

Operational consequences:

- same-group concurrent queries are not coordinated by Kafka ownership
- same-group concurrent offset writers are last-writer-wins
- external consumers using the same group ID can interfere with Trino progress

Supported usage shape:

- one tracked scan per `(group ID, topic)` at a time

Unsafe or unsupported usage:

- concurrent Trino queries using the same group ID on the same topic
- external Kafka consumers using the same group ID while committed-read mode is
  active
- workloads that rely on Kafka partition ownership or rebalance behavior

Additional query-level safety rule:

- repeated scans of the same `(queryId, groupId, topic)` fail fast within one
  query to avoid unsafe self-joins or duplicate tracked scans

## Retry Requirement

Committed-read mode is intended to run only when Trino system
`retry_policy=NONE`.

At the moment this is an operational requirement:

- the Kafka connector can see catalog session properties
- the Kafka connector cannot directly inspect Trino system session properties
  such as `retry_policy` through the current `ConnectorSession` API

If the deployment enables task or query retries, committed-read mode should be
treated as unsafe until hard enforcement is added outside pure connector scope.

## Transactional Visibility

This feature does not change Kafka transactional visibility.

Out of scope:

- `isolation.level=read_committed`
- transactional consume-process-commit semantics

## Recommended Usage

Use committed-read mode for:

- dedicated service-style tracked consumption
- one logical consumer-group namespace per read path
- workloads that want resume behavior across repeated Trino reads
- controlled rewind/replay workflows that use explicit `_partition_offset`
  windows under a dedicated group ID

Use `system.commit_offsets` for:

- environments where manual-assign `OffsetCommit` is denied but joined-group
  subscribe-mode commits are allowed
- after-the-fact, operator-controlled advancement or repair of group offsets

Use default mode for:

- ad hoc historical reads
- exploratory queries
- queries that need unrestricted offset or timestamp lower-bound filtering

## README Scope

This README is the operator-facing contract for Kafka functionality changed on
this branch. When Kafka implementation changes, update this file first.
