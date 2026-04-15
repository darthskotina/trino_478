# Kafka Connector Operator Notes

This README documents the branch-specific committed-read behavior intended for
this modified Kafka connector. It complements the upstream connector reference
in [docs/src/main/sphinx/connector/kafka.md](../../docs/src/main/sphinx/connector/kafka.md)
and is meant to capture the operational contract and caveats of the
consumer-group offset commit feature.

## Modes

The connector has two read modes:

- default mode
- committed-read mode

Default mode keeps the existing bounded-scan behavior:

- split planning is based on broker beginning and end offsets
- multiple splits per partition are allowed
- reads ignore Kafka consumer-group committed offsets
- no offsets are committed by reads
- `kafka.consumer-group-id` and its current hardcoded default remain
  compatibility behavior for this mode only

Committed-read mode is opt-in and changes read semantics:

- the connector resumes from Kafka committed offsets for the effective group ID
- the connector plans at most one data split per partition
- the connector commits the next offset to read after source split exhaustion
- commit semantics are split-local and best-effort; they are not whole-query
  success semantics

## Properties

Catalog properties used by committed-read mode:

- `kafka.committed-read-enabled`
- `kafka.committed-read-missing-offset-policy`

Session properties used by committed-read mode:

- `committed_read_enabled`
- `committed_read_group_id`

Legacy compatibility property:

- `kafka.consumer-group-id`

Compatibility rules:

- `kafka.consumer-group-id` remains active only for default-mode consumer
  construction
- `kafka.consumer-group-id` is never used as a fallback committed-read group
- `committed_read_group_id` is a new session property used only when
  committed-read mode is enabled

## Effective Group ID

Committed-read mode resolves the effective group ID as follows:

1. session `committed_read_group_id`, if set
2. otherwise fail the query

Rules:

- committed-read mode requires an explicit session group ID
- blank group IDs are invalid
- one query has one effective committed-read group ID across all Kafka scans in
  that session
- separate sessions may use different committed-read group IDs concurrently in
  the same catalog
- a single shared session or JDBC connection must not be mutated concurrently by
  multiple clients expecting different group IDs

Example:

```sql
SET SESSION kafka.committed_read_enabled = true;
SET SESSION kafka.committed_read_group_id = 'svc-orders-main';

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

Important consequence:

- a split may commit even if the overall query later fails elsewhere
- a small split may commit even when the query contains `LIMIT`, if Trino has
  already exhausted that split locally

This feature does not provide:

- exactly-once query semantics
- whole-query atomic offset commits
- commit-on-query-success semantics

## Predicate Rules In Committed-Read Mode

Committed-read mode preserves Kafka-like consumer-group resume semantics. For
that reason, SQL predicates must not be allowed to move a group's stored
position forward past unread records.

Committed-read mode therefore rejects:

- lower-bound predicates on `_partition_offset`
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

## Missing Or Invalid Committed Offsets

Catalog property `kafka.committed-read-missing-offset-policy` controls behavior
when:

- no committed offset exists for the effective group ID
- the committed offset is outside the current `[logStart, logEnd]` range

Supported values:

- `EARLIEST`
- `LATEST`
- `ERROR`

`LATEST` may produce a checkpoint-only split:

- the split emits no rows
- the split exists only to persist the resolved initial position

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

Use default mode for:

- ad hoc historical reads
- exploratory queries
- queries that need unrestricted offset or timestamp lower-bound filtering

## README Scope

This README is the operator-facing contract for the modified Kafka connector.
When implementation changes, keep this file aligned with:

- `docs/src/main/sphinx/connector/kafka.md`
- `KAFKA_CONSUMER_GROUP_OFFSET_COMMIT_PLAN.md`
