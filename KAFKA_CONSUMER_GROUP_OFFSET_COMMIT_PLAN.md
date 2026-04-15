# Kafka Consumer Group Offset Commit Plan

## Objective

Add an opt-in committed-read mode to the Trino Kafka connector that:

- reads from Kafka consumer-group committed offsets
- processes one Trino split per Kafka partition in that mode
- commits the next safe offset after source split exhaustion
- allows the committed-read consumer group ID to be overridden at session scope
- preserves current behavior as the default mode

This plan is intentionally limited to implementation planning. It does not include code changes.

## Agreed Scope

### In Scope

- Connector-local implementation in the Kafka plugin
- Opt-in committed-read mode
- One split per Kafka partition when committed-read mode is enabled
- Start position resolution from Kafka committed offsets for the effective committed-read group ID
- Commit of the next offset after source split exhaustion
- Catalog properties for committed-read mode
- Session properties for per-query/per-session committed-read mode and group ID override
- Tests that verify offsets were committed on Kafka's side
- Documentation updates for the new behavior and caveats

### Out of Scope

- Whole-query-atomic offset commits
- Query-success-only commits
- Kafka `subscribe(...)` / rebalance-based group management
- Multi-split-per-partition ordered checkpointing
- Backward reads in committed-read mode as a first-class feature
- Any Trino SPI or engine changes outside the Kafka connector unless an unexpected blocker is discovered

## Current Behavior Summary

The connector currently behaves like a bounded table scan over Kafka:

- `KafkaSplitManager` plans offset ranges from `beginningOffsets(...)` and `endOffsets(...)`
- `KafkaRecordSet` reads each split using manual `assign(...)` and `seek(...)`
- consumers are created with `enable.auto.commit=false`
- no `commitSync(...)`, `commitAsync(...)`, or committed-offset resume path exists

There is already a `kafka.consumer-group-id` catalog property in the plugin, but today it does not produce committed-read semantics on reads.

## Intended Semantics

### Default Mode

Default behavior remains unchanged:

- split planning continues to use current offset-range logic
- multiple splits per partition remain allowed
- reads continue to ignore committed group offsets
- no offsets are committed

### Committed-Read Mode

When committed-read mode is enabled:

- the connector plans at most one non-empty split per Kafka partition
- the scan start offset is derived from the effective committed-read group ID and the current bounded query snapshot
- the scan end offset remains the partition end snapshot determined during split planning
- messages produced after split planning are not read or committed by that query
- the connector commits the next offset to read after source split exhaustion

This is a best-effort per-split completion model, not a whole-query commit barrier.

## Semantic Contract

### 1. Effective Settings

Committed-read mode uses the following properties:

- Catalog `kafka.committed-read-enabled`
- Catalog `kafka.committed-read-group-id`
- Catalog `kafka.committed-read-missing-offset-policy`
- Session `committed_read_enabled`
- Session `committed_read_group_id`

The existing `kafka.consumer-group-id` property remains unchanged and is retained for current branch behavior. In committed-read mode it is used only as the last fallback source of a group ID.

Effective committed-read mode resolution:

1. session `committed_read_enabled`, if set
2. otherwise catalog `kafka.committed-read-enabled`

Effective committed-read group ID resolution:

1. session `committed_read_group_id`, if set
2. otherwise catalog `kafka.committed-read-group-id`, if set
3. otherwise existing `kafka.consumer-group-id`

Rules:

- blank group IDs are invalid
- if committed-read mode is enabled and the effective group ID is blank or absent, fail the query
- the new committed-read group ID properties do not alter default-mode behavior

### 2. Missing or Invalid Committed Offset Policy

Add catalog property `kafka.committed-read-missing-offset-policy` with enum values:

- `EARLIEST`: start from the current partition beginning offset
- `LATEST`: start from the current partition end offset
- `ERROR`: fail the query

Default value: `ERROR`

This policy applies both when:

- no committed offset exists for the effective group ID
- the committed offset is invalid for the current partition snapshot

For this feature, an invalid committed offset is any committed offset outside the current `[logStart, logEnd]` snapshot range for the partition.

### 3. Exact Start and End Offset Resolution

For each partition in committed-read mode:

- `logStart` = current broker beginning offset snapshot
- `logEnd` = current broker end offset snapshot
- `filteredBegin` and `filteredEnd` = the existing pushdown-adjusted bounds after applying current offset/timestamp/partition filters
- `committedBase` = valid committed offset if present, otherwise the result of the missing-offset policy

The final split range is:

- `start = min(filteredEnd, max(filteredBegin, committedBase))`
- `end = filteredEnd`

Implications:

- pushed-down lower and upper bounds always apply
- committed offsets never bypass query predicates
- if `start == end`, the partition produces no split and no commit
- the connector does not create empty committed-read splits

### 4. Commit Semantics

The connector cannot observe whole-query success from the record cursor path, so committed-read mode is defined in terms of source split exhaustion, not query success.

For a committed-read split:

- commit only if the cursor naturally exhausts its planned `[start, end)` range
- commit only from cursor close after the split is marked fully consumed
- commit the exact next offset to read: `split.messagesRange.end()`
- never derive the commit target from `consumer.position()`
- do not commit on early close, cancellation, `LIMIT` short-circuit, or local read failure

This contract avoids partial-consumption commits, but it does not provide whole-query atomicity.

### 5. Failure Semantics

Committed-offset lookup failures are fatal:

- if committed offsets cannot be fetched from Kafka, fail the query
- if commit fails, fail the query
- do not silently downgrade to best-effort progression

## Architectural Decisions

### 1. Keep the Feature Plugin-Local

The chosen scope fits inside the Kafka connector by changing:

- configuration and session properties
- split planning
- committed-offset lookup
- consumer startup position selection
- consumer close/commit behavior
- tests and docs

We are explicitly avoiding designs that require worker-to-coordinator reporting of read completion state across many splits per partition.

### 2. One Split Per Partition in Committed-Read Mode

This is the key simplification that makes offset commits implementable.

Reasoning:

- Kafka stores one committed offset per topic-partition per group
- the current connector can create many Trino splits per partition
- committing progress from many parallel splits against one Kafka partition would create offset races

Trade-off:

- reduced intra-partition parallelism
- potentially longer-running partition scans
- behavior is acceptable because it is opt-in and preserves default mode

### 3. Manual Assignment Stays

The design continues to use `assign(...)`, not `subscribe(...)`.

Implications:

- the group ID is used only as an offset namespace and commit identity
- Kafka ownership coordination and rebalancing are not introduced
- the connector does not gain exclusive ownership of a topic-partition

### 4. Supported Usage Shape

Committed-read mode is intended for tracked-progress consumption where the effective group ID is dedicated to that read path.

Supported shape:

- one tracked scan per group/topic at a time

Unsafe shapes that must be documented as unsupported:

- concurrent Trino queries using the same effective group ID on the same topic
- self-joins or repeated scans of the same tracked topic within one query when they share a group ID
- external Kafka consumers using the same group ID while Trino committed-read mode is active

## Implementation Plan

## Phase 1: Configuration and Session Properties

### 1.1 Extend `KafkaConfig`

Add new catalog-level properties:

- `kafka.committed-read-enabled`
- `kafka.committed-read-group-id`
- `kafka.committed-read-missing-offset-policy`

Keep current behavior as the default:

- `kafka.committed-read-enabled=false`
- `kafka.consumer-group-id` remains unchanged
- `kafka.committed-read-group-id` is optional
- `kafka.committed-read-missing-offset-policy=ERROR`

### 1.2 Extend `KafkaSessionProperties`

Add session properties:

- `committed_read_enabled`
- `committed_read_group_id`

Rules:

- the session group ID property is nullable/optional
- blank values are rejected
- session override applies only to committed-read mode

### 1.3 Define Effective Settings Helpers

Add connector-local helper methods to resolve:

- whether committed-read mode is active for the current session
- the effective committed-read group ID for the current session
- the effective missing-offset policy

These helpers must be used consistently across split planning and consumer construction.

### 1.4 Reconcile Existing Config/Test Mismatch First

Before feature work starts, reconcile the branch state around `kafka.consumer-group-id`:

- keep the existing property intact
- make the new committed-read properties additive rather than renaming the existing one
- update config tests and docs so the current branch state and the planned committed-read behavior are no longer ambiguous

## Phase 2: Split Planning Changes

### 2.1 Update `KafkaSplitManager`

When committed-read mode is disabled:

- preserve the current split planning behavior exactly

When committed-read mode is enabled:

- enumerate partitions as today
- fetch `logStart` and `logEnd` as today
- apply existing pushdown logic to get `filteredBegin` and `filteredEnd`
- look up committed offsets for the effective committed-read group ID
- resolve final `[start, end)` using the exact formula defined above
- plan one split per partition only when `start < end`

### 2.2 Fetch Committed Offsets from Kafka Admin APIs

During split planning, fetch committed offsets using Kafka admin APIs for the effective group ID:

- prefer `Admin.listConsumerGroupOffsets(groupId)` for broker-stored offset lookup
- do committed-offset lookup once per query planning pass, not once per cursor

This keeps committed-offset resolution explicit and independent from the polling consumer state.

### 2.3 Handle Missing and Invalid Offsets Explicitly

For each partition:

- if no committed offset exists, apply the configured missing-offset policy
- if the committed offset is outside `[logStart, logEnd]`, treat it as invalid and apply the same policy
- if the policy is `ERROR`, fail the query with a message that identifies the topic, partition, group ID, and invalid or missing condition

## Phase 3: Consumer Startup and Commit Behavior

### 3.1 Keep Consumer Construction Session-Aware

The consumer factory must honor the effective committed-read group ID for the current session when committed-read mode is enabled.

Rules:

- in default mode, preserve current consumer construction behavior
- in committed-read mode, apply the effective committed-read group ID at runtime

### 3.2 Start Reads from the Planned Offset

In committed-read mode:

- keep using manual `assign(...)`
- `seek(...)` to the planned split start offset

The planned split offset already encodes committed-offset resume behavior plus predicate bounds.

### 3.3 Commit After Source Split Exhaustion

Implement split-local commit semantics in the cursor lifecycle:

- track whether the split was fully consumed
- set `fullyConsumed=true` only when the cursor naturally exhausts the planned range
- track local failure state so exceptions suppress commit
- issue the commit from `close()` only when `fullyConsumed && !failed`

This is intentionally defined as source split exhaustion, not query success.

### 3.4 Commit the Exact Split End Offset

Use Kafka commit APIs in the conventional way:

- commit the next offset to read, not the last emitted offset
- commit `split.messagesRange.end()` exactly
- commit only the single topic-partition being scanned
- use synchronous commit so failures are surfaced to the query

Do not:

- commit `consumer.position()`
- commit from partial-consumption paths
- swallow commit failures

## Phase 4: Failure and Concurrency Rules

### 4.1 Expected Guarantees

The feature provides:

- persisted per-partition progress after source split exhaustion
- resume from committed offsets on subsequent committed-read runs
- no commit on early close or partial split consumption

### 4.2 Explicit Non-Guarantees

The feature does not claim:

- exactly-once query semantics
- whole-query atomicity of commits
- commit-on-query-success semantics
- safe concurrent use of the same group ID on the same tracked topic

Important consequence:

- a partition may commit progress after its split is fully consumed even if the overall query later fails elsewhere

### 4.3 Documentation Caveats

Document that:

- committed-read mode is intended for dedicated tracked-progress use cases
- ad hoc historical reads should use default mode or a different group ID
- concurrent queries with the same effective group ID may overwrite each other's progress
- self-joins and repeated scans of the same tracked topic are unsafe if they share a group ID
- messages appended after split planning are outside the query snapshot and are not committed by that query

## Phase 5: Test Plan

## 5.1 Test Strategy

Use integration tests backed by `TestingKafka` and verify commits directly against Kafka.

Preferred verification method:

- Kafka `Admin.listConsumerGroupOffsets(groupId)`

Why this method:

- asserts broker-stored offsets directly
- less brittle than CLI parsing
- aligns with how Kafka exposes committed group offsets

### 5.2 Test Harness Support

Add a reusable Kafka-side assertion helper in `plugin/trino-kafka` tests that can:

- fetch committed offsets for a group ID
- fetch beginning and end offsets for a partition
- create deterministic assertions around missing, invalid, and advanced offsets

### 5.3 Core Test Cases

#### Test 1: No Commit in Default Mode

- create a topic with known data
- run a read in default mode
- assert no committed offset exists for the test group

#### Test 2: Commit Happens After Full Split Exhaustion

- create a dedicated topic
- use a dedicated committed-read group ID
- run a deterministic committed-read query that fully exhausts the split
- assert Kafka stores the exact split end offset for that group

#### Test 3: No Commit on Early Close

- create a dedicated topic
- run `SELECT ... LIMIT 1` in committed-read mode
- assert no committed offset was written

This is the primary negative-path test because it deterministically exercises partial consumption.

#### Test 4: Resume from Committed Offset

- run the first query in committed-read mode
- verify committed offsets were advanced
- produce more data
- run the second query with the same group ID
- verify the second read resumes from the committed offsets rather than rereading old data

#### Test 5: Session Group ID Override Wins Over Catalog Defaults

- configure catalog-level committed-read settings
- set a session-level committed-read group ID override
- run a read
- assert commits appear under the session group ID, not the catalog default or legacy `kafka.consumer-group-id`

#### Test 6: One Non-Empty Split Per Partition in Committed-Read Mode

- create a multi-partition topic
- enable committed-read mode
- verify planning behavior or observable results consistent with one non-empty split per partition

#### Test 7: Missing Offset Policy `EARLIEST`

- use a fresh group ID with no committed offsets
- configure `EARLIEST`
- verify the first run starts from the partition beginning offset

#### Test 8: Missing Offset Policy `LATEST`

- use a fresh group ID with no committed offsets
- configure `LATEST`
- verify the first run starts from the partition end snapshot and reads nothing historical

#### Test 9: Missing Offset Policy `ERROR`

- use a fresh group ID with no committed offsets
- configure `ERROR`
- assert the query fails deterministically

#### Test 10: Stale or Invalid Committed Offset Handling

- create a committed offset that falls outside the current `[logStart, logEnd]` range
- verify the configured missing-offset policy is applied

#### Test 11: Commit Failure Propagates

- add a focused unit or narrowly scoped connector test around the commit path
- force commit failure deterministically
- assert the failure surfaces to the query path rather than being swallowed

### 5.4 Determinism Guidelines

To keep tests stable:

- use dedicated topics per test
- use dedicated group IDs per test
- prefer single-partition topics for commit and resume tests
- use known message counts and assert exact next offsets
- use `LIMIT 1` rather than synthetic mid-query failures for the primary no-commit negative case

## Phase 6: Documentation Updates

Update Kafka connector docs to describe:

- the new catalog properties
- the new session properties
- the precedence rules for effective committed-read group ID resolution
- default mode versus committed-read mode
- one-split-per-partition behavior in committed-read mode
- exact start-offset resolution semantics
- split-exhaustion commit semantics
- failure behavior when lookup or commit fails
- limitations and concurrency caveats

## Proposed Implementation Order

1. Reconcile current `kafka.consumer-group-id` config/test/doc state
2. Add committed-read config model and session-property model
3. Add effective settings resolution helpers
4. Add committed-offset lookup and exact split-range resolution
5. Update split planning to one non-empty split per partition in committed-read mode
6. Add split-local exhaustion tracking and exact end-offset commit behavior
7. Add failure propagation for committed-offset lookup and commit
8. Add Kafka-side test helpers and deterministic tests
9. Update connector documentation

## Acceptance Criteria

The feature is ready for implementation review when all of the following are true:

- default connector behavior remains unchanged
- existing `kafka.consumer-group-id` behavior remains intact and is only used as the last fallback group ID source in committed-read mode
- committed-read mode is opt-in
- the effective committed-read group ID can be changed at session scope
- committed-read mode plans one non-empty split per partition
- start-offset resolution follows the explicit formula in this plan
- missing and invalid committed offsets follow the configured policy
- full source split exhaustion commits the exact split end offset visible from Kafka
- early close and partial consumption do not commit
- committed-offset lookup failures and commit failures fail the query
- rerunning with the same group resumes from committed offsets
- docs explain the feature and its limitations clearly
