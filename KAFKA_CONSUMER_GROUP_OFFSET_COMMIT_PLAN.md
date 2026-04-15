# Kafka Consumer Group Offset Commit Plan

## Objective

Add an opt-in committed-read mode to the Trino Kafka connector that:

- reads from Kafka consumer-group committed offsets
- processes at most one Trino split per Kafka partition in that mode, with an optional checkpoint-only split for `LATEST` initialization
- commits the next safe offset after source split exhaustion
- allows the committed-read consumer group ID to be overridden at session scope
- preserves current behavior as the default mode

This plan is intentionally limited to implementation planning. It does not include code changes.

## Agreed Scope

### In Scope

- Connector-local implementation in the Kafka plugin
- Opt-in committed-read mode
- One data split per Kafka partition when committed-read mode is enabled, plus an optional checkpoint-only split for `LATEST` initialization
- Start position resolution from Kafka committed offsets for the effective committed-read group ID
- Commit of the next offset after source split exhaustion
- Explicit split metadata for checkpoint-only committed-read execution
- Catalog properties for committed-read mode
- Session properties for per-query/per-session committed-read mode and required group ID override
- Tests that verify offsets were committed on Kafka's side
- Documentation updates for the new behavior and caveats, including a modified-connector README or equivalent operator-facing document

### Out of Scope

- Whole-query-atomic offset commits
- Query-success-only commits
- Kafka `subscribe(...)` / rebalance-based group management
- Multi-split-per-partition ordered checkpointing
- Backward reads in committed-read mode as a first-class feature
- Kafka transactional visibility semantics such as `isolation.level=read_committed`
- Any Trino SPI or engine changes outside the Kafka connector unless an unexpected blocker is discovered

## Current Behavior Summary

The connector currently behaves like a bounded table scan over Kafka:

- `KafkaSplitManager` plans offset ranges from `beginningOffsets(...)` and `endOffsets(...)`
- `KafkaRecordSet` reads each split using manual `assign(...)` and `seek(...)`
- consumers are created with `enable.auto.commit=false`
- no `commitSync(...)`, `commitAsync(...)`, or committed-offset resume path exists

There is already a `kafka.consumer-group-id` catalog property in the plugin, but today it does not produce committed-read semantics on reads.

The current branch also gives `kafka.consumer-group-id` a hardcoded default value in `KafkaConfig`. That legacy default must not be used as an implicit committed-read group.

## Intended Semantics

### Default Mode

Default behavior remains unchanged:

- split planning continues to use current offset-range logic
- multiple splits per partition remain allowed
- reads continue to ignore committed group offsets
- no offsets are committed

### Committed-Read Mode

When committed-read mode is enabled:

- the connector plans at most one data-carrying split per Kafka partition
- the scan start offset is derived from the effective committed-read group ID and the current bounded query snapshot
- the scan end offset remains the partition end snapshot determined during split planning
- committed progress is defined relative to the query's effective pushed-down bounds
- lower-bound predicates on `_partition_offset` and `_timestamp` may intentionally advance the committed resume position past earlier unread records for that group
- messages produced after split planning are not read or committed by that query
- the connector commits the next offset to read after source split exhaustion

When the effective committed offset is missing or invalid and the configured policy is `LATEST`, the connector may also plan a checkpoint-only split that emits no rows and exists only to persist the resolved initial resume point after applying the same pushed-down bounds.

This is a best-effort per-split completion model, not a whole-query commit barrier.

## Semantic Contract

### 1. Effective Settings

Committed-read mode uses the following properties:

- Catalog `kafka.committed-read-enabled`
- Catalog `kafka.committed-read-missing-offset-policy`
- Session `committed_read_enabled`
- Session `committed_read_group_id`

The existing `kafka.consumer-group-id` property remains unchanged for legacy/default-mode consumer construction, but it is not consulted in committed-read mode.

Effective committed-read mode resolution:

1. session `committed_read_enabled`, if set
2. otherwise catalog `kafka.committed-read-enabled`

Effective committed-read group ID resolution:

1. session `committed_read_group_id`, if set
2. otherwise fail the query

Rules:

- blank group IDs are invalid
- if committed-read mode is enabled and the session `committed_read_group_id` is blank or absent, fail the query
- the committed-read group ID must be explicitly provided at session scope
- the legacy `kafka.consumer-group-id` property must never be used as a fallback committed-read group
- the committed-read group ID properties do not alter default-mode behavior

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
- `checkpointCommitTarget` = defined only for checkpoint-only `LATEST` initialization, and equals the final clamped empty-range position after applying the same pushed-down bounds; in that branch `start == end == checkpointCommitTarget`

The final split range is:

- `start = min(filteredEnd, max(filteredBegin, committedBase))`
- `end = filteredEnd`

Implications:

- pushed-down lower and upper bounds always apply
- committed offsets never bypass query predicates
- committed-read mode tracks progress relative to the effective pushed-down bounds of the query
- lower-bound predicates on `_partition_offset` and `_timestamp` may advance committed progress past earlier unread records; this is intentional for tracked filtered-consumption use cases
- the planner must not generate a split that rewinds a valid existing committed offset solely because a query's filtered end is lower than that committed offset
- if `start == end` because the partition already has a valid committed offset at or beyond `end`, the partition produces no split and no commit
- if `start == end` because the committed offset is missing or invalid and the policy is `LATEST`, the connector plans a checkpoint-only split that emits no rows and commits the resolved clamped `checkpointCommitTarget`, not raw `committedBase`
- for other `start == end` cases, the partition produces no split and no commit

### 4. Commit Semantics

The connector cannot observe whole-query success from the record cursor path, so committed-read mode is defined in terms of source split exhaustion, not query success.

For a committed-read split:

- commit only if the cursor naturally exhausts its planned `[start, end)` range
- commit only from cursor close after the split is marked fully consumed
- commit the exact next offset to read for a data-carrying split: `split.messagesRange.end()`
- commit the resolved clamped checkpoint target for a checkpoint-only `LATEST` initialization split
- never derive the commit target from `consumer.position()`
- do not commit on early close or local read failure before the connector has locally exhausted the split
- query-level short-circuit such as `LIMIT` may still allow Trino to drain a small split into a page before the query stops; if local split exhaustion has already happened, commit is expected
- do not commit from partial-consumption paths that stop before local split exhaustion
- same-group concurrent writers remain last-writer-wins at Kafka's offset store, so no hard guarantee is made against backward movement under unsupported concurrent use; at most, the connector may perform a best-effort stale-state check before commit

This contract avoids partial-consumption commits, but it does not provide whole-query atomicity.

### 5. Failure Semantics

Committed-offset lookup failures are fatal:

- if committed offsets cannot be fetched from Kafka, fail the query
- if commit fails, fail the query
- if a planned start offset becomes invalid before execution and the worker hits `OffsetOutOfRangeException` or equivalent runtime invalidation, fail the query and suppress commit for that split
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
- a checkpoint-only split is acceptable for `LATEST` initialization because it still preserves the one-split-per-partition write discipline when represented explicitly rather than inferred from an empty range

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
- concurrent Trino queries using the same group ID are not coordinated through Kafka consumer-group membership
- same-group concurrent writers are effectively last-writer-wins on the stored offsets

### 4. Why Group ID Override Stays Scoped to Committed-Read Mode

Default mode continues to behave like the current bounded table scan:

- split planning still starts from connector-planned offsets, not broker-stored committed offsets
- the connector still uses manual `assign(...)` and explicit `seek(...)`
- a group ID in default mode is therefore not a resume control and does not prevent reading offsets earlier than that group's currently committed offset

Because of that, session-scoped group override is intentionally meaningful only in committed-read mode. Exposing a general group override in default mode would be technically possible, but it would be mostly connector identity metadata rather than committed-read behavior.

### 5. Supported Usage Shape

Committed-read mode is intended for tracked-progress consumption where the effective group ID is dedicated to that read path.

Supported shape:

- one tracked scan per group/topic at a time

Unsafe shapes that must be documented as unsupported:

- concurrent Trino queries using the same effective group ID on the same topic
- self-joins or repeated scans of the same tracked topic within one query when they share a group ID
- external Kafka consumers using the same group ID while Trino committed-read mode is active
- any workload that assumes Kafka consumer-group rebalancing or partition ownership enforcement
- workloads that require transactional visibility semantics from Kafka; this feature intentionally does not change `isolation.level`

## Implementation Plan

## Phase 1: Configuration and Session Properties

### 1.1 Extend `KafkaConfig`

Add new catalog-level properties:

- `kafka.committed-read-enabled`
- `kafka.committed-read-missing-offset-policy`

Keep current behavior as the default:

- `kafka.committed-read-enabled=false`
- `kafka.consumer-group-id` remains unchanged
- `kafka.committed-read-missing-offset-policy=ERROR`

### 1.2 Extend `KafkaSessionProperties`

Add session properties:

- `committed_read_enabled`
- `committed_read_group_id`

Rules:

- the session group ID property is required whenever committed-read mode is enabled
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
- keep it out of committed-read group resolution entirely
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
- plan one data split per partition only when `start < end`
- if `start == end` and the offset state was missing or invalid and the policy is `LATEST`, plan a checkpoint-only split carrying the explicit clamped `checkpointCommitTarget`

### 2.1.1 Extend `KafkaSplit` for Explicit Checkpoint Metadata

Do not infer checkpoint-only behavior from `messagesRange.begin() == messagesRange.end()`.

Extend `KafkaSplit` with explicit committed-read execution metadata, at minimum:

- whether the split is checkpoint-only
- the explicit checkpoint commit target for checkpoint-only splits

This metadata is connector-local and keeps the worker-side read path simple and unambiguous.

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
- if the policy is `LATEST` and the partition resolves to an empty range, distinguish between durable initialization and no-op cases:
  - missing/invalid offset plus `LATEST`: checkpoint-only split whose explicit commit target is the final clamped empty-range position, not raw `logEnd`
  - already valid committed offset at or beyond `end`: no split and no commit

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

For a checkpoint-only `LATEST` initialization split:

- assign the partition as usual
- do not emit rows
- treat the split as locally exhausted immediately
- commit the split's explicit checkpoint target on close

### 3.3 Commit After Source Split Exhaustion

Implement split-local commit semantics in the cursor lifecycle:

- track whether the split was fully consumed
- set `fullyConsumed=true` only when the cursor naturally exhausts the planned range
- track local failure state so exceptions suppress commit
- issue the commit from `close()` only when `fullyConsumed && !failed`

Note:

- Trino may still pull an entire small split into a page before a query-level `LIMIT` stops the query
- therefore the contract must be phrased in terms of local split exhaustion, not user-visible query short-circuit alone

This is intentionally defined as source split exhaustion, not query success.

### 3.4 Commit the Exact Split End Offset

Use Kafka commit APIs in the conventional way:

- commit the next offset to read, not the last emitted offset
- commit `split.messagesRange.end()` exactly
- commit only the single topic-partition being scanned
- use synchronous commit so failures are surfaced to the query
- for checkpoint-only `LATEST` initialization splits, commit the explicit clamped checkpoint target instead of `split.messagesRange.end()`

Do not:

- commit `consumer.position()`
- commit raw `committedBase` for checkpoint-only `LATEST` initialization
- commit from partial-consumption paths
- swallow commit failures
- claim a hard no-backwards guarantee under unsupported same-group concurrency

### 3.5 Handle Runtime Offset Invalidation Explicitly

The worker-side read path must treat post-planning offset invalidation as fatal:

- if `seek(...)` or `poll(...)` fails because the planned start offset is no longer valid, fail the split and suppress commit
- do not re-run the missing-offset policy on the worker
- do not silently jump to a new broker offset, because that would change the query snapshot after planning
- surface the failure with topic, partition, group ID, planned start offset, and the runtime error

## Phase 4: Failure and Concurrency Rules

### 4.1 Expected Guarantees

The feature provides:

- persisted per-partition progress after source split exhaustion
- resume from committed offsets on subsequent committed-read runs
- no commit on early close or partial split consumption
- durable initial checkpointing for missing/invalid `LATEST` offsets
- explicit checkpoint-only execution semantics via split metadata rather than empty-range inference

### 4.2 Explicit Non-Guarantees

The feature does not claim:

- exactly-once query semantics
- whole-query atomicity of commits
- commit-on-query-success semantics
- safe concurrent use of the same group ID on the same tracked topic
- Kafka consumer-group membership, partition ownership, or rebalance coordination
- Kafka transactional visibility guarantees such as `isolation.level=read_committed`

Important consequence:

- a partition may commit progress after its split is fully consumed even if the overall query later fails elsewhere
- a small split may still commit even when the query contains `LIMIT`, if Trino has already locally exhausted that split before the query stops
- unsupported same-group concurrency can still overwrite a newer committed offset with an older one because Kafka offset commits are not compare-and-set

### 4.3 Documentation Caveats

Document that:

- committed-read mode is intended for dedicated tracked-progress use cases
- ad hoc historical reads should use default mode or a different group ID
- concurrent queries with the same effective group ID may overwrite each other's progress
- self-joins and repeated scans of the same tracked topic are unsafe if they share a group ID
- messages appended after split planning are outside the query snapshot and are not committed by that query
- default mode can still read offsets earlier than a group's committed offset because it uses explicit planning and `seek(...)`, not committed-read resume behavior
- same-group Trino queries are not coordinated through Kafka consumer-group membership
- `LIMIT` is not a reliable no-commit signal for small committed-read splits
- committed-read mode tracks progress relative to effective pushed-down bounds, so lower-bound predicates on `_partition_offset` and `_timestamp` may intentionally skip earlier unread records for that group
- checkpoint-only `LATEST` initialization commits the final clamped empty-range position rather than raw broker `logEnd`
- this feature does not change Kafka transactional visibility; `isolation.level=read_committed` remains out of scope in this iteration
- these trade-offs must be described in a modified-connector README or equivalent operator-facing document, not only in connector reference docs

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
- create more than one source page worth of rows for the partition so a single `LIMIT 1` page fetch cannot exhaust the split
- run `SELECT ... LIMIT 1` in committed-read mode
- assert no committed offset was written

This is the primary negative-path test only when the split is guaranteed to remain larger than the first source page. A small split is allowed to commit if Trino exhausts it before the `LIMIT` stops the query.

#### Test 4: Resume from Committed Offset

- run the first query in committed-read mode
- verify committed offsets were advanced
- produce more data
- run the second query with the same group ID
- verify the second read resumes from the committed offsets rather than rereading old data

#### Test 5: Session Group ID Controls the Committed-Read Namespace

- enable committed-read mode
- set a session-level committed-read group ID
- run a read
- assert commits appear under the session group ID

#### Test 6: One Data Split Per Partition in Committed-Read Mode

- create a multi-partition topic
- enable committed-read mode
- verify planning behavior or observable results consistent with one data split per partition, plus a checkpoint-only split only when `LATEST` initialization requires it

#### Test 7: Missing Offset Policy `EARLIEST`

- use a fresh group ID with no committed offsets
- configure `EARLIEST`
- verify the first run starts from the partition beginning offset

#### Test 8: Missing Offset Policy `LATEST`

- use a fresh group ID with no committed offsets
- configure `LATEST`
- verify the first run starts from the partition end snapshot and reads nothing historical
- verify Kafka stores the initial checkpoint for that group even when the split emits no rows
- when an upper-bound predicate reduces `filteredEnd` below broker `logEnd`, verify the checkpoint commits the final clamped empty-range position rather than raw `logEnd`

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

#### Test 12: Missing Session Group ID Fails

- enable committed-read mode
- do not set `committed_read_group_id`
- assert the query fails before split planning proceeds

#### Test 13: Legacy `kafka.consumer-group-id` Is Not Used as Committed-Read Fallback

- configure a legacy `kafka.consumer-group-id`
- enable committed-read mode without `committed_read_group_id`
- assert the query fails instead of silently using the legacy group

#### Test 14: Runtime Offset Invalidation Suppresses Commit

- add a focused unit or narrowly scoped connector test around the cursor read path
- simulate `OffsetOutOfRangeException` after planning but before successful exhaustion
- assert the query fails and no commit is written for that split

### 5.4 Determinism Guidelines

To keep tests stable:

- use dedicated topics per test
- use dedicated group IDs per test
- prefer single-partition topics for commit and resume tests
- use known message counts and assert exact next offsets
- when using `LIMIT 1` as a negative-path test, force the split to be larger than one source page
- do not use a small split as evidence that `LIMIT` prevents commits

## Phase 6: Documentation Updates

Update Kafka connector docs to describe:

- the new catalog properties
- the new session properties
- the requirement that committed-read group ID is explicitly set in session scope
- default mode versus committed-read mode
- one-split-per-partition behavior in committed-read mode
- checkpoint-only `LATEST` initialization behavior
- exact start-offset resolution semantics
- split-exhaustion commit semantics
- why `LIMIT` is not a reliable no-commit signal for small splits
- failure behavior when lookup or commit fails
- runtime failure behavior when planned offsets become invalid before execution
- limitations and concurrency caveats
- the accepted lower-bound pushdown trade-off for committed progress
- the explicit exclusion of Kafka transactional visibility semantics such as `read_committed`
- the modified-connector README or equivalent operator-facing document that captures these accepted trade-offs

## Proposed Implementation Order

1. Reconcile current `kafka.consumer-group-id` config/test/doc state
2. Add committed-read config model and session-property model
3. Add effective settings resolution helpers
4. Add committed-offset lookup and exact split-range resolution
5. Extend split metadata for explicit checkpoint-only execution and clamped checkpoint commit targets
6. Update split planning to one data split per partition plus `LATEST` checkpoint-only initialization when required
7. Add split-local exhaustion tracking and exact commit-target behavior
8. Add failure propagation for committed-offset lookup, runtime offset invalidation, and commit
9. Add Kafka-side test helpers and deterministic tests
10. Update connector documentation and modified-connector README

## Acceptance Criteria

The feature is ready for implementation review when all of the following are true:

- default connector behavior remains unchanged
- existing `kafka.consumer-group-id` behavior remains intact for legacy/default-mode behavior and is never used as a committed-read fallback
- committed-read mode is opt-in
- the effective committed-read group ID can be changed at session scope
- committed-read mode requires an explicit session `committed_read_group_id`
- committed-read mode plans one data split per partition, with a checkpoint-only split when needed for missing/invalid `LATEST` initialization
- start-offset resolution follows the explicit formula in this plan
- missing and invalid committed offsets follow the configured policy
- full source split exhaustion commits the exact planned target offset visible from Kafka
- early close and partial consumption do not commit unless the split has already been locally exhausted by the connector
- committed-offset lookup failures and commit failures fail the query
- runtime offset invalidation fails the query and suppresses commit
- rerunning with the same group resumes from committed offsets
- checkpoint-only `LATEST` initialization commits the final clamped empty-range position rather than raw `committedBase` or raw broker `logEnd`
- checkpoint-only behavior is represented explicitly in split metadata rather than inferred from an empty range
- docs and the modified-connector README explain the feature, its intentional lower-bound pushdown trade-off, and its non-goals clearly
