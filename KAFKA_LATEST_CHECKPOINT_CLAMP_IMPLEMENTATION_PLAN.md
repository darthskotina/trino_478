# Kafka LATEST Checkpoint Clamp Implementation Plan

## Objective

Fix committed-read split planning for the `LATEST` missing-offset policy so that
checkpoint-only splits commit a broker-valid offset within the current Kafka
snapshot, even when `filteredEnd` falls below `logStart`.

This plan is intentionally implementation-focused. It does not include code
changes.

## Problem Summary

Current committed-read split planning can create a checkpoint-only split for the
`LATEST` policy using raw `filteredEnd` as both:

- the empty split range position
- the Kafka commit target

That is unsafe when topic retention has advanced the broker beginning offset:

- `filteredEnd` may be lower than the current `logStart`
- the planned checkpoint target may therefore be outside the valid broker range
- the connector may attempt to commit an invalid offset instead of a clamped
  empty-range checkpoint

## Desired Behavior

Keep existing `filteredEnd` logic and existing normal data-split semantics.

Only change the `LATEST` checkpoint-only branch in committed-read split
planning.

Required semantics:

- normal data split:
  - `start = min(filteredEnd, committedBase)`
  - `end = filteredEnd`
- `LATEST` checkpoint-only split:
  - `checkpointTarget = min(logEnd, max(logStart, filteredEnd))`
  - `start = checkpointTarget`
  - `end = checkpointTarget`
  - `commitTarget = checkpointTarget`

This is a narrow fix:

- no new connector state
- no SPI changes
- no worker/coordinator protocol changes
- no change to worker-side commit logic for normal data splits

## Scope

### In Scope

- Localized split-planning change in the Kafka connector
- Test coverage for clamped checkpoint-only `LATEST` behavior
- Small doc update where the behavior is described

### Out of Scope

- Redesigning committed-read mode
- Changing `KafkaFilterManager` pushdown semantics
- Changing worker-side record cursor behavior for normal splits
- Broad end-range clamping for all committed-read scans

## Implementation Strategy

### 1. Keep the Fix in `KafkaSplitManager`

Primary file:

- `plugin/trino-kafka/src/main/java/io/trino/plugin/kafka/KafkaSplitManager.java`

Reasoning:

- the bug is in split planning, not in execution
- `KafkaSplitManager` already has all required inputs:
  - `logStart`
  - `logEnd`
  - `filteredEnd`
  - missing-offset policy
- this avoids touching cursor logic, split metadata shape, or connector state

### 2. Do Not Change `KafkaFilterManager`

Relevant file:

- `plugin/trino-kafka/src/main/java/io/trino/plugin/kafka/KafkaFilterManager.java`

Reasoning:

- `filteredEnd` is still a useful logical bound
- the defect is not that filtering produced `filteredEnd < logStart`
- the defect is treating that raw value as a Kafka checkpoint target
- clamping in filtering would broaden scope and risk changing general scan
  behavior

### 3. Clamp Only the `LATEST` Checkpoint-Only Path

Do not clamp normal committed-read data split `end`.

Do not change:

- `start = min(filteredEnd, committedBase)`
- `end = filteredEnd`
- normal data split commit target semantics

Only clamp the checkpoint-only target for `LATEST`.

Reasoning:

- this is the narrowest valid fix
- the branch intent was to clamp the empty-range checkpoint position, not to
  redefine all read bounds

## Detailed Change Plan

### Step 1: Inspect `planCommittedReadSplit(...)`

Relevant method:

- `KafkaSplitManager.planCommittedReadSplit(...)`

Review the existing flow carefully:

- committed offset resolution
- current `start` and `end` calculation
- data-split branch
- checkpoint-only `LATEST` branch

Confirm exactly where raw `filteredEnd` or derived `start` is currently reused
as the checkpoint target.

### Step 2: Add a Small Helper for Checkpoint Clamping

Recommended approach:

- add a private helper in `KafkaSplitManager`
- keep it close to `applyMissingOffsetPolicy(...)`
- make the helper deterministic and self-contained

Suggested helper naming:

- `clampCheckpointTarget`

Suggested contract:

- input:
  - `logStart`
  - `logEnd`
  - `filteredEnd`
- output:
  - a valid Kafka offset within `[logStart, logEnd]`

Suggested implementation formula:

- `min(logEnd, max(logStart, filteredEnd))`

Guidelines:

- do not make the helper generic across unrelated split-planning paths
- do not expose it outside the class
- keep it obvious that this is for checkpoint-only planning

### Step 3: Update the `LATEST` Checkpoint-Only Branch

In `planCommittedReadSplit(...)`:

- keep existing normal data split logic unchanged
- when entering the `LATEST` checkpoint-only path:
  - compute `checkpointTarget` with the clamp helper
  - create `Range(checkpointTarget, checkpointTarget)`
  - create `KafkaCommittedReadSplitMetadata(groupId, true, checkpointTarget)`

Important:

- do not continue using derived `start` implicitly as the checkpoint target
- compute a clearly named `checkpointTarget` explicitly

Reasoning:

- this keeps the semantics readable
- it avoids future regressions if the surrounding `start` logic changes

### Step 4: Preserve Existing Data-Split Semantics

The implementing agent must verify that the fix does not alter non-empty split
planning.

Preserve:

- valid committed offsets
- `EARLIEST` handling
- `ERROR` handling
- data split commit targets

No broad clamping should be introduced outside the checkpoint-only `LATEST`
branch.

### Step 5: Keep Split Metadata Shape Unchanged

Relevant file:

- `plugin/trino-kafka/src/main/java/io/trino/plugin/kafka/KafkaCommittedReadSplitMetadata.java`

No structural changes are needed.

Only semantic change:

- for checkpoint-only `LATEST` splits, `commitTarget` must always be within the
  broker snapshot

## Test Plan

Primary test target:

- `plugin/trino-kafka/src/test/java/io/trino/plugin/kafka/TestKafkaCommittedReadSplitManager.java`

Secondary test target:

- `plugin/trino-kafka/src/test/java/io/trino/plugin/kafka/TestKafkaCommittedReadMode.java`

The most important thing is to add deterministic coverage for the broken edge
case where `filteredEnd < logStart`.

### Must-Have Test 1: `filteredEnd < logStart` Clamps to `logStart`

Purpose:

- prove the bug is fixed in split planning

Setup:

- create a topic with data
- create a state where the broker snapshot has `logStart > 0`
  deterministically via Kafka admin `deleteRecords(...)`
- use committed-read mode with missing-offset policy `LATEST`
- apply an upper-bound predicate that yields `filteredEnd < logStart`

Recommended deterministic setup:

- create a `TopicPartition`
- call `Admin.deleteRecords(Map.of(topicPartition, RecordsToDelete.beforeOffset(targetOffset)))`
- wait until the broker low watermark / beginning offset reaches `targetOffset`
- use that `targetOffset` as the expected `logStart`

Assertions:

- exactly one checkpoint-only split is planned
- split metadata is present
- `checkpointOnly == true`
- `range.begin == range.end`
- `range.begin == logStart`
- `commitTarget == logStart`

Notes for implementation:

- prefer Kafka admin `deleteRecords(...)` over time-based retention waiting
- the current repo does not expose a dedicated `TestingKafka` helper for this,
  but the test classpath already includes Kafka admin APIs, so the test can do
  this directly or add a small helper to `TestingKafka`

### Must-Have Test 2: In-Range `LATEST` Checkpoint Behavior Stays the Same

Strengthen the existing `LATEST` checkpoint test so that it clearly proves:

- when `filteredEnd` is inside `[logStart, logEnd]`
- the checkpoint target remains exactly `filteredEnd`

Assertions:

- checkpoint-only split is created
- `range.begin == range.end == filteredEnd`
- `commitTarget == filteredEnd`

Purpose:

- prevent accidental semantic broadening

### Must-Have Test 3: Invalid Committed Offset with `LATEST` Uses Clamped Checkpoint

Purpose:

- validate the production path where a committed offset exists but is outside
  the current broker range

Suggested setup:

- create a topic with data
- deterministically advance `logStart` above an existing committed offset using
  Kafka admin `deleteRecords(...)`
- create or alter the group state so the group still has a committed offset
  below the new `logStart`
- use committed-read mode with missing-offset policy `LATEST`

Assertions:

- checkpoint-only split is planned
- `checkpointOnly == true`
- `range.begin == range.end == logStart`
- `commitTarget == logStart`

Purpose for inclusion as must-have:

- `KafkaSplitManager` uses the same `missingOrInvalid()` branch for both missing
  and invalid committed offsets, so regression coverage must include both cases

### Must-Have Test 4: Normal Data Split Behavior Is Unchanged

Add or refine a test showing that the fix does not affect non-empty data split
planning.

Suggested setup:

- valid committed offset or `EARLIEST` policy
- `filteredEnd` inside broker range
- committed-read mode produces a data-carrying split

Assertions:

- `checkpointOnly == false`
- `start = min(filteredEnd, committedBase)`
- `end = filteredEnd`
- commit target remains the planned `end`

Purpose:

- ensure the fix stays narrow

### Must-Have Test 5: Integration-Level Commit Assertion if Feasible

Relevant file:

- `plugin/trino-kafka/src/test/java/io/trino/plugin/kafka/TestKafkaCommittedReadMode.java`

If feasible without making the test brittle:

- run a committed-read query under `LATEST`
- ensure no rows are read
- assert the actual committed Kafka offset equals the clamped checkpoint target

If that is difficult to set up deterministically:

- prioritize split-manager coverage first
- document that the planning test is the primary regression guard

## Nice-to-Have Tests

### Boundary Tests

Add direct coverage for:

- `filteredEnd == logStart`
- `filteredEnd == logEnd`

Purpose:

- prevent off-by-one errors

### Defensive Clamp Test for `filteredEnd > logEnd`

Even if current planning usually bounds `filteredEnd` by end offsets, a helper
test can still verify:

- `checkpointTarget` never exceeds `logEnd`

This is optional but useful if the helper is explicit enough to justify focused
testing.

## Test Design Guidance

Prefer deterministic planning assertions over fragile retention-timing tests.

Order of preference:

1. split-manager test with controlled setup
2. connector integration test with `TestingKafka`
3. avoid long waits or timing-sensitive retention behavior unless unavoidable

For any test that needs `logStart > 0`, use Kafka admin `deleteRecords(...)`
to advance the broker low watermark instead of relying on retention.

When adding assertions, do not stop at `checkpointOnly == true`.

Always assert:

- split range begin
- split range end
- commit target

## Documentation Updates

Primary docs to update:

- `plugin/trino-kafka/README.md`
- `docs/src/main/sphinx/connector/kafka.md`

Documentation ownership note:

- `plugin/trino-kafka/README.md` already documents committed-read mode and
  `LATEST` checkpoint-only behavior, so it is the minimum required doc target
- update `docs/src/main/sphinx/connector/kafka.md` only if committed-read mode
  is intentionally being documented in the public connector docs as part of the
  same implementation pass

Minimum required documentation change:

- describe that `LATEST` checkpoint-only initialization commits a clamped
  empty-range position within the broker snapshot
- clarify that this is not raw `filteredEnd` when pushed-down bounds fall
  outside retained data

Suggested wording theme:

- when `LATEST` produces a checkpoint-only split, the committed offset is
  clamped into the current broker snapshot `[logStart, logEnd]` before being
  persisted

If only one doc is updated in the implementation pass, prefer the official
connector docs:

- `docs/src/main/sphinx/connector/kafka.md`

## Risks and Pitfalls

### 1. Accidentally Clamping Normal Data Split `end`

Avoid this. It would broaden the fix and change more semantics than required.

### 2. Reusing `start` as an Implicit Checkpoint Target

Avoid deriving checkpoint behavior indirectly from `start`.

Instead:

- compute a dedicated `checkpointTarget`
- use it explicitly for both empty split range and commit target

### 3. Off-by-One Mistakes

Remember:

- Kafka ranges are `[begin, end)`
- checkpoint-only split means `begin == end`
- commit target is the next offset to read
- no `+1` or `-1` should be introduced in the clamp logic

### 4. Flaky Retention-Based Tests

If retention manipulation depends on timing, tests may become unstable.

Prefer:

- controlled planning assertions
- topic-local, deterministic setup

### 5. Incomplete Assertions

Do not only assert that a split exists.

Also assert:

- whether it is checkpoint-only
- exact split range
- exact commit target

## Acceptance Criteria

The implementation is ready for review when all of the following are true:

- `LATEST` checkpoint-only planning clamps the checkpoint target into
  `[logStart, logEnd]`
- checkpoint-only split range is exactly the clamped target
- checkpoint-only metadata commit target is exactly the clamped target
- normal committed-read data split planning remains unchanged
- there is a regression test covering `filteredEnd < logStart`
- there is a regression test covering an invalid committed offset under
  `LATEST`
- the simple in-range `LATEST` checkpoint case still passes
- docs mention clamped checkpoint-only `LATEST` behavior

## Suggested Implementation Order

1. Inspect and update `KafkaSplitManager` split planning logic
2. Add or refine split-manager tests for the clamped checkpoint target
3. Add integration-level assertion only if it can be done deterministically
4. Update Kafka connector docs
5. Run targeted Kafka connector tests

## Suggested Test Commands

Narrow validation:

```bash
./mvnw -pl plugin/trino-kafka -Dtest=TestKafkaCommittedReadSplitManager test
```

Broader targeted validation:

```bash
./mvnw -pl plugin/trino-kafka -Dtest=TestKafkaCommittedReadSplitManager,TestKafkaCommittedReadMode,TestKafkaRecordSetCommittedRead,TestKafkaConfig test
```

## Handoff Notes

Key decision to preserve:

- clamp only the `LATEST` checkpoint-only path
- do not broaden the fix into general committed-read end-range clamping

Files to inspect before implementing:

- `plugin/trino-kafka/src/main/java/io/trino/plugin/kafka/KafkaSplitManager.java`
- `plugin/trino-kafka/src/test/java/io/trino/plugin/kafka/TestKafkaCommittedReadSplitManager.java`
- `plugin/trino-kafka/src/test/java/io/trino/plugin/kafka/TestKafkaCommittedReadMode.java`
- `plugin/trino-kafka/README.md`
- `docs/src/main/sphinx/connector/kafka.md`

If only one new regression test is added, it must be the explicit
`filteredEnd < logStart` clamp case. That is the core defect this plan is meant
to eliminate.
