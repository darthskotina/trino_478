# Committed-Read Offset Rewind Implementation Plan

## Goal

Introduce a new Kafka connector session property that keeps the current default
behavior in committed-read mode by default, but can be enabled at runtime to
allow reads from offsets earlier than the consumer group's currently committed
offset.

When the new property allows rewind:

- committed-read queries may read an earlier offset window under the same group
- if the planned split is fully consumed and close-time commit is allowed, the
  connector may commit a new offset that is lower than the group's previously
  stored offset

This plan is intentionally limited to connector-local behavior. It does not try
to add query-global commit guarantees or cross-query coordination.

## Non-Goals

- no change to query-global semantics
- no change to partial-read commit suppression logic
- no change to Kafka broker behavior or consumer-group protocol
- no support for rewinding by `_timestamp` unless explicitly chosen later
- no attempt to serialize concurrent same-group queries

## Current Behavior Summary

The current branch implements committed-read mode as a resume-oriented mode:

- `KafkaSessionProperties` defines `committed_read_enabled` and
  `committed_read_group_id`
- `KafkaFilterManager` rejects lower-bound predicates on `_partition_offset`
  and `_timestamp` in committed-read mode
- `KafkaFilterManager` only pushes down begin offsets in default mode, not in
  committed-read mode
- `KafkaSplitManager` plans committed-read splits from the resolved committed
  offset to the filtered end offset
- `KafkaRecordSet` commits the split's `commitTarget` on close only when the
  split is eligible for commit

The practical consequence is that merely removing the validation would be
insufficient. Rewind support requires committed-read planning to honor filtered
begin offsets, not just filtered end offsets.

## Proposed Feature Contract

Add a new session property:

- name: `committed_read_allow_offset_rewind`
- default: `false`

Behavior:

- `false`: preserve the current behavior
- `true`: allow lower-bound predicates on `_partition_offset` in
  committed-read mode and allow a fully consumed query to commit an earlier
  offset than the group's current stored offset

Important semantic clarification:

- when rewind is enabled and the query supplies an explicit lower-bound
  `_partition_offset` predicate, that predicate is authoritative for choosing
  the planned start offset
- this also applies when the group has no committed offset or its committed
  offset is no longer valid on the broker
- in other words, an explicit rewind request may intentionally override the
  catalog's `kafka.committed-read-missing-offset-policy=LATEST` fallback and
  force a historical read window
- this is the intended recovery workflow for operators who need to reread data
  after the group offset has advanced too far
- when rewind is not explicitly requested, the existing missing-offset-policy
  behavior remains unchanged
- equality is not a special case outside the normal range model:
  `_partition_offset = x` must be planned as the half-open window `[x, x+1)`
  so that it reads exactly offset `x`

Inherited committed-read safety constraints remain in force:

- the existing repeated-scan/self-join guard keyed by
  `(queryId, groupId, topicName)` remains required and unchanged
- committed-read mode, including rewind-enabled mode, still carries the same
  deployment requirement that Trino `retry_policy=NONE`
- rewind is an extension of committed-read semantics, not a separate execution
  mode with weaker safety assumptions

Rationale for the chosen name:

- the default remains conservative and backward-compatible
- enabling rewind is an explicit operator decision
- the property reads naturally in code, tests, and documentation

## Recommended Scope

Implement rewind only for `_partition_offset` in this iteration.

Do not extend the same flag to `_timestamp`.

Reason:

- offset rewind is deterministic and maps directly to Kafka's stored group
  offset model
- timestamp rewind would require additional policy discussion because timestamp
  lower bounds are first converted into offsets, and those resolved offsets are
  topic-configuration-sensitive
- limiting the first iteration to `_partition_offset` keeps the change clean
  and easier to reason about

## Required Code Changes

### 1. Add Session Property

File:

- `plugin/trino-kafka/src/main/java/io/trino/plugin/kafka/KafkaSessionProperties.java`

Changes:

- add the new boolean property to the property list
- add a getter helper
- document that it is only meaningful when committed-read mode is enabled

Notes:

- this is connector-local and low risk
- no config property is required unless you explicitly want a catalog-level
  default override

## 2. Relax Predicate Validation Conditionally

File:

- `plugin/trino-kafka/src/main/java/io/trino/plugin/kafka/KafkaFilterManager.java`

Changes:

- make lower-bound `_partition_offset` validation conditional on the new
  session property
- leave `_timestamp` validation unchanged in this iteration

Target behavior:

- committed-read + rewind blocked: current rejection remains
- committed-read + rewind allowed: `_partition_offset >= x`,
  `_partition_offset > x`, `_partition_offset = x`, and bounded windows become
  valid
- `_partition_offset = x` must retain single-offset semantics by planning the
  half-open range `[x, x+1)` rather than an empty range

Important detail:

- `filterRangeByDomain()` already converts supported offset predicates into a
  low/high range span
- the current committed-read path ignores that low value for planning
- that planning gap is what must be fixed next

## 3. Push Down Begin Offsets In Committed-Read Mode When Rewind Is Allowed

File:

- `plugin/trino-kafka/src/main/java/io/trino/plugin/kafka/KafkaFilterManager.java`

Changes:

- in the offset pushdown section, allow begin-offset override in committed-read
  mode when rewind is allowed

Current behavior:

- committed-read mode uses only the filtered end offset

Required new behavior:

- committed-read mode with rewind allowed must carry both:
  - filtered begin offset
  - filtered end offset

Why this matters:

- without this, a bounded rewind query such as
  `_partition_offset >= 50 AND _partition_offset < 70` would still plan from
  the group's committed offset rather than from `50`
- in cases where the group's committed offset is already beyond the requested
  window, the query would incorrectly produce no split and therefore could not
  rewind the stored offset

## 4. Redefine Committed-Read Split Planning For Rewind Mode

File:

- `plugin/trino-kafka/src/main/java/io/trino/plugin/kafka/KafkaSplitManager.java`

Changes:

- change committed-read split planning to accept filtered begin and filtered end
  rather than only filtered end
- keep existing resolution of committed offset validity and missing-offset
  policy
- add explicit planning rules for the rewind-enabled path

Recommended planning rules:

When rewind is blocked:

- keep the current behavior unchanged

When rewind is allowed:

- base start candidate from the filtered begin offset if present
- otherwise use the resolved committed offset as today
- use filtered end as the end bound
- if the query provides an explicit filtered begin offset, treat it as an
  intentional override even when missing-offset policy is `LATEST`
- clamp the effective start to broker log start and effective end to broker log
  end as the current filtering path already does
- if effective start < effective end, plan one data split with
  `commitTarget = effective end`
- if effective start == effective end after clamping, plan no data split and do
  not commit any new offset
- do not introduce checkpoint-only rewind commits for ordinary empty filtered
  windows, including:
  - `begin >= end`
  - explicit rewind windows that become empty after broker-range clamping
  - missing offset plus `LATEST` plus explicit rewind where the requested window
    is empty after planning
- preserve checkpoint-only behavior only for the existing committed-read
  initialization path where no explicit rewind window is requested and
  missing-offset policy `LATEST` resolves the group to the broker end

Decision table for the rewind-enabled path:

- explicit rewind requested, effective start < effective end:
  plan a data split and commit `effective end` only after full consumption
- explicit rewind requested, effective start == effective end:
  no split, no commit
- no explicit rewind requested, valid committed offset:
  preserve current behavior
- no explicit rewind requested, missing/invalid committed offset, policy
  `LATEST`:
  preserve current checkpoint-only initialization behavior

Key semantic point:

- with rewind enabled, the committed offset is no longer guaranteed to be
  monotonic
- the stored group offset becomes "the next offset after the fully consumed
  explicitly planned window"

## 5. Keep Commit Gating Logic As-Is Unless Testing Proves Otherwise

File:

- `plugin/trino-kafka/src/main/java/io/trino/plugin/kafka/KafkaRecordSet.java`

Current state:

- commit is already suppressed on failure, interruption, non-fully-consumed
  close, and `position < split end`

Plan:

- do not change this logic for the rewind feature
- reuse the existing commit gate with the new split bounds and commit target

Reason:

- rewind changes what range is planned
- it does not change the core safety rule for when a planned split may commit

## 6. Update Tests

### Unit tests

Files:

- `plugin/trino-kafka/src/test/java/io/trino/plugin/kafka/TestKafkaCommittedReadSplitManager.java`
- `plugin/trino-kafka/src/test/java/io/trino/plugin/kafka/TestKafkaRecordSetCommittedRead.java`

Add coverage for:

- committed-read with rewind blocked still rejects lower-bound
  `_partition_offset`
- committed-read with rewind allowed accepts lower-bound
  `_partition_offset`
- translation semantics for `_partition_offset = x` produce a split covering
  exactly `[x, x+1)`
- translation semantics for `_partition_offset > x` produce a split beginning at
  `x + 1`
- translation semantics for `_partition_offset >= x` produce a split beginning
  at `x`
- bounded rewind window where committed offset is ahead of the requested start
  produces a split covering the requested window
- rewind enabled plus missing-offset policy `LATEST` plus no committed offset
  plus explicit lower-bound `_partition_offset` produces a historical data
  split rather than a checkpoint-only split
- rewind enabled plus missing-offset policy `LATEST` plus invalid committed
  offset plus explicit lower-bound `_partition_offset` produces a historical
  data split starting from the requested window after broker-range clamping
- explicit rewind window that becomes empty after broker-range clamping produces
  no split and no commit
- explicit rewind with `begin >= end` produces no split and no commit
- mixed per-partition outcomes are handled correctly when one partition plans a
  rewind data split and another resolves to an empty window
- fully consuming a rewind-planned split keeps `commitTarget` at the window end
- early close of a rewind-planned split still suppresses commit

### Broker-backed integration tests

File:

- `plugin/trino-kafka/src/test/java/io/trino/plugin/kafka/TestKafkaCommittedReadMode.java`

Add coverage for:

- create topic with enough rows to make the offset windows unambiguous
- under group `G`, read forward and verify committed offset advances on Kafka
- issue a committed-read query under the same group with rewind enabled and an
  explicit lower-bound offset before the current committed position
- verify on the Kafka side that the group offset moves backward to the new
  fully consumed window end
- run the same group again without explicit rewind filter and verify the query
  resumes from the rewound committed offset
- verify the rewind-blocked default still rejects the same query shape
- verify that a brand-new group under catalog missing-offset policy `LATEST`
  can still read an explicit historical offset window when rewind is enabled
- verify that a group with an invalid committed offset under catalog
  missing-offset policy `LATEST` can still read an explicit historical offset
  window when rewind is enabled

Important test design note:

- use explicit offset windows large enough that split behavior is
  deterministic
- validate broker state with Kafka `Admin`, not just Trino results

## 7. Update Documentation

Files:

- `plugin/trino-kafka/README.md`
- `KAFKA_CONSUMER_GROUP_OFFSET_COMMIT_PLAN.md`

Required updates:

- current docs explicitly say committed-read mode rejects lower-bound
  `_partition_offset` predicates
- introduce the new flag and explain the default
- explain that enabling the flag changes committed-read semantics from strict
  resume behavior to filter-relative checkpoint behavior for offset predicates
- state clearly that rewind support is for `_partition_offset` only in this
  iteration
- warn that same-group concurrent queries can overwrite each other's stored
  offsets, including backward movement
- restate that the existing repeated-scan/self-join guard still applies to
  committed-read rewind mode
- restate that committed-read rewind mode inherits the existing operational
  requirement that Trino `retry_policy=NONE`
- document exact predicate semantics for `_partition_offset = x`,
  `_partition_offset > x`, and `_partition_offset >= x`
- document that explicit rewind windows which resolve to empty ranges produce no
  split and no commit

## Decisions Confirmed Before Implementation

These decisions are already fixed for the coding instance:

1. Property name:
   `committed_read_allow_offset_rewind`

2. Scope:
   `_partition_offset` only

## Complexity Assessment

Implementation difficulty: moderate

Why it is feasible:

- all core behavior lives inside the Kafka connector
- Kafka itself permits committing a lower offset for the same group and
  partition
- there is no engine-level monotonicity contract to fight

Why it is not trivial:

- current committed-read planning only models `committedBase -> filteredEnd`
- rewind needs a deliberate `filteredBegin -> filteredEnd` planning path
- docs and tests currently encode the opposite contract and must be updated

## Suggested Implementation Order

1. add the session property and getter
2. make offset lower-bound validation conditional
3. propagate filtered begin offsets into committed-read planning when rewind is
   allowed
4. update split-planning logic and unit tests together
5. add broker-backed integration tests that verify backward commits on Kafka
6. update README and design doc to match the new optional semantics

## Acceptance Criteria

- default behavior is unchanged
- rewind-enabled committed-read queries can read below the group's current
  committed offset using `_partition_offset`
- fully consumed rewind-enabled reads commit the new window end on Kafka, even
  if it is lower than the prior committed offset
- partial or failed rewind-enabled reads do not commit
- subsequent committed-read queries under the same group resume from the
  rewound offset
- broker-backed tests prove the stored group offset actually moves as intended
- the existing repeated-scan/self-join guard remains active for committed-read
  rewind mode
- committed-read rewind documentation explicitly carries forward the operational
  requirement that Trino `retry_policy=NONE`
