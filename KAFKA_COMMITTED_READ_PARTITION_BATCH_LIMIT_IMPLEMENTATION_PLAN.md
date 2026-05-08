# Kafka Committed-Read Partition Batch Limit Plan

## Goal

Add a new Kafka connector session property that limits how many source records
one committed-read query may consume from each Kafka partition, while preserving
the connector's existing offset-commit model:

- committed-read mode stays opt-in
- the cap is enforced during split planning, not by SQL `LIMIT`
- `0` means unlimited and preserves the current behavior
- offsets are still committed only after full planned split exhaustion
- rewind-enabled queries may intentionally move the stored group offset
  backward, but only to the end of the fully consumed capped window

This change is aimed at reducing large one-shot load spikes against Kafka when
the first committed-read query for a group would otherwise scan a large backlog.

## Agreed Operator Contract

### Property

Add a new session property:

- `committed_read_max_rows_per_partition`

Recommended type:

- `BIGINT`-backed session property in Java (`long`)

Rationale:

- Kafka offsets are `long`
- the cap is combined with an offset start position during split planning
- using `long` avoids artificial narrowing and keeps overflow handling local to
  one helper

Validation:

- value must be `>= 0`
- `0` means unlimited

Scope:

- meaningful only when `committed_read_enabled = true`
- default mode continues to ignore committed offsets and should not change
  behavior because of this property

No catalog property is required for this iteration. The feature is intended to
be dynamically adjustable at session scope.

### Per-Partition Semantics

The cap is per partition, not global per topic.

Implications:

- a query over a topic with `P` selected partitions may read up to
  `P * committed_read_max_rows_per_partition` source rows in total
- each partition still advances its Kafka committed offset independently
- `_partition_offset` ordering remains per partition only; there is no global
  topic-wide offset order to preserve across partitions

### Normal Committed-Read Mode

For a partition without explicit rewind:

- `start` = resolved committed base as today
- `uncappedEnd` = filtered end as today
- if cap is `0`, `end = uncappedEnd`
- if cap is positive, `end = min(uncappedEnd, start + cap)`
- if `start < end`, plan one data split and set `commitTarget = end`
- if `start == end`, preserve today's behavior:
  - no split and no commit for ordinary empty windows
  - checkpoint-only split only for the existing `LATEST` initialization branch

Result:

- repeated plain `SELECT *` drains each partition in batches
- the committed offset after a fully consumed batch is always the next unread
  offset for that partition

EARLIEST interaction:

- for a new group under missing-offset policy `EARLIEST`, the resolved
  committed base is the current `logStart`
- the cap then applies normally from that resolved start
- example:
  - `logStart = 0`
  - `filteredEnd = 500000`
  - cap = `10000`
  - planned range is `[0, 10000)`
  - commit target is `10000`

### Rewind-Enabled Mode

When `committed_read_allow_offset_rewind = true` and the query contains an
explicit `_partition_offset` lower bound:

- the explicit lower bound remains authoritative for choosing `start`
- this applies independently to every selected partition
- the cap is then applied from that explicit `start`
- `commitTarget` remains the planned `end`

Examples for one partition with cap `10000`:

- `_partition_offset >= 20000`
  - `start = 20000`
  - `end = min(filteredEnd, 30000)`
  - rows read are offsets `20000..29999`
  - commit target is `30000`
- `_partition_offset > 20000`
  - `start = 20001`
  - `end = min(filteredEnd, 30001)`
  - rows read are offsets `20001..30000`
  - commit target is `30001`
- `_partition_offset = 5`
  - `start = 5`
  - `end = 6`
  - reads exactly one row if offset `5` exists
  - commit target is `6`

Important rewind contract:

- a rewind query is allowed to rebase the stored Kafka group offset backward
- a subsequent plain committed-read query resumes from that rebased offset
- rerunning the same rewind query is also allowed to rewind again, because the
  explicit lower bound remains authoritative for that query shape

This behavior is intentional and must be documented as an operator-facing
feature, not treated as an implementation accident.

### Multi-Partition Rewind Semantics

`_partition_offset` is partition-local in Kafka. Therefore a rewind predicate
applies to every selected partition, not to one inferred partition.

Examples:

- `WHERE _partition_offset >= 20000`
  applies the same lower bound to every selected partition
- `WHERE _partition_id = 3 AND _partition_offset >= 20000`
  applies only to partition `3`

For a topic with multiple partitions:

- the same numeric offset may exist in many partitions
- each partition gets its own capped window
- each partition commits its own new offset independently

## Non-Goals

- no global per-topic row cap
- no whole-query atomic commit barrier
- no query-success-only commit semantics
- no change to Kafka group membership behavior (`assign(...)` stays)
- no `_timestamp` rewind support
- no extra coordinator state beyond Kafka's existing committed offsets
- no protection against operators intentionally rerunning the same rewind query

## Current Branch Behavior Summary

The current branch already provides:

- committed-read mode with one planned split per partition
- explicit committed-read group ID via session property
- optional rewind based on `_partition_offset` lower bounds
- close-time commit only after full planned split consumption

The current large-backlog problem exists because committed-read planning
currently uses:

- `start = committed base` or explicit rewind lower bound
- `end = filtered end`

with no additional batch cap for data-carrying splits.

Committed-read still must produce exactly one split per selected partition
after this change. The new cap narrows that one split's range, but must not
reintroduce `messages-per-split` sub-partitioning in committed-read mode.

## Required Code Changes

### 1. Add Session Property

File:

- `plugin/trino-kafka/src/main/java/io/trino/plugin/kafka/KafkaSessionProperties.java`

Changes:

- add `committed_read_max_rows_per_partition`
- default it to `0`
- validate `>= 0`
- add a helper getter returning `long`
- describe clearly that it limits committed-read scan planning per partition

Implementation note:

- use `longProperty(...)` unless there is a strong reason to prefer `integer`
- keep the property session-only in this iteration

### 2. Extend Committed-Read Split Planning Inputs

File:

- `plugin/trino-kafka/src/main/java/io/trino/plugin/kafka/KafkaSplitManager.java`

Changes:

- read the new session property once near other committed-read session settings
- thread the cap into committed-read split planning

Scoping rule:

- read the session property unconditionally for simplicity
- consume it only inside committed-read split planning
- no extra default-mode guard is required beyond the existing
  `committedReadMode` branch, because `planCommittedReadSplit(...)` is only
  reached in committed-read mode

Recommended method shape:

- extend `planCommittedReadSplit(...)` to accept
  `maxRowsPerPartition`

### 3. Cap Data Split End During Planning

File:

- `plugin/trino-kafka/src/main/java/io/trino/plugin/kafka/KafkaSplitManager.java`

Core rule:

- compute `uncappedStart` and `uncappedEnd` exactly from the already agreed
  committed-read / rewind logic
- only after `start` is chosen and only inside the `start < uncappedEnd`
  data-split branch, cap the data-carrying end as:
  - `cappedEnd = min(uncappedEnd, saturatedAdd(start, maxRowsPerPartition))`
    when cap > 0
  - `cappedEnd = uncappedEnd` when cap == 0

Important detail:

- cap only data-carrying splits
- do not alter the existing checkpoint-only `LATEST` initialization logic when
  there is no explicit rewind window and planning resolves to an empty range
- evaluate the checkpoint-only branch against uncapped values
- do not modify `end` before the `start < uncappedEnd` gate

Reason:

- checkpoint-only behavior is about persisting a resolved initial resume point
- the new feature is about limiting data reads, not changing empty-range
  initialization semantics

Invariant:

- the cap is applied only when `start < uncappedEnd`
- therefore a missing/invalid-offset `LATEST` initialization still resolves to
  the same checkpoint-only split as today, because `start == uncappedEnd`
  before capping and the cap is never consulted for that branch

### 4. Preserve Existing Rewind Start Semantics

Files:

- `plugin/trino-kafka/src/main/java/io/trino/plugin/kafka/KafkaFilterManager.java`
- `plugin/trino-kafka/src/main/java/io/trino/plugin/kafka/KafkaSplitManager.java`

Plan:

- keep the current rewind lower-bound pushdown and validation model
- do not redesign rewind selection semantics for this feature

What must remain true:

- when rewind is disabled, lower-bound `_partition_offset` remains rejected
- when rewind is enabled, explicit lower bounds keep overriding the committed
  base for choosing `start`
- then the new cap is applied from that chosen `start`

### 5. Keep Commit Gating Logic Conservative

File:

- `plugin/trino-kafka/src/main/java/io/trino/plugin/kafka/KafkaRecordSet.java`

Plan:

- do not redesign close-time commit behavior for this feature
- continue committing only after verified full split exhaustion

Why:

- this branch already models commit safety at split exhaustion time
- the new feature changes the planned split length, not the correctness rule
  for when a split may commit

Code comment to add:

- document near the commit gate that the connector intentionally commits the
  planned batch end, not the whole broker snapshot end

### 6. Keep Default Mode Unchanged

Files:

- `plugin/trino-kafka/src/main/java/io/trino/plugin/kafka/KafkaSplitManager.java`
- `plugin/trino-kafka/src/main/java/io/trino/plugin/kafka/KafkaSessionProperties.java`

Requirement:

- `messages-per-split` remains the planning control for default mode
- the new session property must not change default-mode split planning
- `messages-per-split` remains irrelevant in committed-read mode, which still
  plans exactly one split per selected partition

## Exact Planning Rules To Encode

### Normal Committed-Read

Given:

- `resolvedCommittedBase`
- `filteredEnd`
- `cap`

Plan:

- `start = min(filteredEnd, resolvedCommittedBase)` under existing rules
- `uncappedEnd = filteredEnd`
- if `start < uncappedEnd` and `cap > 0`, set
  `end = min(uncappedEnd, saturatedAdd(start, cap))`
- otherwise `end = uncappedEnd`

Commit:

- `commitTarget = end`

Upper-bound interaction:

- the cap can only narrow the window
- it never widens the window
- a tighter explicit upper-bound predicate naturally takes precedence via
  `min(uncappedEnd, ...)`

EARLIEST interaction:

- if missing-offset policy resolves the committed base from `EARLIEST`, that
  resolved base is treated exactly like any other committed start
- the cap is then applied from that resolved start

### Rewind-Enabled With Explicit Lower Bound

Given:

- explicit `_partition_offset` lower bound already converted into filtered begin
- `filteredBegin`
- `filteredEnd`
- `cap`

Plan:

- `start = filteredBegin`
- `uncappedEnd = filteredEnd`
- if `start < uncappedEnd` and `cap > 0`, set
  `end = min(uncappedEnd, saturatedAdd(start, cap))`
- otherwise `end = uncappedEnd`

Commit:

- `commitTarget = end`

Upper-bound interaction:

- the cap can only narrow the explicit rewind window
- if the query's upper bound is tighter than `start + cap`, the upper bound
  wins naturally

### Empty Windows

Rules:

- if `start == end` for an explicit rewind window, plan no split and commit
  nothing
- if `start == end` in ordinary committed-read mode because committed progress
  already reached the filtered end, keep current no-split/no-commit behavior
- if `start == end` only because missing/invalid offset policy is `LATEST`
  without explicit rewind, preserve current checkpoint-only initialization

### Overflow Guard

Use safe arithmetic when computing `start + cap`.

Recommended approach:

- add a small `saturatedAdd(long left, long right)` helper
- implement capped-end calculation as
  `min(uncappedEnd, saturatedAdd(start, cap))`
- `saturatedAdd` should return `Long.MAX_VALUE` on overflow rather than throw

The implementation must not wrap negative on large offsets.

## Test Plan

### Split-Planning Unit Tests

File:

- `plugin/trino-kafka/src/test/java/io/trino/plugin/kafka/TestKafkaCommittedReadSplitManager.java`

Add coverage for:

- cap `0` preserves current committed-read planning by mirroring an existing
  committed-read scenario and asserting identical range and metadata with the
  property explicitly set to `0`
- specifically, add a cap-`0` variant of an existing "one split per partition"
  committed-read test rather than relying on a vague preservation assertion
- normal committed-read with cap plans `[committedBase, committedBase + cap)`
  when enough unread rows exist
- new group under missing-offset policy `EARLIEST` plus cap plans
  `[logStart, logStart + cap)` when enough rows exist
- normal committed-read with cap respects a tighter filtered upper bound
- multi-partition committed-read applies the cap independently per partition
- rewind-enabled planning with `_partition_offset >= x` and cap plans
  `[x, x + cap)` per partition
- rewind-enabled planning with `_partition_offset > x` and cap plans
  `[x + 1, x + 1 + cap)` per partition
- rewind-enabled planning with `_partition_offset = x` still plans `[x, x + 1)`
  even when cap is larger
- explicit rewind under missing-offset policy `LATEST` still produces a
  historical data split rather than a checkpoint-only split
- explicit rewind under invalid committed offset plus policy `LATEST` still
  produces a historical capped data split
- empty explicit rewind windows still produce no split and no commit

### Cursor / Commit Unit Tests

File:

- `plugin/trino-kafka/src/test/java/io/trino/plugin/kafka/TestKafkaRecordSetCommittedRead.java`

Add coverage for:

- a fully consumed capped split commits the capped end, not the original broker
  snapshot end
- early close of a capped split still suppresses commit
- rewind-planned capped split commits the rebased end
- equality rewind split with cap larger than one row still commits `x + 1`
- commit failure on a fully consumed capped data split propagates the same
  `TrinoException` path as any other committed-read data split

### Broker-Backed Integration Tests

File:

- `plugin/trino-kafka/src/test/java/io/trino/plugin/kafka/TestKafkaCommittedReadMode.java`

Required scenarios:

- new group, one partition, catalog missing-offset policy `EARLIEST`, cap
  `10000`, topic has more than `10000` rows:
  - first plain committed-read query returns `10000`
  - Kafka committed offset becomes `10000`
  - second plain committed-read query returns the next batch
  - repeated queries eventually drain the topic
- new group, one partition, catalog missing-offset policy `LATEST`, cap
  `10000`, topic already has rows:
  - first plain committed-read query returns `0`
  - Kafka committed offset becomes the broker end
  - this verifies the cap does not affect checkpoint-only initialization
- multi-partition topic with cap:
  - each partition advances by at most the cap
  - total rows may exceed the cap because the cap is per partition
- rewind-enabled query after a group already reached a higher committed offset:
  - plain query advances offset to a high value
  - rewind query with explicit lower bound reads only one capped batch
  - Kafka committed offset moves backward only to the capped rewind batch end
  - next plain query resumes from that rebased committed offset
- equality rewind query:
  - `_partition_offset = x` returns at most one row per selected partition
  - Kafka committed offset becomes `x + 1`
- `>` rewind query:
  - commit target is one past the last emitted offset
- default rewind-blocked behavior remains unchanged when
  `committed_read_allow_offset_rewind = false`

Important integration assertions:

- read Kafka committed offsets from the broker with `Admin`
- verify actual stored offsets, not only query row counts

## Documentation Updates

### Operator README

File:

- `plugin/trino-kafka/README.md`

Required updates:

- add the new session property to the property list
- explain per-partition batching semantics
- state that the cap applies in committed-read mode only
- state that `0` means unlimited
- explain that the cap limits planned offset windows, not guaranteed returned
  row counts
- note that due to Kafka realities such as compaction, tombstones, or other
  offset-to-row mismatches, the actual number of returned rows may be smaller
  than the configured cap
- explain that rewind plus cap may deliberately move the stored group offset
  backward only to the capped window end
- explain that subsequent plain reads resume from that rebased offset
- document that rerunning the same rewind query can rewind again
- document that `_partition_offset` is partition-local and rewind predicates
  apply to every selected partition unless `_partition_id` narrows the scope

### Main Connector Docs

File:

- `docs/src/main/sphinx/connector/kafka.md`

Required updates:

- mention the modified connector's committed-read batching extension if this
  branch uses the main connector doc as operator documentation
- clarify the distinction between `kafka.messages-per-split` in default mode
  and `committed_read_max_rows_per_partition` in committed-read mode
- carry over the multi-partition caveat and rewind semantics
- clarify that committed-read batching still produces one split per selected
  partition and that the property limits planned offset windows, not exact
  output row counts

### Design / Plan Docs

File:

- `KAFKA_CONSUMER_GROUP_OFFSET_COMMIT_PLAN.md`

Required updates:

- update the semantic contract section so it no longer implies that a committed
  data split always runs to the broker snapshot end
- add the new per-partition batch-limit contract
- add the rewind-plus-cap rebase behavior
- document the ordering invariant that capping happens only inside the normal
  data-split branch and does not participate in checkpoint-only `LATEST`
  initialization

## Suggested Implementation Order

1. add the session property and validation
2. extend committed-read split planning to accept the cap
3. cap data split end for normal committed-read mode
4. apply the same cap to rewind-planned data splits
5. add split-planning unit tests
6. add cursor commit unit tests
7. add broker-backed integration tests
8. update README, connector docs, and plan docs

## Acceptance Criteria

- committed-read queries with cap `0` behave exactly as they do today
- committed-read queries with positive cap read at most that many source rows
  per selected partition
- fully consumed capped reads commit the capped end offset
- partial or failed capped reads do not commit
- repeated plain committed-read queries drain a backlog in batches
- rewind-enabled capped queries may move the stored group offset backward to the
  capped rewind window end
- subsequent plain committed-read queries resume from that rebased offset
- `_partition_offset` rewind semantics remain partition-local and are clearly
  documented
- default mode remains unchanged

## Risks To Watch

- off-by-one mistakes on `>`, `>=`, and `=` rewind predicates
- silent misunderstanding of per-partition versus per-topic cap semantics
- overflow when computing `start + cap`
- accidental changes to checkpoint-only `LATEST` initialization behavior
- documentation drift between README, main connector docs, and design notes
