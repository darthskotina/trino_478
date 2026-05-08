# Kafka Scoped-Read Implementation Plan

## Goal

Prevent accidental broad Kafka scans in normal read mode by rejecting topic reads unless the query supplies an explicit scope in `WHERE`.

The restriction should:

- be enabled by default
- apply only to normal mode
- not apply to committed-read mode
- remain dynamically overridable per session

This change affects Kafka table scans only. Insert / producer paths are unaffected.

## Semantic Choice

The guard should be based on effective scan narrowing, not merely on the presence of domains in `KafkaTableHandle.constraint()`.

In practice, that means the plan should validate only predicates that the Kafka connector actually uses to narrow a normal-mode read:

- partition filtering via `_partition_id`
- lower-bound pushdown via `_partition_offset`
- lower-bound pushdown via `_timestamp`

This is important because `_timestamp` upper bounds are not always honored in normal mode. In `KafkaFilterManager`, normal-mode `_timestamp` lower bounds are pushed down unconditionally, but upper bounds are only pushed down for LogAppendTime topics or when `timestamp_upper_bound_force_push_down_enabled` is enabled. Therefore:

- `_partition_id = 1 AND _timestamp >= ...` is a valid scoped read
- `_partition_id = 1 AND _timestamp < ...` is not a valid scoped read

The same principle applies to `_partition_offset`:

- `_partition_id = 1 AND _partition_offset >= 100` is valid
- `_partition_id = 1 AND _partition_offset < 100` is not valid

## Working Rule

For normal mode, a Kafka read is scoped only if both of the following are true:

1. The query includes an effective partition predicate.
2. The query includes an effective lower bound on `_partition_offset` or `_timestamp`.

### Effective partition predicate

The `_partition_id` predicate must resolve to an explicit finite set of partitions.

Examples that count:

- `_partition_id = 1`
- `_partition_id IN (1, 2, 3)`
- any domain representation that is equivalent to a finite set of singleton partition values

This naturally covers single-partition topics: `_partition_id = 0` still qualifies because equality is an explicit singleton partition set.

Examples that do not count:

- `_partition_id IS NOT NULL`
- `_partition_id > -1`
- `_partition_id BETWEEN 1 AND 3`
- arbitrary open or closed ranges that are not representable as a finite set of singleton partitions

This is intentionally stricter than "any narrowing partition domain". The reason is implementation timing: the guard should run before Kafka metadata calls, so it must rely on structural inspection of the pushed-down domain rather than comparing against the live topic partition set.

### Effective lower bound

The second part of the scope must be a lower bound that advances the scan start:

- `_partition_offset >= x`
- `_partition_offset > x`
- `_partition_offset = x`
- `_timestamp >= t`
- `_timestamp > t`
- `_timestamp = t`

Upper-bound-only predicates do not qualify:

- `_partition_offset < x`
- `_timestamp < t`

For `_timestamp`, the rule intentionally depends only on the lower bound because that is the part Kafka normal-mode planning always uses to advance `partitionBeginOffsets`. Upper bounds may additionally narrow the end of the scan, but they must not satisfy the scope rule on their own.

## Accepted Examples

- `SELECT * FROM topic WHERE _partition_id = 1 AND _partition_offset > 100`
- `SELECT * FROM topic WHERE _partition_id = 1 AND _partition_offset BETWEEN 100 AND 200`
- `SELECT * FROM topic WHERE _partition_id = 1 AND _timestamp >= TIMESTAMP '2024-01-01 00:00:00.000'`
- `SELECT * FROM topic WHERE _partition_id IN (1, 2, 3) AND _partition_offset >= 500`

## Rejected Examples

- `SELECT * FROM topic`
- `SELECT * FROM topic WHERE _partition_id = 1`
- `SELECT * FROM topic WHERE _partition_offset > 100`
- `SELECT * FROM topic WHERE _timestamp >= TIMESTAMP '2024-01-01 00:00:00.000'`
- `SELECT * FROM topic WHERE _partition_id = 1 AND _partition_offset < 100`
- `SELECT * FROM topic WHERE _partition_id = 1 AND _timestamp < TIMESTAMP '2024-01-01 00:00:00.000'`
- `SELECT * FROM topic WHERE _partition_id > -1 AND _partition_offset >= 100`
- `SELECT * FROM topic WHERE _partition_id BETWEEN 1 AND 3 AND _partition_offset >= 100`

## Why This Is Viable

The connector already has the right structure for this feature:

- `KafkaSessionProperties` already exposes Kafka catalog session properties
- `KafkaSplitManager#getSplits` already distinguishes normal mode from committed-read mode
- `KafkaFilterManager#getKafkaFilterResult` already contains the connector's actual scan-narrowing rules for `_partition_id`, `_partition_offset`, and `_timestamp`

That means the new restriction can be implemented as a split-planning validation without changing connector architecture.

## Recommended Property Model

Use a positive safety flag with connector-config backing:

- catalog config: `kafka.enforce-read-scope`
- session property: `enforce_read_scope`
- default: `true`

Reasoning:

- positive polarity reads more clearly than `allow_unscoped_reads`
- config backing lets operators disable the feature globally for staged rollout if needed
- session override still provides the required dynamic escape hatch

Recommended behavior:

- catalog default comes from `KafkaConfig`
- session property overrides the catalog default for the current session

## Enforcement Point

Enforce the restriction during split planning in:

- `plugin/trino-kafka/src/main/java/io/trino/plugin/kafka/KafkaSplitManager.java`

Do not enforce it in `KafkaMetadata#applyFilter`.

Reasoning:

- `applyFilter` is invoked during predicate accumulation and may be called multiple times
- the table handle seen in `getSplits` reflects the final accumulated pushdown the connector will plan against
- the guard should run where the connector knows the final scan semantics

The enforcement should be based on `KafkaTableHandle.constraint()`, not on the `Constraint constraint` parameter or `DynamicFilter dynamicFilter` passed to `getSplits`. Today the Kafka connector plans from the handle constraint, and the plan should state that explicitly so the assumption is visible.

Queries that do not produce a Kafka table scan, such as metadata-only statements, are unaffected. If Trino later adds a no-scan optimization for some Kafka read shape, the enforcement assumption should be revisited.

## Recommended Design

### 1. Add config-backed session property

Extend:

- `plugin/trino-kafka/src/main/java/io/trino/plugin/kafka/KafkaConfig.java`
- `plugin/trino-kafka/src/main/java/io/trino/plugin/kafka/KafkaSessionProperties.java`

Add:

- config field and accessor for `kafka.enforce-read-scope`
- session property `enforce_read_scope`
- helper accessor in `KafkaSessionProperties`

Acceptance for this step:

- the catalog default is centrally configurable
- the session property can override it dynamically

### 2. Add dedicated scoped-read validation

Add a helper in:

- `plugin/trino-kafka/src/main/java/io/trino/plugin/kafka/KafkaFilterManager.java`

Recommended shape:

- `validateNormalReadScope(ConnectorSession session, KafkaTableHandle tableHandle)`

Recommended logic:

- return immediately if committed-read mode is enabled
- return immediately if `enforce_read_scope` is `false`
- if `tableHandle.constraint().isAll()`, reject immediately
- resolve effective internal field names through `KafkaInternalFieldManager`
- read domains from `TupleDomain`
- require partition scope in the form of an explicit finite set of singleton partition values
- require an effective lower bound on `_partition_offset` or `_timestamp`

The helper should use structural inspection of `Domain`, not live partition metadata:

- do not rely on `filterValuesByDomain` and comparing sizes against topic partitions
- instead, inspect the `_partition_id` domain directly and accept only single-value or all-singleton discrete sets
- use lower-bound detection, not just domain presence, for `_partition_offset` and `_timestamp`

For `_timestamp`, the helper should explicitly ignore upper-bound-only domains as qualifying scope, even if the domain exists in the handle.

### 3. Run validation before expensive Kafka work

Invoke the validation from `KafkaSplitManager#getSplits` before offset discovery and split generation.

Recommended order:

1. inspect mode and enforcement property
2. if `tableHandle.constraint().isNone()`, return an empty `FixedSplitSource` immediately
3. validate scoped-read requirement for normal mode
4. continue with offset discovery and split planning

This keeps the failure cheap and avoids starting Kafka-side planning for a read that should be rejected.

### 4. Keep committed-read behavior separate

Do not fold this logic into the committed-read predicate validation.

The separation should stay explicit:

- committed-read mode uses its current validation rules
- normal mode uses the new scoped-read rule

That avoids unintended interaction with:

- committed offsets
- rewind behavior
- committed-read predicate restrictions already present on this branch

### 5. Use a user-error code, not a Kafka planning failure code

The rejection is a guardrail on query shape, not a Kafka-side split failure.

Recommended error-code choice:

- `StandardErrorCode.QUERY_REJECTED`

Alternative:

- a new connector-local error code such as `KAFKA_UNSCOPED_READ`

Avoid reusing `KAFKA_SPLIT_ERROR` for this case.

### 6. Format the error message with resolved internal field names

Do not hardcode `_partition_id`, `_partition_offset`, and `_timestamp` in the error text.

Instead, resolve the effective names via `KafkaInternalFieldManager` so installations with a custom prefix see accurate guidance.

Recommended message shape:

- `Kafka reads in normal mode require scope predicates on '%s' and a lower bound on either '%s' or '%s' for topic '%s'. Set session property 'enforce_read_scope' = false to override.`

## Edge Cases To Handle Explicitly

### `constraint.isAll()`

Reject as unscoped when enforcement is enabled.

### `constraint.isNone()`

Handle this in `KafkaSplitManager` by returning an empty `FixedSplitSource` before invoking the validator or `KafkaFilterManager`.

This is mostly a defensive path. In normal flow the Kafka connector may rarely see `isNone()`, but spelling it out avoids ambiguity and prevents the later `verify(!constraint.isNone())` in `KafkaFilterManager` from becoming load-bearing.

### `_partition_id IN (...)`

This should pass the partition-scope check.

### `_partition_id BETWEEN ...`

This should not pass the partition-scope check under the recommended design because the validator is intentionally limited to explicit finite partition sets before Kafka metadata calls.

### `_timestamp` upper bounds on CreateTime topics

These must not satisfy the scope rule by themselves because the connector may keep scanning from log start unless upper-bound pushdown is enabled.

## Test Plan

### Dedicated split-manager test class

Use a dedicated test host:

- `plugin/trino-kafka/src/test/java/io/trino/plugin/kafka/TestKafkaScopedReadSplitManager.java`

This is cleaner than extending `TestKafkaCommittedReadSplitManager`, which should remain focused on committed-read behavior.

Recommended coverage:

- rejects unconstrained normal-mode scan
- rejects `_partition_id` only
- rejects `_partition_offset` only
- rejects `_timestamp` only
- rejects `_partition_id = 1 AND _partition_offset < 100`
- rejects `_partition_id = 1 AND _timestamp < ...`
- rejects trivial partition predicate such as `_partition_id > -1`
- rejects `_partition_id BETWEEN 1 AND 3 AND _partition_offset >= 100`
- accepts `_partition_id = 1 AND _partition_offset >= 100`
- accepts `_partition_id = 1 AND _timestamp >= ...`
- accepts `_partition_id IN (1, 2, 3) AND _partition_offset >= 100`
- allows unscoped scan when `enforce_read_scope = false`
- proves committed-read mode is unaffected

### Integration coverage

Extend:

- `plugin/trino-kafka/src/test/java/io/trino/plugin/kafka/TestKafkaIntegrationPushDown.java`

Recommended scenarios:

- `SELECT count(*) FROM default.<topic>` fails by default
- `SELECT count(*) FROM default.<topic> WHERE _partition_id = 1 AND _partition_offset > 2` succeeds
- `SELECT count(*) FROM default.<topic> WHERE _partition_id = 1 AND _timestamp >= TIMESTAMP '...'` succeeds
- `SELECT count(*) FROM default.<topic> WHERE _partition_id = 1 AND _timestamp < TIMESTAMP '...'` fails
- unscoped query succeeds after `SET SESSION kafka.enforce_read_scope = false`

The integration tests should make clear that the guard is tied to actual scan planning, not just SQL syntax.

## Complexity

Implementation complexity remains low to medium.

Low complexity aspects:

- no connector-architecture change
- existing predicate plumbing can be reused
- config and session property patterns already exist

Medium complexity aspects:

- the validation must align with actual scan-narrowing semantics, not raw domain presence
- partition-scope detection must distinguish explicit finite partition sets from broader range predicates
- tests need to cover timestamp pushdown nuances explicitly

## Tradeoffs

### 1. Tuple-domain-only enforcement

The plan still intentionally relies on pushed-down `TupleDomain` constraints rather than arbitrary `Constraint.getExpression()` analysis.

Tradeoff:

- simpler and more predictable implementation
- logically scoped but non-pushdownable predicates may still be rejected

That is acceptable for this feature.

### 2. Scoped does not mean small

Even with the tighter rule, a scoped read can still be large:

- `_partition_id = 1 AND _partition_offset > 10` may read a long tail
- `_partition_id IN (1, 2, 3) AND _timestamp >= ...` may still span substantial data

This change is a guardrail against accidental broad scans, not a byte- or row-budget limiter.

### 3. Rollout control

Adding config backing is deliberate. Without it, operators would need to disable enforcement session by session during rollout or incident response.

## Acceptance Criteria

The change is complete when all of the following are true:

- normal-mode Kafka scans without scope predicates fail by default
- upper-bound-only `_partition_offset` or `_timestamp` predicates do not satisfy the scope rule
- trivial or range-based `_partition_id` predicates do not satisfy the scope rule
- explicit finite partition selection plus lower-bound offset or timestamp predicates succeed
- `_partition_id IN (...)` plus a valid lower bound succeeds
- committed-read mode remains unaffected
- the session property can disable enforcement dynamically
- the catalog config can change the default globally
- targeted tests cover default rejection, accepted scoped reads, override behavior, timestamp nuance, partition triviality, and committed-read non-regression

## Out Of Scope

The following are intentionally not part of this change:

- analyzing arbitrary `Constraint.getExpression()` trees
- planner-level rejection outside split planning
- enforcing exact row-count or byte-count limits on scoped reads
- changing committed-read semantics
- changing metadata-only or non-scan statement behavior
