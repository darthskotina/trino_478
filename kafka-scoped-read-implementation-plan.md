# Kafka Scoped-Read Implementation Plan

## Goal

Prevent accidental full-topic reads in the Kafka connector during normal read mode by requiring users to provide an explicit read scope in `WHERE`.

The restriction should:

- apply by default in normal mode
- not apply in committed-read mode
- be overridable dynamically with a catalog session property

Accepted examples under the proposed rule:

- `SELECT * FROM topic WHERE _partition_id = 1 AND _partition_offset > 100`
- `SELECT * FROM topic WHERE _partition_id = 1 AND _partition_offset BETWEEN 100 AND 200`
- `SELECT * FROM topic WHERE _partition_id = 1 AND _timestamp >= TIMESTAMP '2024-01-01 00:00:00.000'`

Rejected examples:

- `SELECT * FROM topic`
- `SELECT * FROM topic WHERE _partition_id = 1`
- `SELECT * FROM topic WHERE _partition_offset > 100`
- `SELECT * FROM topic WHERE _timestamp >= TIMESTAMP '2024-01-01 00:00:00.000'`

## Scope Rule

For normal mode only, treat a read as scoped if the pushed-down constraint contains:

- a predicate on `_partition_id`, and
- a predicate on either `_partition_offset` or `_timestamp`

Notes:

- lower-bound-only predicates are acceptable
- `_timestamp` counts as a valid scoping mechanism
- only pushdownable tuple-domain predicates are in scope for this change
- logically equivalent but non-pushdownable expressions may still be rejected

## Why This Is Viable

The connector already has the right enforcement point and predicate plumbing:

- `KafkaSessionProperties` already defines Kafka catalog session properties
- `KafkaSplitManager#getSplits` already branches normal mode vs committed-read mode
- `KafkaFilterManager#getKafkaFilterResult` already interprets `_partition_id`, `_partition_offset`, and `_timestamp` constraints into bounded Kafka reads

This means the feature can be added without changing connector architecture.

## Recommended Design

### 1. Add a session-property override

Add a new Kafka catalog session property in:

- `plugin/trino-kafka/src/main/java/io/trino/plugin/kafka/KafkaSessionProperties.java`

Recommended property:

- name: `allow_unscoped_reads`
- type: `boolean`
- default: `false`

Behavior:

- `false`: normal-mode scans must satisfy the scope rule
- `true`: bypass the restriction for the current session

This follows the existing connector pattern for dynamic behavior flags and avoids introducing a static connector config for an operational override.

### 2. Enforce the rule during split planning

Do not enforce this in `KafkaMetadata#applyFilter`.

That method is part of predicate accumulation and pushdown negotiation. Rejecting there is riskier because the optimizer may still be refining the effective constraint.

Instead, enforce during split planning in:

- `plugin/trino-kafka/src/main/java/io/trino/plugin/kafka/KafkaSplitManager.java`

Recommended flow:

1. Determine whether committed-read mode is enabled.
2. If committed-read mode is enabled, skip the new restriction entirely.
3. If committed-read mode is disabled and `allow_unscoped_reads` is `false`, validate the table constraint before split generation.
4. If validation fails, throw a `TrinoException` with `KAFKA_SPLIT_ERROR`.

This keeps the check close to the final read shape and ensures failure happens before broker work begins.

### 3. Keep predicate interpretation logic in `KafkaFilterManager`

Add a dedicated helper in:

- `plugin/trino-kafka/src/main/java/io/trino/plugin/kafka/KafkaFilterManager.java`

Recommended helper responsibilities:

- inspect `KafkaTableHandle.constraint()`
- resolve internal Kafka field names via `KafkaInternalFieldManager`
- detect whether `_partition_id` domain is present
- detect whether `_partition_offset` or `_timestamp` domain is present
- reject unscoped normal-mode reads

This keeps Kafka internal-field logic in one place alongside the existing committed-mode predicate validation.

### 4. Respect custom internal field prefixes

Do not hardcode `_partition_id`, `_partition_offset`, and `_timestamp` in detection logic.

Instead, resolve the effective column names through:

- `KafkaInternalFieldManager`

Why:

- the connector supports configurable internal-field prefixes
- hardcoded names would silently break installations using a custom prefix

The user-facing error message can still mention the default canonical field names for clarity.

### 5. Leave committed-read behavior unchanged

Committed-read mode already has dedicated predicate restrictions and planning behavior. The new scoped-read rule should not run there.

That avoids interference with:

- existing committed-read validation
- rewind behavior
- committed offset semantics
- committed-read test coverage already present on this branch

## Detailed Implementation Steps

### Step 1. Extend `KafkaSessionProperties`

Add:

- property constant for `allow_unscoped_reads`
- boolean property metadata with default `false`
- accessor method `isAllowUnscopedReads(ConnectorSession session)`

Acceptance for this step:

- property appears in connector session properties
- property is readable from `ConnectorSession`

### Step 2. Add scoped-read validation helper

Add a helper in `KafkaFilterManager` with a shape like:

- `validateNormalReadScope(ConnectorSession session, KafkaTableHandle kafkaTableHandle, boolean committedReadMode)`

Expected logic:

- return immediately if `committedReadMode` is `true`
- return immediately if `allow_unscoped_reads` is `true`
- read `TupleDomain<ColumnHandle>` from `kafkaTableHandle.constraint()`
- if no domain exists or no relevant internal-field predicates exist, reject
- require both:
  - `_partition_id` domain present
  - `_partition_offset` or `_timestamp` domain present

Recommended error message:

- `Normal Kafka reads require scope predicates on '_partition_id' and either '_partition_offset' or '_timestamp' for topic '%s'. Set session property 'allow_unscoped_reads' = true to override.`

### Step 3. Invoke validation from `KafkaSplitManager#getSplits`

Call the new validation before offset discovery and split generation.

That is the right place because:

- the final table constraint is available
- the mode decision is already made there
- failures happen before expensive Kafka interactions

### Step 4. Keep existing committed-mode validation intact

Do not merge the new rule into committed-read predicate validation.

The code should make the separation explicit:

- committed-read mode: existing rules only
- normal mode: new scoped-read rule plus existing pushdown behavior

### Step 5. Add targeted tests

Add focused tests at both unit/split-manager level and integration level.

## Test Plan

### Unit or split-manager tests

Recommended coverage in Kafka connector tests:

- normal mode rejects unconstrained scan
- normal mode rejects only `_partition_id`
- normal mode rejects only `_partition_offset`
- normal mode rejects only `_timestamp`
- normal mode accepts `_partition_id` plus `_partition_offset`
- normal mode accepts `_partition_id` plus `_timestamp`
- normal mode override property allows unconstrained scan
- committed-read mode is not affected by the new restriction

Best fit:

- extend `TestKafkaCommittedReadSplitManager` with new normal-mode planning cases, or
- add a dedicated split-manager test class if cleaner

### Integration tests

Extend:

- `plugin/trino-kafka/src/test/java/io/trino/plugin/kafka/TestKafkaIntegrationPushDown.java`

Recommended scenarios:

- `SELECT count(*) FROM default.<topic>` fails by default
- `SELECT count(*) FROM default.<topic> WHERE _partition_id = 1 AND _partition_offset > 2` succeeds
- `SELECT count(*) FROM default.<topic> WHERE _partition_id = 1 AND _timestamp >= TIMESTAMP '...'` succeeds
- the unscoped query succeeds when `kafka.allow_unscoped_reads = true`

This gives broker-backed evidence that the default restriction works while existing bounded-read behavior still functions.

## Expected Complexity

Implementation complexity is low to medium.

Low complexity aspects:

- no change to connector architecture
- no new table handle fields required
- session-property plumbing already exists
- split-planning validation is already a connector pattern

Medium complexity aspects:

- the behavior depends on what Trino turns into tuple domains
- some logically scoped queries may still fail if they are not pushdownable
- tests need to be careful to distinguish validation failure from unrelated Kafka behavior

## Risks and Tradeoffs

### 1. Tuple-domain-only enforcement

This change intentionally relies on pushed-down `TupleDomain` constraints, not arbitrary expression analysis.

Tradeoff:

- simpler and more robust implementation
- some non-pushdownable scoped predicates may still be rejected

This is acceptable for the current requirement.

### 2. “Scoped” does not mean “small”

The chosen rule prevents the worst accidental full-topic scans, but it does not guarantee a tiny read.

Examples:

- `_partition_id = 1 AND _partition_offset > 10` may still read a large tail
- `_partition_id IN (...) AND _timestamp >= ...` may still span substantial data

This is still a meaningful operational safeguard and matches the agreed requirement.

### 3. Backward compatibility

Queries that currently work in normal mode without scope predicates will start failing by default.

Mitigation:

- explicit session override via `allow_unscoped_reads`
- clear error message with remediation

## Acceptance Criteria

The change is complete when all of the following are true:

- normal-mode Kafka scans without scope predicates fail by default
- normal-mode Kafka scans with `_partition_id` plus `_partition_offset` succeed
- normal-mode Kafka scans with `_partition_id` plus `_timestamp` succeed
- committed-read mode remains unaffected
- the session property `allow_unscoped_reads` disables the restriction dynamically
- targeted tests cover the default restriction, accepted scoped reads, override behavior, and committed-read non-regression

## Out of Scope

The following are intentionally not part of this change:

- analyzing arbitrary `Constraint.getExpression()` trees
- planner-level query-shape rejection outside split planning
- limiting the exact size of a scoped read window
- changing committed-read semantics
- introducing a static catalog config to replace the session override
