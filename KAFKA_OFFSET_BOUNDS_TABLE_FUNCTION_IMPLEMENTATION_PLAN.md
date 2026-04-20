# Kafka Offset Bounds Table Function Implementation Plan

## Goal

Add a Kafka connector table function that lets a user query the current broker
offset bounds for a topic:

- for all partitions in the topic
- or for one explicitly requested partition

The feature is intentionally limited to metadata lookup. It must not perform a
topic data scan, and it must not change the semantics of normal Kafka table
reads or committed-read mode.

## Non-Goals

- no changes to row-reading behavior for Kafka tables
- no changes to committed-read planning or commit behavior
- no new hidden/internal columns on Kafka tables
- no exposure of arbitrary broker topics outside the connector's existing topic
  visibility model
- no topic-wide synthetic single-offset abstraction that hides partition
  boundaries
- no support for multiple partition arguments in the first iteration
- no attempt to expose consumer-group committed offsets in the same feature

## Why The Feature Should Be Partition-Oriented

Kafka offsets are defined per partition, not per topic.

That means:

- a topic does not have one canonical "minimum offset"
- a topic does not have one canonical "maximum offset"
- the only primary broker truth is partition-level offset bounds

The correct first-class result shape is therefore one row per partition.

If a user wants a topic-level summary, they can derive it with SQL:

```sql
SELECT
    min(log_start_offset) AS topic_min_log_start_offset,
    max(log_end_offset) AS topic_max_log_end_offset
FROM TABLE(kafka.system.offsets(schema_name => 'default', table_name => 'orders'));
```

## Current Codebase Context

The connector already performs the exact Kafka metadata lookups needed for this
feature during split planning:

- `KafkaSplitManager` resolves `partitionsFor(topic)`
- `KafkaSplitManager` fetches `beginningOffsets(...)`
- `KafkaSplitManager` fetches `endOffsets(...)`

Relevant code paths today:

- `plugin/trino-kafka/src/main/java/io/trino/plugin/kafka/KafkaSplitManager.java`
- `plugin/trino-kafka/src/main/java/io/trino/plugin/kafka/KafkaConnector.java`
- `plugin/trino-kafka/src/main/java/io/trino/plugin/kafka/KafkaMetadata.java`

The Kafka plugin does not currently expose connector table functions, so the
main implementation step is adding that SQL surface to the connector.

## Final User-Facing Contract

### Function Name

Chosen contract:

- schema: `system`
- function: `offsets`

Example invocation:

```sql
SELECT *
FROM TABLE(kafka.system.offsets(schema_name => 'default', table_name => 'orders'));
```

### Arguments

Required argument:

- `schema_name VARCHAR`
- `table_name VARCHAR`

Optional argument:

- `partition BIGINT`

Chosen semantics:

- `(schema_name, table_name)` identifies a Kafka table that is already exposed
  by the connector's metadata / table-description model
- if `partition` is omitted, return one row per partition for the topic
- if `partition` is provided, return exactly one row for that partition
- if the `(schema_name, table_name)` pair is not connector-visible, fail
  before broker offset lookup
- if the partition does not exist for the topic, fail with a clear user error

### Output Columns

Chosen output schema:

- `partition_id BIGINT`
- `log_start_offset BIGINT`
- `log_end_offset BIGINT`
- `last_readable_offset BIGINT`

Column semantics:

- `log_start_offset` is the current broker beginning offset for the partition
- `log_end_offset` is the current broker end offset for the partition
- `log_end_offset` is exclusive, matching Kafka semantics
- `last_readable_offset` is:
  - `log_end_offset - 1` when the partition is non-empty
  - `NULL` when `log_start_offset == log_end_offset`

### Example Queries

Inspect all partitions:

```sql
SELECT *
FROM TABLE(kafka.system.offsets(schema_name => 'default', table_name => 'orders'));
```

Inspect one partition directly through the function parameter:

```sql
SELECT *
FROM TABLE(kafka.system.offsets(schema_name => 'default', table_name => 'orders', partition => 2));
```

Get only the topic-wide summary:

```sql
SELECT
    min(log_start_offset) AS topic_min_log_start_offset,
    max(log_end_offset) AS topic_max_log_end_offset
FROM TABLE(kafka.system.offsets(schema_name => 'default', table_name => 'orders'));
```

Get the current last readable offset per partition:

```sql
SELECT partition_id, last_readable_offset
FROM TABLE(kafka.system.offsets(schema_name => 'default', table_name => 'orders'));
```

### Visibility And Authorization Semantics

This function is scoped to connector-visible topics, not arbitrary broker topic
names.

Chosen contract:

- `(schema_name, table_name)` must resolve through the Kafka connector's
  existing metadata model
- the function must not be a side channel for discovering Kafka topics that
  are not exposed through connector tables
- function analysis must perform explicit access-control checks for the
  resolved connector table
- authorization is intentionally object-level rather than column-level
- a user who is allowed to access the connector-visible Kafka table object may
  call the function for that table
- the function does not require column-level `SELECT` permission on message or
  key columns, because it returns only broker offset metadata
- column-level policies on Kafka table payload columns do not apply to this
  function

This keeps the feature aligned with the current Kafka connector product model,
where schema/table metadata defines the visible surface and maps to broker
topic names.

This is an intentional product choice, not an omission:

- the function is broader than ordinary payload-column read authorization
- the function is narrower than arbitrary broker-topic inspection
- the function is treated as metadata access on a connector-visible table
  object

## Metadata-Only Semantics

This feature must be explicitly documented as a metadata lookup, not a topic
scan.

Expected Kafka interactions:

- `partitionsFor(topic)`
- `beginningOffsets(requestedPartitions)`
- `endOffsets(requestedPartitions)`

Important consequences:

- the function does not read topic records
- the function does not decode Kafka messages
- the function does not perform a full scan over topic data
- runtime cost scales mainly with partition count, not with message count

### Impact Of Optional `partition`

This is the main reason to support the optional argument in v1.

If the user writes:

```sql
SELECT *
FROM TABLE(kafka.system.offsets(schema_name => 'default', table_name => 'orders', partition => 2));
```

the connector should narrow Kafka calls to that single requested partition
rather than fetching all partitions and filtering later in Trino.

This avoids an API shape where:

- SQL says "one partition"
- but Kafka still sees "all partitions"

The optional parameter therefore improves both usability and operational
behavior for high-partition-count topics.

Important nuance:

- `partitionsFor(topic)` may still be used for live partition discovery and
  validation
- the narrowing requirement applies to `beginningOffsets(...)` and
  `endOffsets(...)` after the requested partition set has been resolved

### End-Offset Semantics And Consumer Configuration

This function reports end offsets as seen through the connector's configured
Kafka consumer settings.

Chosen contract:

- offset lookups use the connector's Kafka client configuration path
- the function does not override consumer isolation or other client properties
  to force a separate raw-broker view
- documentation must describe the result as connector-consumer-visible offset
  bounds under the effective Kafka client configuration

This keeps the feature consistent with the connector's existing Kafka client
behavior and avoids introducing a second, differently configured metadata
surface.

## Final Architecture

## 1. Add A Kafka Connector Table Function

Add the first table function to `trino-kafka` using the same general connector
pattern used in plugins such as OpenSearch.

High-level pieces required:

- a provider class for the function definition
- a function handle carrying resolved arguments
- connector wiring so `KafkaConnector#getTableFunctions()` returns the function
- connector wiring for `getFunctionProvider()` so the function can use the
  dedicated table-function processor path

This plan does not use the ordinary Kafka table-scan runtime as the execution
path for the function.

`KafkaMetadata#applyTableFunction(...)` is not part of the chosen execution
model for this function. If implemented for interface completeness, it must
return `Optional.empty()` for the offset-bounds function handle so the planner
does not rewrite the function into a `TableScanNode`.

This is the largest implementation slice of the feature.

## 2. Use The Dedicated Table-Function Runtime Path

Do not route this feature through the ordinary Kafka table-scan path built
around `KafkaTableHandle`, `KafkaSplit`, `KafkaColumnHandle`,
`KafkaSplitManager#getSplits(..., ConnectorTableHandle, ...)`, and
`KafkaRecordSetProvider`.

Chosen approach:

- define a dedicated `ConnectorTableFunctionHandle`
- define a dedicated function split path using
  `ConnectorSplitManager#getSplits(..., ConnectorTableFunctionHandle)`
- define a dedicated `ConnectorSplit` type for offset-bounds work
- provide rows through the table-function processor path exposed by
  `FunctionProvider`

Reason:

- the current Kafka runtime hard-casts the normal scan path to Kafka-specific
  table, split, and column handle types
- forcing a second logical feature through that path would require broad
  branching in metadata, split planning, and record-set production
- the dedicated table-function runtime is a better fit for metadata-only row
  generation and mirrors patterns already used elsewhere in the repo

Implementation consequence:

- any function handle and split classes used in this path must be serializable
  in the same way other distributed connector handles and splits are

## 3. Factor Out Broker Offset Lookup And Topic Resolution

The connector already resolves topic partitions and begin/end offsets in
`KafkaSplitManager`.

Chosen refactor:

- extract a small connector-local service or helper responsible for:
  - resolving the connector-visible `(schema_name, table_name)` pair to its
    Kafka topic name
  - resolving available partitions for a topic
  - validating an optional requested partition
  - fetching `beginningOffsets(...)` and `endOffsets(...)` for the requested
    partition set

Why this is worthwhile:

- keeps semantics aligned between split planning and the new function
- reduces duplicate Kafka client code
- makes unit and integration testing easier

Helper responsibilities:

- input:
  - `ConnectorSession`
  - connector-visible `(schema_name, table_name)` identifier
  - optional partition id
- output:
  - ordered partition metadata rows or records containing:
    - partition id
    - log start offset
    - log end offset

## 4. Keep Execution On Metadata Paths Only

The table function should produce rows from broker metadata, not from Kafka
message page sources.

Target behavior:

- one output row per requested partition
- row generation should use offset metadata only
- no message polling
- no decoder involvement

The implementation may use whichever Trino table-function execution path fits
the Kafka connector best, but this invariant is fixed:

- the function returns broker metadata rows only
- it does not reuse message-polling execution paths
- it does not invoke `poll(...)` or any row decoder path

## 5. Split Responsibilities By Phase

The plan fixes validation and resolution timing as follows.

### Analyze Phase

Perform at function analysis time:

- null / blank `schema_name` validation
- null / blank `table_name` validation
- `partition` type and range validation
- reject `partition < 0`
- reject `partition > Integer.MAX_VALUE`
- resolve the requested `(schema_name, table_name)` pair through the
  connector-visible metadata model
- explicit access-control checks for the resolved table object using the
  metadata-function authorization rule defined by this plan

This keeps syntax, visibility, and authorization failures early and stable.

### Split Generation / Execution-Planning Phase

Perform during the function split-generation path:

- live broker partition discovery for the resolved Kafka topic name
- validation that the requested partition currently exists
- `beginningOffsets(...)` / `endOffsets(...)` lookup

This keeps live broker-state validation aligned with the actual offset snapshot
used for result production.

## Validation Rules

Required validations:

- `schema_name` must be non-null and non-blank
- `table_name` must be non-null and non-blank
- `(schema_name, table_name)` must resolve to a connector-visible Kafka table
- if `partition` is provided:
  - it must be non-negative
  - it must be less than or equal to `Integer.MAX_VALUE`
  - it must exist in the topic's current partition set

Failure handling rules:

- clear distinction between:
  - unknown or non-visible connector table
  - invalid partition argument
  - broker lookup failure

## Semantic Details To Preserve

### 1. Exclusive End Offset

The function must not rename or describe `log_end_offset` as the offset of the
last readable record.

Correct contract:

- `log_end_offset` is exclusive
- `last_readable_offset` is the convenience field for the last currently
  readable record

This must be covered by tests and docs because it is the easiest place for an
off-by-one regression.

### 2. Snapshot Semantics

The function returns a point-in-time broker snapshot.

That means:

- the values may change immediately after lookup
- no transactional stability is implied across repeated calls
- no stronger consistency guarantee is required than what Kafka already gives
  for these APIs

### 3. Topic-Level Aggregation Is Derived, Not Primary

The function should not return a single row with topic-level min/max values.

Reason:

- it would hide the partition dimension
- it would encourage misinterpretation of topic offsets as globally ordered
- SQL aggregation already gives the derived view when needed

## Delivery Plan

## Step 1. Define The SQL Contract

Finalize:

- function name
- schema name
- argument names and types
- output column names and meanings
- connector-visible schema/table semantics
- authorization semantics
- failure cases

These choices are locked by this plan so tests and docs align from the start.

## Step 2. Wire Table Function Support Into `KafkaConnector`

Files likely involved:

- `plugin/trino-kafka/src/main/java/io/trino/plugin/kafka/KafkaConnector.java`
- Kafka module/binding classes

Required outcome:

- the connector advertises the new table function
- the connector exposes a `FunctionProvider` for the function processor path

## Step 3. Add Function Definition And Handle

Add a dedicated function provider and handle class.

Required outcome:

- the function accepts `schema_name`
- the function accepts `table_name`
- the function optionally accepts `partition`
- analysis produces a stable output descriptor and handle

## Step 4. Extend Metadata Only For Topic Resolution Support

Files likely involved:

- `plugin/trino-kafka/src/main/java/io/trino/plugin/kafka/KafkaMetadata.java`

Required outcome:

- metadata provides the schema/table-to-topic resolution needed by function
  analysis
- `applyTableFunction(...)` is not used for this function's execution path and
  must not trigger rewrite to a table scan

## Step 5. Implement Broker Offset Lookup Helper

Files likely involved:

- a new helper/service under `plugin/trino-kafka/...`
- `KafkaSplitManager` call sites if refactored to reuse it

Required outcome:

- connector-visible schema/table resolution
- all-partitions lookup path
- single-partition lookup path
- stable validation and ordering

## Step 6. Implement Row Production For The Function

Required outcome:

- use the dedicated table-function processor path
- return one row per requested partition
- populate `partition_id`, `log_start_offset`, `log_end_offset`,
  `last_readable_offset`
- ensure the `partition` argument narrows the Kafka request set rather than
  filtering only after row production
- ensure no `KafkaRecordSetProvider` / decoder / consumer-poll path is used

## Step 7. Add Tests And Documentation

Required outcome:

- semantic coverage for offset bounds
- validation coverage
- user-facing examples in docs

## Implementation Notes

### Ordering

The implementation should emit rows sorted by `partition_id` ascending when
convenient for determinism, but this is not a user-facing semantic guarantee.

Reason:

- stable output makes tests deterministic
- Trino does not guarantee row order without `ORDER BY`

Documentation and tests must therefore use `ORDER BY partition_id` whenever
deterministic ordering is required.

### Type Choice For `partition`

The optional `partition` argument remains a single `BIGINT`.

This is a deliberate v1 constraint:

- it covers the main optimization case
- it avoids complicating validation and analysis with arrays or sets
- it leaves room for a future extension without changing existing semantics

### Topic Validation Source

Topic visibility must be resolved through connector metadata first. Live Kafka
metadata is then used for partition validation and offset lookup.

This preserves the connector's existing topic exposure model while still using
live broker state for current partition and offset information.

### Authorization Scope

The function uses an intentionally table-object-level authorization model.

This is a product-level rule:

- a user who can access the connector-visible Kafka table object may call the
  function for that table
- the function does not require column-level `SELECT` permission on message or
  key columns
- column-level policies on Kafka payload columns do not restrict this function
- this is an intentional choice because the function returns broker metadata,
  not table payload data
- implementation should use the connector access-control API in a way that
  enforces access to the resolved `(schema_name, table_name)` object without
  depending on any specific payload-column set

## Test Plan

### Unit / Connector-Level Tests

Add focused tests for:

- function analysis with `schema_name` and `table_name`
- function analysis with `schema_name`, `table_name`, and `partition`
- invalid blank schema name
- invalid blank table name
- invalid negative partition
- invalid partition above `Integer.MAX_VALUE`
- connector-non-visible table rejection
- access-control rejection for a non-accessible table object
- success for an accessible table object even when payload-column policies
  would block ordinary table reads
- output descriptor shape
- table-function wiring registration
- handle and split serialization coverage for the table-function path

### Integration Tests With `TestingKafka`

Add broker-backed tests for:

- single-partition topic returns one row
- multi-partition topic returns one row per partition
- `partition => x` returns only the requested partition
- unknown live partition fails during split generation / execution planning
- empty partition returns:
  - `log_start_offset == log_end_offset`
  - `last_readable_offset IS NULL`
- retained topic where log start has advanced above zero returns the advanced
  low watermark correctly
- topic-level SQL aggregation over the function returns expected derived values
- a table whose `table_name` differs from Kafka `topicName` still resolves and
  returns the broker offsets for the mapped topic
- connector-visible table succeeds while a non-exposed broker topic is not
  reachable through the function, if the test fixture can represent both states

### Regression Tests For Semantics

Explicitly verify:

- no data scan semantics are required for correctness
- `log_end_offset` remains exclusive
- `last_readable_offset` is computed correctly
- `beginningOffsets(...)` and `endOffsets(...)` narrow to the requested
  partition set after topic partition discovery
- no `poll(...)` / decoder path is used for the function runtime

The last item is especially important because the user-facing reason for
supporting optional `partition` in v1 is to avoid unnecessary all-partition
lookups.

## Risks And Tradeoffs

### Moderate Risk: First Table Function In `trino-kafka`

This is the largest structural change.

Why it is acceptable:

- the repo already contains connector table function examples
- the feature is self-contained
- it does not require changing Kafka read semantics

### Low Risk: Optional `partition` Argument

This is not expected to be a major complexity source.

Why:

- the connector already works with `TopicPartition`
- the broker APIs already support querying an arbitrary partition subset
- validation is straightforward once partition metadata is available

### Low Risk: Broker Load

For ordinary partition counts this should remain lightweight.

Important nuance:

- all-partitions lookups scale with partition count
- they do not scale with message volume
- `partition => x` is the right optimization for large-partition-count topics

## Locked Decisions

This plan fixes the following decisions:

- use a connector table function, not hidden columns or a system table
- keep the function in schema `system`
- name the function `offsets`
- require `schema_name VARCHAR` and `table_name VARCHAR`
- support optional `partition BIGINT`
- reject `partition` values above `Integer.MAX_VALUE`
- perform explicit access-control checks during function analysis
- intentionally authorize at the connector-visible table-object level rather
  than the payload-column level
- intentionally allow the function when the user can access the connector-
  visible table object even if payload-column policies would restrict ordinary
  table reads
- use live broker metadata only after connector-visible schema/table
  resolution
- narrow `beginningOffsets(...)` and `endOffsets(...)` to the requested
  partition set when `partition` is provided
- use the dedicated table-function processor path, not the ordinary Kafka
  table-scan path
- do not use `applyTableFunction(...)` to rewrite this function into a table
  scan
- expose broker bounds only in v1
- report connector-consumer-visible offset bounds under the effective Kafka
  client configuration
- keep topic-level summary as derived SQL, not as a separate function mode

## Acceptance Criteria

The feature is complete when:

- a user can query all partitions for a connector-visible Kafka table through a
  Kafka table function
- a user can query one explicit partition through the optional `partition`
  argument
- the function works only for connector-visible schema/table pairs
- access control is enforced explicitly during function analysis using the
  plan's intentional table-object-level metadata authorization model
- the connector narrows `beginningOffsets(...)` and `endOffsets(...)` to the
  requested partition set when `partition` is provided
- no Kafka topic data scan is performed
- no consumer `poll(...)` or row decoder path is used
- the result schema clearly distinguishes inclusive start from exclusive end
- invalid topic/partition inputs fail clearly
- schema/table aliases to Kafka topic names are handled correctly
- tests cover empty partitions, advanced log start, visibility/access rules,
  intentional non-application of payload-column policies, table-function
  wiring/serialization, and single-partition narrowing behavior
- documentation includes example queries and explains that the function returns
  metadata snapshots, not topic rows
- documentation does not claim row-order guarantees without `ORDER BY`

## Explicitly Deferred Follow-Ups

The following are intentionally deferred beyond this plan:

- exposing consumer-group committed offsets alongside broker bounds
- accepting multiple partitions in one function call
- returning a separate topic-summary mode from the function itself
- adding timestamp-based metadata lookup variants
- introducing a Kafka system table in parallel with the function
