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
FROM TABLE(kafka.system.offsets(topic => 'orders'));
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
FROM TABLE(kafka.system.offsets(topic => 'orders'));
```

### Arguments

Required argument:

- `topic VARCHAR`

Optional argument:

- `partition BIGINT`

Chosen semantics:

- if `partition` is omitted, return one row per partition for the topic
- if `partition` is provided, return exactly one row for that partition
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
FROM TABLE(kafka.system.offsets(topic => 'orders'));
```

Inspect one partition directly through the function parameter:

```sql
SELECT *
FROM TABLE(kafka.system.offsets(topic => 'orders', partition => 2));
```

Get only the topic-wide summary:

```sql
SELECT
    min(log_start_offset) AS topic_min_log_start_offset,
    max(log_end_offset) AS topic_max_log_end_offset
FROM TABLE(kafka.system.offsets(topic => 'orders'));
```

Get the current last readable offset per partition:

```sql
SELECT partition_id, last_readable_offset
FROM TABLE(kafka.system.offsets(topic => 'orders'));
```

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
FROM TABLE(kafka.system.offsets(topic => 'orders', partition => 2));
```

the connector should narrow Kafka calls to that single requested partition
rather than fetching all partitions and filtering later in Trino.

This avoids an API shape where:

- SQL says "one partition"
- but Kafka still sees "all partitions"

The optional parameter therefore improves both usability and operational
behavior for high-partition-count topics.

## Final Architecture

## 1. Add A Kafka Connector Table Function

Add the first table function to `trino-kafka` using the same general connector
pattern used in plugins such as OpenSearch.

High-level pieces likely required:

- a provider class for the function definition
- a function handle carrying resolved arguments
- connector wiring so `KafkaConnector#getTableFunctions()` returns the function
- metadata wiring so `KafkaMetadata#applyTableFunction(...)` maps the handle to
  a connector table handle and output columns

This is the largest implementation slice of the feature.

## 2. Introduce A Dedicated Table Handle For Offset-Bounds Reads

Do not overload existing Kafka table scans or committed-read handles.

Chosen approach:

- define a dedicated handle representing an offset-bounds function invocation
- include:
  - topic name
  - optional partition id
- keep this logically separate from the ordinary `KafkaTableHandle` used for
  message scans

Reason:

- function execution is metadata-only and should stay isolated from message
  scan semantics
- a dedicated handle keeps planning and testing simpler

## 3. Factor Out Broker Offset Lookup

The connector already resolves topic partitions and begin/end offsets in
`KafkaSplitManager`.

Chosen refactor:

- extract a small connector-local service or helper responsible for:
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
  - topic name
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

## Validation Rules

Required validations:

- `topic` must be non-null and non-blank
- the topic must exist
- if `partition` is provided:
  - it must be non-negative
  - it must exist in the topic's current partition set

Failure handling rules:

- clear distinction between:
  - unknown topic
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
- failure cases

These choices are locked by this plan so tests and docs align from the start.

## Step 2. Wire Table Function Support Into `KafkaConnector`

Files likely involved:

- `plugin/trino-kafka/src/main/java/io/trino/plugin/kafka/KafkaConnector.java`
- Kafka module/binding classes

Required outcome:

- the connector advertises the new table function

## Step 3. Add Function Definition And Handle

Add a dedicated function provider and handle class.

Required outcome:

- the function accepts `topic`
- the function optionally accepts `partition`
- analysis produces a stable output descriptor and handle

## Step 4. Extend Metadata To Apply The Function

Files likely involved:

- `plugin/trino-kafka/src/main/java/io/trino/plugin/kafka/KafkaMetadata.java`

Required outcome:

- metadata recognizes the function handle
- metadata maps it to the execution path and output columns

## Step 5. Implement Broker Offset Lookup Helper

Files likely involved:

- a new helper/service under `plugin/trino-kafka/...`
- `KafkaSplitManager` call sites if refactored to reuse it

Required outcome:

- all-partitions lookup path
- single-partition lookup path
- stable validation and ordering

## Step 6. Implement Row Production For The Function

Required outcome:

- return one row per requested partition
- populate `partition_id`, `log_start_offset`, `log_end_offset`,
  `last_readable_offset`
- ensure the `partition` argument narrows the Kafka request set rather than
  filtering only after row production

## Step 7. Add Tests And Documentation

Required outcome:

- semantic coverage for offset bounds
- validation coverage
- user-facing examples in docs

## Implementation Notes

### Ordering

Return rows ordered by `partition_id` ascending.

Reason:

- stable output makes tests deterministic
- users inspecting the function output get predictable ordering

### Type Choice For `partition`

The optional `partition` argument remains a single `BIGINT`.

This is a deliberate v1 constraint:

- it covers the main optimization case
- it avoids complicating validation and analysis with arrays or sets
- it leaves room for a future extension without changing existing semantics

### Topic Validation Source

Topic existence and partition validation should be based on the live Kafka
metadata returned for the topic at execution time.

This avoids creating a separate connector-side registry for topic metadata and
keeps behavior aligned with the broker snapshot used for the offset lookup.

## Test Plan

### Unit / Connector-Level Tests

Add focused tests for:

- function analysis with `topic` only
- function analysis with `topic` and `partition`
- invalid blank topic
- invalid negative partition
- unknown partition rejection
- output descriptor shape

### Integration Tests With `TestingKafka`

Add broker-backed tests for:

- single-partition topic returns one row
- multi-partition topic returns one row per partition
- `partition => x` returns only the requested partition
- empty partition returns:
  - `log_start_offset == log_end_offset`
  - `last_readable_offset IS NULL`
- retained topic where log start has advanced above zero returns the advanced
  low watermark correctly
- topic-level SQL aggregation over the function returns expected derived values

### Regression Tests For Semantics

Explicitly verify:

- no data scan semantics are required for correctness
- `log_end_offset` remains exclusive
- `last_readable_offset` is computed correctly
- single-partition parameter path narrows the requested Kafka partition set

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
- require `topic VARCHAR`
- support optional `partition BIGINT`
- narrow Kafka lookup to the requested partition set when `partition` is
  provided
- return one row per partition, ordered by `partition_id`
- expose broker bounds only in v1
- keep topic-level summary as derived SQL, not as a separate function mode
- use a dedicated offset-bounds handle rather than reusing `KafkaTableHandle`

## Acceptance Criteria

The feature is complete when:

- a user can query all partitions for a topic through a Kafka table function
- a user can query one explicit partition through the optional `partition`
  argument
- the connector fetches only the requested partition set from Kafka
- no Kafka topic data scan is performed
- the result schema clearly distinguishes inclusive start from exclusive end
- invalid topic/partition inputs fail clearly
- tests cover empty partitions, advanced log start, and single-partition
  narrowing behavior
- documentation includes example queries and explains that the function returns
  metadata snapshots, not topic rows

## Explicitly Deferred Follow-Ups

The following are intentionally deferred beyond this plan:

- exposing consumer-group committed offsets alongside broker bounds
- accepting multiple partitions in one function call
- returning a separate topic-summary mode from the function itself
- adding timestamp-based metadata lookup variants
- introducing a Kafka system table in parallel with the function
