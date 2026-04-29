# Kafka Connector — `commit_offsets` Procedure Implementation Plan

## Goal

Add a new connector procedure `kafka.system.commit_offsets` that lets a caller commit consumer-group offsets to Kafka via a brief subscribe-mode session, independently of the read path. As part of the same change, rename the existing `committed_read_group_id` session property to a mode-agnostic `consumer_group_id` and broaden its scope so it also overrides the default-mode read group ID.

The procedure exists because in the operator's Kafka deployment, manual-assign `OffsetCommit` is denied while subscribe-mode (joined-group) `OffsetCommit` is allowed for the same principal. The connector's existing read path (manual `assign(...)` for parallelism) remains unchanged; the procedure is the explicit, user-driven write surface.

## Non-Goals

- Do not change the connector's data-read path (`assign(...)` + `seek(...)` + `commitSync` in `KafkaRecordSet`) for either default mode or committed-read mode.
- Do not auto-call this procedure at the end of default-mode SELECTs.
- Do not wire the procedure into committed-read mode's automatic commit path.
- Do not change `kafka.consumer-group-id` (catalog property) semantics for default-mode reads beyond making it overridable by the renamed session property.
- Do not introduce per-Trino concurrency protection between procedure callers (last-writer-wins is the documented Kafka semantic).
- Do not introduce a separate Kafka identity (cert/principal) for the procedure — it inherits the connector's existing SSL/TLS resource configuration.

## Working Decisions (locked in)

- **Procedure name:** `kafka.system.commit_offsets`.
- **Argument shape:** both single-partition form and multi-partition map form supported in one signature; reject if neither or both are supplied.
- **Schema/table strictness:** the procedure resolves `(schema_name, table_name)` through the connector's `TableDescriptionSupplier`. Topic-only or arbitrary-topic-name calls are rejected.
- **Group ID resolution for the procedure:** explicit `group_id` argument wins; falls back to the renamed session property `consumer_group_id`; if both absent, fail.
- **Session property rename:** `committed_read_group_id` → `consumer_group_id`. Hard rename, no deprecated alias.
- **Default-mode read path uses the renamed property:** if `consumer_group_id` session property is set, default-mode reads use it as the consumer `group.id`; otherwise they fall back to the legacy catalog `kafka.consumer-group-id`.
- **Committed-read mode requires `consumer_group_id`** under the new name; failure mode is unchanged from today.
- **Validation:** validate every requested offset against `[logStart, logEnd]` (inclusive on both ends, since `logEnd` legitimately means "caught up"); allow override via `allow_out_of_range BOOLEAN DEFAULT false`.
- **Partition-ownership convergence:** fail fast if `subscribe(topic) + poll` does not assign all requested partitions; one bounded retry (single rejoin) before failing; total wait timeout is a code-level constant.
- **Concurrency safety:** no in-Trino registry; document last-writer-wins.
- **Access control:** require both `checkCanExecuteProcedure(...)` and `checkCanSelectFromColumns(...)`.
- **Identity:** procedure consumer reuses connector SSL/TLS and bootstrap configuration via a new `KafkaConsumerFactory.configureForGroup(...)` method.

## Implementation Steps

### 1. Rename session property `committed_read_group_id` → `consumer_group_id`

**File:** `plugin/trino-kafka/src/main/java/io/trino/plugin/kafka/KafkaSessionProperties.java`

- Rename the `COMMITTED_READ_GROUP_ID` constant to `CONSUMER_GROUP_ID` and its string value from `"committed_read_group_id"` to `"consumer_group_id"`.
- Rename the public accessor methods:
  - `getCommittedReadGroupId(ConnectorSession)` → `getConsumerGroupIdSessionProperty(ConnectorSession)` (returns `Optional<String>`).
  - `getRequiredCommittedReadGroupId(ConnectorSession)` → `getRequiredCommittedReadGroupId(ConnectorSession)` retained in name only as a wrapper that calls the new getter and throws `INVALID_SESSION_PROPERTY` if absent. Update its error message to reference the new property name.
- Update the property description to: `"Kafka consumer group ID; required by committed-read mode and overrides the legacy default-mode group ID when set"`.
- Update the blank-value validation message to use the new name.

**Files that call the renamed accessors (update call sites):**

- `plugin/trino-kafka/src/main/java/io/trino/plugin/kafka/DefaultKafkaConsumerFactory.java`
- `plugin/trino-kafka/src/main/java/io/trino/plugin/kafka/KafkaSplitManager.java`

### 2. Broaden `consumer_group_id` to default-mode reads

**File:** `plugin/trino-kafka/src/main/java/io/trino/plugin/kafka/DefaultKafkaConsumerFactory.java`

Replace `getEffectiveConsumerGroupId(ConnectorSession)` resolution with:

1. If `KafkaSessionProperties.isCommittedReadEnabled(session)` is true, return `KafkaSessionProperties.getRequiredCommittedReadGroupId(session)` (unchanged behavior — committed-read mode still requires the property to be set).
2. Otherwise, if `KafkaSessionProperties.getConsumerGroupIdSessionProperty(session)` is present, return it.
3. Otherwise, return the legacy catalog `consumerGroupId` (`kafka.consumer-group-id`).

This is a behavioral change for default-mode reads: a session-set `consumer_group_id` will now flow into the consumer's `group.id`. The change is intentional and is documented in the README update (step 8).

### 3. Add a group-ID override path on `KafkaConsumerFactory`

**Files:**
- `plugin/trino-kafka/src/main/java/io/trino/plugin/kafka/KafkaConsumerFactory.java`
- `plugin/trino-kafka/src/main/java/io/trino/plugin/kafka/DefaultKafkaConsumerFactory.java`

Add a default method on the interface:

```java
default Properties configureForGroup(ConnectorSession session, String groupId)
{
    Properties properties = configure(session);
    properties.setProperty(GROUP_ID_CONFIG, groupId);
    return properties;
}
```

The default implementation delegates to `configure(session)` and overrides the `group.id`. This keeps SSL/TLS, bootstrap servers, deserializers, buffer size, `enable.auto.commit=false`, and resource-file overrides flowing through the existing path. Custom `KafkaConsumerFactory` implementations (if any) inherit the override behavior automatically.

Add a sibling default method that lets callers opt out of consumer subscription via `auto.offset.reset=none` for the procedure case:

```java
default Properties configureForOffsetCommit(ConnectorSession session, String groupId)
{
    Properties properties = configureForGroup(session, groupId);
    properties.setProperty(AUTO_OFFSET_RESET_CONFIG, "none");
    return properties;
}
```

`session.timeout.ms`, `heartbeat.interval.ms`, `partition.assignment.strategy`, `client.id`, and `group.instance.id` are intentionally left at Kafka client defaults.

### 4. Add the offset-bounds validation helper

**File:** `plugin/trino-kafka/src/main/java/io/trino/plugin/kafka/KafkaOffsetBoundsService.java`

Add a method:

```java
public Map<TopicPartition, OffsetBounds> getPartitionBounds(
        ConnectorSession session,
        String topicName,
        Set<Integer> requestedPartitions);

public record OffsetBounds(long logStart, long logEnd) {}
```

This delegates to `getTopicPartitionOffsets(session, topicName, Optional.empty())` and returns a map keyed by partition for the requested partition set. If any requested partition does not exist in the topic's partition list, the method throws `TrinoException(KAFKA_SPLIT_ERROR, ...)` with a clear "Partition X does not exist for topic 'Y'" message.

This method is the only Kafka-side I/O performed by the validation path. Its cost is bounded by:
- one `Metadata` RPC (`partitionsFor`),
- one `ListOffsets` RPC per leader broker for `beginningOffsets`,
- one `ListOffsets` RPC per leader broker for `endOffsets`.

It does not scan messages.

### 5. Add the `Procedure` infrastructure to the Kafka connector

**New file:** `plugin/trino-kafka/src/main/java/io/trino/plugin/kafka/procedure/CommitOffsetsProcedure.java`

Pattern: model on `plugin/trino-iceberg/src/main/java/io/trino/plugin/iceberg/procedure/RollbackToSnapshotProcedure.java`. Skeleton:

```java
public class CommitOffsetsProcedure
        implements Provider<Procedure>
{
    private static final MethodHandle COMMIT_OFFSETS;
    private static final Duration POLL_TIMEOUT = Duration.ofSeconds(10);
    private static final Duration MAX_WAIT_FOR_ASSIGNMENT = Duration.ofSeconds(30);
    private static final int MAX_REJOIN_ATTEMPTS = 1;

    static {
        COMMIT_OFFSETS = lookup().unreflect(
                CommitOffsetsProcedure.class.getMethod(
                        "commitOffsets",
                        ConnectorSession.class,
                        ConnectorAccessControl.class,
                        String.class,   // schema_name
                        String.class,   // table_name
                        String.class,   // group_id (nullable)
                        Long.class,     // partition (nullable)
                        Long.class,     // offset (nullable)
                        SqlMap.class,   // offsets (nullable, map<int, bigint>)
                        Boolean.class));// allow_out_of_range (nullable)
    }

    private final KafkaConsumerFactory consumerFactory;
    private final KafkaOffsetBoundsService offsetBoundsService;

    @Inject
    public CommitOffsetsProcedure(
            KafkaConsumerFactory consumerFactory,
            KafkaOffsetBoundsService offsetBoundsService) { ... }

    @Override
    public Procedure get()
    {
        return new Procedure(
                "system",
                "commit_offsets",
                ImmutableList.of(
                        new Procedure.Argument("SCHEMA_NAME", VARCHAR),
                        new Procedure.Argument("TABLE_NAME", VARCHAR),
                        new Procedure.Argument("GROUP_ID", VARCHAR, false, null),
                        new Procedure.Argument("PARTITION", BIGINT, false, null),
                        new Procedure.Argument("OFFSET", BIGINT, false, null),
                        new Procedure.Argument("OFFSETS", mapType(INTEGER, BIGINT), false, null),
                        new Procedure.Argument("ALLOW_OUT_OF_RANGE", BOOLEAN, false, false)),
                COMMIT_OFFSETS.bindTo(this));
    }

    public void commitOffsets(...) { ... }
}
```

**Body of `commitOffsets(...)`** — execute in this order, all inside a `ThreadContextClassLoader`:

1. **Argument shape validation.**
   - Resolve `schemaName`, `tableName`. Reject blank/null.
   - Resolve `groupId`: explicit argument wins; else `KafkaSessionProperties.getConsumerGroupIdSessionProperty(session)`; else throw `TrinoException(INVALID_PROCEDURE_ARGUMENT, "group_id must be supplied either as a procedure argument or via session property 'consumer_group_id'")`.
   - Reject blank `groupId`.
   - Reject if both `(partition, offset)` and `offsets` are supplied; reject if neither is supplied.
   - If single-partition form: build `Map<Integer, Long> requested = Map.of(partition.intValue(), offset)`.
     - Reject `partition < 0`, `partition > Integer.MAX_VALUE`, `offset < 0`.
   - If map form: extract `Map<Integer, Long>` from the `SqlMap`. Reject empty map. Reject any null key/value, any key < 0, any key > Integer.MAX_VALUE, any value < 0.
   - `allowOutOfRange` defaults to `false` if null.

2. **Resolve the topic via the table description supplier.**
   - Construct `SchemaTableName(schemaName, tableName)`.
   - Call `offsetBoundsService.getTopicDescription(session, schemaTableName)`; throw `TableNotFoundException(schemaTableName)` if absent. (Same path as `OffsetBoundsTableFunction.analyze`.)
   - Extract `topicName` from the description.

3. **Access control.**
   - `accessControl.checkCanExecuteProcedure(null, new SchemaRoutineName("system", "commit_offsets"))`.
   - `accessControl.checkCanSelectFromColumns(null, schemaTableName, ImmutableSet.of())` (same shape as `OffsetBoundsTableFunction`).

4. **Bounds validation.**
   - Call `offsetBoundsService.getPartitionBounds(session, topicName, requested.keySet())`.
   - For every `(partition, offset)` in `requested`:
     - If the partition is missing in the result, throw "Partition X does not exist for topic 'Y'".
     - If `!allowOutOfRange` and `offset < bounds.logStart() || offset > bounds.logEnd()`, throw `TrinoException(INVALID_PROCEDURE_ARGUMENT, "Offset X for partition Y is outside the broker range [logStart, logEnd]; pass allow_out_of_range => true to override")`.

5. **Open a short-lived subscribe-mode consumer.**
   - Build properties via `consumerFactory.configureForOffsetCommit(session, groupId)`.
   - Construct `KafkaConsumer<byte[], byte[]>`.

6. **Subscribe and converge on partition ownership.**
   - `consumer.subscribe(Set.of(topicName))`.
   - Loop with deadline `now + MAX_WAIT_FOR_ASSIGNMENT`:
     - `consumer.poll(POLL_TIMEOUT)` (the poll drives the join/sync; we discard returned records — they shouldn't appear because we never set a position, but if they do, we ignore them).
     - If `consumer.assignment()` contains every requested partition (as `TopicPartition` instances on `topicName`), break.
     - If `attempts < MAX_REJOIN_ATTEMPTS`, call `consumer.enforceRebalance()` once and continue polling; increment the attempt counter.
     - If the deadline is exceeded, throw `TrinoException(KAFKA_SPLIT_ERROR, "Could not acquire partitions [missing] for group ID 'X' on topic 'Y' within Zs; another consumer is likely a member of the same group")`.

7. **Commit.**
   - Build `Map<TopicPartition, OffsetAndMetadata>` from `requested`.
   - Call `consumer.commitSync(...)`.
   - On `KafkaException`, throw `TrinoException(KAFKA_SPLIT_ERROR, "Failed to commit Kafka offsets for group ID 'X' on topic 'Y'", e)`.

8. **Cleanup.**
   - In a `finally`, call `consumer.unsubscribe()` and `consumer.close()`. Suppress and chain exceptions consistent with the pattern in `KafkaRecordSet.close()`.

### 6. Bind the procedure into Guice and expose it via `KafkaConnector`

**File:** `plugin/trino-kafka/src/main/java/io/trino/plugin/kafka/KafkaConnectorModule.java`

Add to `setup(Binder binder)`:

```java
import io.trino.plugin.kafka.procedure.CommitOffsetsProcedure;
import io.trino.spi.procedure.Procedure;
...
newSetBinder(binder, Procedure.class).addBinding().toProvider(CommitOffsetsProcedure.class).in(Scopes.SINGLETON);
```

**File:** `plugin/trino-kafka/src/main/java/io/trino/plugin/kafka/KafkaConnector.java`

- Add `private final Set<Procedure> procedures;` field.
- Inject `Set<Procedure> procedures` in the constructor; assign with `requireNonNull` and `ImmutableSet.copyOf`.
- Override `Set<Procedure> getProcedures()` to return the field. (The default `Connector.getProcedures()` returns `ImmutableSet.of()`.)

### 7. Touch points and contract preservation

Re-verify after editing:

- `DefaultKafkaConsumerFactory.configure(session)` still sets `enable.auto.commit=false`, deserializers, bootstrap, buffer size, key/value deserializer, and resource-file overrides for both default and committed-read modes.
- `KafkaSplitManager.getSplits(...)` still resolves the committed-read group ID via `KafkaSessionProperties.getRequiredCommittedReadGroupId(session)` after the rename.
- `KafkaCommittedReadRegistry.register(...)` still keys on the committed-read group ID and is unaffected by the rename.
- `KafkaRecordSet.close()` still gates commit on the same five conditions and uses `commitSync` on the same assign-mode consumer.
- `kafka.system.offsets` PTF (`OffsetBoundsFunction`) is unchanged.
- `KafkaFilterManager.validateNormalReadScope(...)` is unchanged.

### 8. Documentation update

**File:** `plugin/trino-kafka/README.md`

Add and update the following sections (alphabetize when reasonable, as per the project's style requirement):

- **Properties / Session properties:** rename `committed_read_group_id` to `consumer_group_id`. Update the description to note that it is required by committed-read mode and overrides the legacy default-mode group ID when set.
- **Effective Settings — Committed-read group ID resolution:** rename the property, otherwise unchanged.
- **Modes — Default mode:** add a bullet noting that the default-mode consumer group ID is `kafka.consumer-group-id` unless overridden by the session property `consumer_group_id`.
- **New section "Manual Offset Commit Procedure":**
  - Describe `kafka.system.commit_offsets` and its argument shapes (single-partition and map).
  - Document group ID resolution: explicit argument wins, then session property, else fail.
  - Document that the procedure uses subscribe-mode (joined-group) commit, not assign-mode commit, so broker ACL surface differs from default-mode reads.
  - Document the same-group concurrency caveat: a concurrent member in the same group will starve the procedure of partition ownership; the procedure fails fast with a clear message.
  - Document the validation policy: offsets are rejected outside `[logStart, logEnd]` unless `allow_out_of_range => true`.
  - Document idempotency: repeated calls with the same args produce the same broker state.
  - Provide three example calls: single-partition, multi-partition map, with `allow_out_of_range`.
- **Cross-references:** update "Recommended Usage" to note that the procedure is the recommended pattern for environments where assign-mode `OffsetCommit` is denied. Update "Modes" to cross-reference the procedure.
- **Compatibility rules:** update the existing Compatibility rules block to reflect the rename and the broadened scope.
- **README Scope:** unchanged; the section explicitly says this README is the source of truth for changes on the branch.

### 9. Testing

#### 9.1 Unit tests

**New file:** `plugin/trino-kafka/src/test/java/io/trino/plugin/kafka/procedure/TestCommitOffsetsProcedureValidation.java`

Tests are unit-only; they do not need a real Kafka broker. They construct the procedure with stubbed `KafkaConsumerFactory` and `KafkaOffsetBoundsService` collaborators and verify argument validation behavior. Use manual stubs (no mocking library, per the project style requirement).

Required cases — every case asserts on `TrinoException` error code and message substring:

- Schema name null / blank → `INVALID_PROCEDURE_ARGUMENT`.
- Table name null / blank → `INVALID_PROCEDURE_ARGUMENT`.
- Both `(partition, offset)` and `offsets` map supplied → `INVALID_PROCEDURE_ARGUMENT` mentioning "must specify either partition+offset or offsets, not both".
- Neither form supplied → `INVALID_PROCEDURE_ARGUMENT` mentioning "must specify either partition+offset or offsets".
- Single-partition: negative partition → `INVALID_PROCEDURE_ARGUMENT`.
- Single-partition: partition > `Integer.MAX_VALUE` → `INVALID_PROCEDURE_ARGUMENT`.
- Single-partition: negative offset → `INVALID_PROCEDURE_ARGUMENT`.
- Map: empty map → `INVALID_PROCEDURE_ARGUMENT`.
- Map: null key / null value (where Kafka's `SqlMap` permits) → `INVALID_PROCEDURE_ARGUMENT`.
- Map: negative partition → `INVALID_PROCEDURE_ARGUMENT`.
- Map: negative offset → `INVALID_PROCEDURE_ARGUMENT`.
- `group_id` argument blank → `INVALID_PROCEDURE_ARGUMENT`.
- `group_id` argument null and session `consumer_group_id` absent → `INVALID_PROCEDURE_ARGUMENT` mentioning fallback to session property.
- `group_id` argument null but session `consumer_group_id` set → resolves to session value (verify by capturing the value the stub factory was called with).
- `group_id` argument set and session `consumer_group_id` also set → explicit argument wins (verify by captured value).
- Schema/table not visible (table description supplier returns empty) → `TableNotFoundException`.
- Bounds validation: offset < `logStart` and `allow_out_of_range = false` → `INVALID_PROCEDURE_ARGUMENT`.
- Bounds validation: offset > `logEnd` and `allow_out_of_range = false` → `INVALID_PROCEDURE_ARGUMENT`.
- Bounds validation: offset = `logEnd` (caught up) → accepted.
- Bounds validation: offset = `logStart` → accepted.
- Bounds validation: offset out of range and `allow_out_of_range = true` → bypasses bounds check (verify the bounds service was consulted but did not reject).
- Bounds validation: offsets for a partition that does not exist → `KAFKA_SPLIT_ERROR` (or whichever code `KafkaOffsetBoundsService.getPartitionBounds` raises) with "Partition X does not exist".

#### 9.2 Integration tests

**New file:** `plugin/trino-kafka/src/test/java/io/trino/plugin/kafka/TestKafkaCommitOffsetsProcedure.java`

Pattern: model on `TestKafkaCommittedReadMode`. Uses the existing Kafka test container fixture and `Admin.listConsumerGroupOffsets` to verify broker-side state. All assertions on broker state, not on connector internals.

Required scenarios:

1. **Happy path single-partition.** Create topic with N rows, call procedure with `(partition=0, offset=N)`, assert `Admin.listConsumerGroupOffsets(groupId)` shows offset N for partition 0.

2. **Happy path multi-partition.** Create topic with multiple partitions; call procedure once with `offsets => MAP(...)` covering all partitions. Assert broker shows the committed offsets for every partition.

3. **Group ID via session property.** Set session `consumer_group_id`. Call procedure without `group_id` argument. Assert broker shows offset under the session-supplied group.

4. **Explicit group ID overrides session property.** Set session `consumer_group_id = 'session-group'`. Call procedure with `group_id => 'arg-group'`. Assert broker shows offset under `'arg-group'` and not `'session-group'`.

5. **Group ID neither in argument nor session.** Call procedure without `group_id`. Assert query fails with the expected message.

6. **Group ID blank argument.** Call with `group_id => ''`. Assert query fails.

7. **Schema/table not visible.** Call with a non-existent table name. Assert `TableNotFoundException`.

8. **Offset out of range, no override.** Call with `offset = logEnd + 1000`. Assert query fails; broker-side committed offset is unchanged from any prior state.

9. **Offset out of range with override.** Call same as above but with `allow_out_of_range => true`. Assert broker shows the requested offset.

10. **Offset for non-existent partition.** Call with a partition number greater than the topic's partition count. Assert query fails.

11. **Negative offset.** Call with `offset = -1`. Assert query fails.

12. **Caught-up commit.** Call with `offset = logEnd`. Assert success; broker shows the value.

13. **Empty partition, offset = 0.** Create topic with no rows. Call with `offset = 0`. Assert success; broker shows offset 0.

14. **Idempotent retry.** Call procedure twice with identical arguments. Assert second call succeeds; broker state matches a single call.

15. **After-the-fact commit followed by committed-read.** Run a default-mode SELECT against a topic. Call the procedure to commit `offset = logEnd`. Then run a committed-read SELECT with the same group ID; assert it returns 0 rows (the resume point is at the end). Then append more messages to the topic and run committed-read SELECT again; assert it returns only the new messages.

16. **Concurrent external consumer in the same group.** Start an in-test Kafka consumer that calls `subscribe(topic)` and polls in a loop on the same group ID. Call the procedure. Assert the procedure fails fast with the "Could not acquire partitions" error message (not a hang). Stop the external consumer in `@AfterEach` / `@AfterAll`.

17. **SSL/TLS path.** If the existing test fixture supports an SSL-enabled broker variant (check the `SslKafkaConsumerFactory` usage in `TestKafkaCommittedReadMode` or sibling tests), repeat scenario (1) over SSL to confirm the procedure picks up `kafka.config.resources` files. If no such fixture exists, skip — call this out in the test class's Javadoc.

18. **Access control: `checkCanExecuteProcedure` denies.** Use a test session whose access control denies execution. Assert the procedure call fails before any Kafka I/O is attempted (broker shows no committed offset row).

19. **Access control: `checkCanSelectFromColumns` denies.** Same shape, denying SELECT on the underlying table. Assert the procedure call fails.

#### 9.3 Regression coverage

Add or extend assertions to confirm pre-existing behavior is unchanged. These can live in existing test classes:

1. **Default-mode SELECT path — no commit.** Extend `TestKafkaCommittedReadMode` (or the closest existing default-mode test) with: run a default-mode SELECT to completion, assert `Admin.listConsumerGroupOffsets(legacyGroupId)` does not contain a committed offset for the topic.

2. **Default-mode SELECT path — uses session `consumer_group_id` if set.** New test in the integration suite: `SET SESSION consumer_group_id = 'override-group'`, run a default-mode SELECT, assert from the broker side that the consumer's group metadata reflects `'override-group'`. (One way to verify is to call `Admin.describeConsumerGroups(...)` and inspect the group state, or to assert that no offset is committed under `legacyGroupId` and the override group exists with no committed offset rows but a recorded group metadata row. Choose whichever is cleanest with the existing fixture.)

3. **Default-mode SELECT path — falls back to legacy `kafka.consumer-group-id` when session unset.** Existing tests should already cover this; add a single explicit assertion if not.

4. **Committed-read SELECT — `consumer_group_id` is required.** Existing test should already assert this under the old name; rename and verify the error message references the new name.

5. **Committed-read SELECT — commit semantics unchanged.** Existing test in `TestKafkaCommittedReadMode` covers this; verify it still passes after the rename and the call-site updates.

6. **Committed-read SELECT — partial-read suppression unchanged.** Same; verify after rename.

7. **`kafka.system.offsets` PTF — unchanged.** Existing tests for `OffsetBoundsTableFunction` should pass without modification.

8. **Read-scope enforcement — unchanged.** Existing tests pass without modification.

9. **SSL/TLS path for reads — unchanged.** Existing tests pass without modification.

#### 9.4 Test execution

Run the targeted suites:

```bash
./mvnw test -pl :trino-kafka -Dtest='TestKafkaCommittedReadMode,TestKafkaCommitOffsetsProcedure,TestCommitOffsetsProcedureValidation,TestKafkaRecordSetCommittedRead,TestKafkaIntegration*'
```

If the broader Kafka suite has additional tests that reference `committed_read_group_id` by string literal, run the full Kafka module:

```bash
./mvnw test -pl :trino-kafka
```

Also run the project validation step to catch checkstyle issues:

```bash
./mvnw clean validate -pl :trino-kafka -am
```

## Acceptance Criteria

The change is complete when all of the following are true:

1. `kafka.system.commit_offsets` is callable with both single-partition and map argument forms, and either form successfully commits offsets to Kafka against a real broker (verified via `Admin.listConsumerGroupOffsets`).
2. The procedure's group ID resolution follows the documented order: explicit argument → session `consumer_group_id` → fail.
3. Out-of-range offsets are rejected by default and accepted when `allow_out_of_range => true`.
4. The procedure fails fast (without hanging past `MAX_WAIT_FOR_ASSIGNMENT`) when another consumer in the same group prevents partition ownership.
5. The procedure's consumer reuses the connector's existing SSL/TLS and bootstrap configuration; no new resource files are required.
6. The session property previously named `committed_read_group_id` is now `consumer_group_id`, and all references in code, tests, and the README use the new name.
7. The default-mode read path's consumer group ID resolves to the session property `consumer_group_id` when set, otherwise to the legacy catalog `kafka.consumer-group-id`.
8. Committed-read mode still requires `consumer_group_id` to be set; its error message references the new name.
9. The `KafkaRecordSet` data-read path is byte-for-byte unchanged in behavior; existing committed-read tests pass without semantic modification (only string updates for the renamed property).
10. The `kafka.system.offsets` PTF is unchanged.
11. Both `checkCanExecuteProcedure` and `checkCanSelectFromColumns` are enforced before any Kafka I/O.
12. The README contains a "Manual Offset Commit Procedure" section, cross-references from "Recommended Usage" and "Modes", and reflects the property rename in "Properties", "Effective Settings", and "Compatibility rules".
13. Targeted test suites pass; `./mvnw clean validate -pl :trino-kafka -am` passes.

## Risks To Watch

- **Partition-ownership convergence flakiness.** The integration test for "concurrent external consumer in the same group" depends on the external consumer actually holding the partition. Use a deterministic setup: start the external consumer, await its `assignment()` non-empty in the test, then call the procedure. Stop the consumer in cleanup.
- **`SqlMap` decoding subtlety.** Trino `SqlMap` access requires reading via the type-specific accessor on a raw block; verify with a small ad-hoc test that the decoder rejects negative keys and null entries cleanly. If `mapType(INTEGER, BIGINT)` is awkward to decode in a procedure, fall back to `mapType(BIGINT, BIGINT)` and validate the integer range manually.
- **Subscribe-mode `poll` timeout vs heartbeat.** With Kafka client defaults (`session.timeout.ms=10s`, `heartbeat.interval.ms=3s`), a `MAX_WAIT_FOR_ASSIGNMENT` of 30s leaves room for two heartbeat cycles. If the rebalance routinely takes longer in the test environment, raise the constant — do not lower the heartbeat.
- **`auto.offset.reset=none` on a subscribe-mode consumer.** If the consumer ever calls `poll` while it has assignment but no committed offset, `none` causes an exception. The procedure deliberately never reads — it only commits — so this should not surface, but if Kafka changes behavior to read on rebalance, the integration test will catch it. The test for "empty partition, offset = 0" exercises this corner.
- **Hard rename surface area.** Any operator script using `SET SESSION committed_read_group_id` will silently no-op (Trino ignores unknown session properties on `SET SESSION` only if a property of that name doesn't exist; with a hard rename it raises `INVALID_SESSION_PROPERTY`). Document loudly in the README and the commit message that this is a hard rename.
- **Behavioral change in default-mode reads.** Users with broker-side ACLs that grant `Group:Read` only on the legacy group ID may see default-mode reads start failing with broker-side authorization errors when they SET SESSION `consumer_group_id`. This is intended behavior — the user took explicit action — but it should be called out in the README.
- **`ThreadContextClassLoader`.** The procedure must wrap the entire body in a `ThreadContextClassLoader` (see `RollbackToSnapshotProcedure.rollbackToSnapshot`). Forgetting this will cause Kafka client class-loading failures at runtime under the plugin classloader.
- **`getProcedures()` plumbing.** Adding `Set<Procedure>` injection to `KafkaConnector` is a real Guice change. Ensure the `Multibinder` is created (via `newSetBinder(binder, Procedure.class)`) so an empty set is injected even if no procedures are bound — necessary for tests that assemble a minimal connector.

## Out Of Scope (explicitly deferred)

- Auto-commit-on-load in default mode.
- Automatic procedure invocation at end-of-query.
- Switching `KafkaRecordSet` from assign-mode `commitSync` to subscribe-mode `commitSync`.
- A dedicated Kafka identity (different cert/principal) for the procedure.
- Per-Trino concurrency protection between procedure callers.
- Deprecated alias for `committed_read_group_id`.
