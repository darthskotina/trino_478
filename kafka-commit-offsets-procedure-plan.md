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
- **Group ID resolution for the procedure:** explicit `group_id` argument wins; falls back to the renamed session property `consumer_group_id`; if both absent, fail. **The procedure must succeed even when `committed_read_enabled=true` and the session property is unset, as long as the explicit `group_id` argument is provided.**
- **Session property rename:** `committed_read_group_id` → `consumer_group_id`. Hard rename, no deprecated alias.
- **Default-mode read path uses the renamed property:** if `consumer_group_id` session property is set, default-mode reads use it as the consumer `group.id`; otherwise they fall back to the legacy catalog `kafka.consumer-group-id`.
- **Committed-read mode requires `consumer_group_id`** under the new name when the read path itself runs; failure mode is unchanged from today. The procedure is exempt — it has its own group-ID resolution.
- **Validation:** validate every requested offset against `[logStart, logEnd]` (inclusive on both ends, since `logEnd` legitimately means "caught up"); allow override via `allow_out_of_range BOOLEAN DEFAULT false`.
- **Partition-ownership convergence:** fail fast if `subscribe(topic) + poll` does not assign all requested partitions; one bounded retry (single rejoin) before failing; total wait timeout is a code-level constant.
- **Concurrency safety:** no in-Trino registry; document last-writer-wins.
- **Access control:** require both `checkCanExecuteProcedure(...)` and `checkCanSelectFromColumns(...)`.
- **Identity:** procedure consumer reuses connector SSL/TLS and bootstrap configuration via new methods on `KafkaConsumerFactory`.
- **Subscribe-mode commit mechanics:** after `subscribe(topic)` and the join-driving `poll(...)`, the procedure pauses all assigned partitions before issuing `commitSync(...)`. The procedure does **not** override `auto.offset.reset`; the Kafka client default applies. This avoids `NoOffsetForPartitionException` on first-time commits and on partitions with no group history.

## Implementation Steps

### 0. Refactor `KafkaConsumerFactory` and `KafkaOffsetBoundsService` to separate base properties from session-group resolution

This step is a prerequisite for everything that follows. Without it, the procedure cannot run when `committed_read_enabled=true` and the session group is unset (today, `DefaultKafkaConsumerFactory.configure(session)` throws `INVALID_SESSION_PROPERTY` in that case via `getEffectiveConsumerGroupId(session)`), and `KafkaOffsetBoundsService.getTopicPartitionOffsets(...)` inherits the same problem because it builds its consumer via `consumerFactory.create(session)`.

The refactor isolates the session-group lookup into a single explicit code path used only by the read pipeline. Metadata-only calls and the new procedure are routed through paths that take the group ID as a parameter (or do not need one).

**File:** `plugin/trino-kafka/src/main/java/io/trino/plugin/kafka/KafkaConsumerFactory.java`

Replace the existing single-method interface with:

```java
public interface KafkaConsumerFactory
{
    /**
     * Build the connection-level Kafka client properties. Does NOT set group.id.
     * Does NOT consult any session group-ID property. Safe to call from metadata
     * paths and from the commit_offsets procedure regardless of committed-read state.
     */
    Properties baseProperties(ConnectorSession session);

    /**
     * Build properties for the read pipeline. Resolves the effective group ID
     * by consulting session properties; throws INVALID_SESSION_PROPERTY when
     * committed-read mode is enabled but no group ID is supplied.
     */
    default Properties configure(ConnectorSession session)
    {
        Properties properties = baseProperties(session);
        properties.setProperty(GROUP_ID_CONFIG, resolveReadPathGroupId(session));
        return properties;
    }

    /**
     * Build properties for an explicit group ID. Bypasses session-group
     * resolution entirely. Used by the commit_offsets procedure.
     */
    default Properties configureForGroup(ConnectorSession session, String groupId)
    {
        requireNonNull(groupId, "groupId is null");
        Properties properties = baseProperties(session);
        properties.setProperty(GROUP_ID_CONFIG, groupId);
        return properties;
    }

    /**
     * Build properties for metadata-only operations that do not exercise group
     * membership (e.g. partitionsFor, beginningOffsets, endOffsets). Sets a
     * stable but unused group.id placeholder so that the Kafka client does not
     * complain; never used for commit or fetch.
     */
    default Properties configureForMetadata(ConnectorSession session)
    {
        Properties properties = baseProperties(session);
        properties.setProperty(GROUP_ID_CONFIG, METADATA_GROUP_ID_PLACEHOLDER);
        return properties;
    }

    String resolveReadPathGroupId(ConnectorSession session);

    default KafkaConsumer<byte[], byte[]> create(ConnectorSession session)
    {
        return new KafkaConsumer<>(configure(session));
    }

    default KafkaConsumer<byte[], byte[]> createForGroup(ConnectorSession session, String groupId)
    {
        return new KafkaConsumer<>(configureForGroup(session, groupId));
    }

    default KafkaConsumer<byte[], byte[]> createForMetadata(ConnectorSession session)
    {
        return new KafkaConsumer<>(configureForMetadata(session));
    }

    String METADATA_GROUP_ID_PLACEHOLDER = "trino-kafka-metadata";
}
```

Note: `METADATA_GROUP_ID_PLACEHOLDER` is a fixed string; metadata calls (`partitionsFor`, `beginningOffsets`, `endOffsets`) do not exercise the group coordinator at the broker level, so the placeholder value is never durable on the broker.

**File:** `plugin/trino-kafka/src/main/java/io/trino/plugin/kafka/DefaultKafkaConsumerFactory.java`

- Replace `configure(ConnectorSession)` with an implementation of `baseProperties(ConnectorSession)` that contains everything the current `configure` does **except** `GROUP_ID_CONFIG`.
- Implement `resolveReadPathGroupId(ConnectorSession)` with the new resolution order:
  1. If `KafkaSessionProperties.isCommittedReadEnabled(session)`, return `KafkaSessionProperties.getRequiredCommittedReadGroupId(session)` (unchanged behavior — committed-read mode still requires the property).
  2. Else if `KafkaSessionProperties.getConsumerGroupIdSessionProperty(session)` is present, return its value.
  3. Else return the legacy catalog `consumerGroupId` (`kafka.consumer-group-id`).

The interface's default `configure` method then composes `baseProperties` + `resolveReadPathGroupId` and preserves the existing read-path semantics.

**File:** `plugin/trino-kafka/src/main/java/io/trino/plugin/kafka/SslKafkaConsumerFactory.java`

The branch has a second `KafkaConsumerFactory` implementation that wraps a `@ForKafkaSsl` delegate (typically `DefaultKafkaConsumerFactory`) and overlays SSL client properties on top. Today it overrides the single-method `configure(...)`. After the interface refactor, that one override is no longer enough — `configureForGroup(...)` and `configureForMetadata(...)` would inherit the interface defaults and call `baseProperties(...)` on `SslKafkaConsumerFactory` (which is now an abstract method on the interface), causing a compile error. Even if it compiled, the SSL overlay would not flow into the new procedure or metadata paths.

Required changes:

- Override `baseProperties(ConnectorSession)`: take the delegate's `baseProperties(session)`, overlay the SSL `map` on top, return the result. This is the only place the SSL overlay needs to be applied — `configure`, `configureForGroup`, and `configureForMetadata` all compose on top of `baseProperties`, so SSL flows into every path automatically.
- Override `resolveReadPathGroupId(ConnectorSession)`: delegate to `delegate.resolveReadPathGroupId(session)`.
- **Delete the existing `configure(ConnectorSession)` override.** The interface's `default` `configure` now produces the same result (delegate `baseProperties` + SSL overlay + delegate `resolveReadPathGroupId`).

Verification: search the codebase for any other implementations of `KafkaConsumerFactory` before merging:

```bash
grep -rn "implements KafkaConsumerFactory" plugin/trino-kafka/src/
```

If a third implementation appears (custom plugin subclassing the connector), it must be updated the same way — override `baseProperties` and `resolveReadPathGroupId`, drop the `configure` override.

**File:** `plugin/trino-kafka/src/main/java/io/trino/plugin/kafka/KafkaOffsetBoundsService.java`

- Change `getTopicPartitionOffsets(session, ...)` to use `consumerFactory.createForMetadata(session)` instead of `consumerFactory.create(session)`. This makes the metadata path independent of committed-read session state.
- This means the existing `system.offsets` PTF will also now work correctly when committed-read mode is enabled and no session group is set. That is a strict improvement; document it in the README's `system.offsets` section if not already covered.

**File:** `plugin/trino-kafka/src/main/java/io/trino/plugin/kafka/KafkaSplitManager.java`

- The `getCommittedOffsets(session, groupId)` method uses `Admin`, not the consumer factory; it is unaffected.
- All other call sites of `consumerFactory.create(session)` are read-path uses and remain on the original method.

**Acceptance for step 0:** all existing tests still pass after this refactor alone, before any procedure code is added. This includes `TestKafkaCommittedReadMode`, default-mode tests, and `TestKafkaIntegration*`.

### 1. Rename session property `committed_read_group_id` → `consumer_group_id`

**File:** `plugin/trino-kafka/src/main/java/io/trino/plugin/kafka/KafkaSessionProperties.java`

- Rename the `COMMITTED_READ_GROUP_ID` constant to `CONSUMER_GROUP_ID` and its string value from `"committed_read_group_id"` to `"consumer_group_id"`.
- Rename the public accessor methods:
  - `getCommittedReadGroupId(ConnectorSession)` → `getConsumerGroupIdSessionProperty(ConnectorSession)` (returns `Optional<String>`).
  - `getRequiredCommittedReadGroupId(ConnectorSession)` is retained in name as a thin wrapper that calls the new getter and throws `INVALID_SESSION_PROPERTY` if absent. Update its error message to reference the new property name (`consumer_group_id`).
- Update the property description to: `"Kafka consumer group ID; required by committed-read mode and overrides the legacy default-mode group ID when set"`.
- Update the blank-value validation message to use the new property name.

**Files that call the renamed accessors (update call sites):**

- `plugin/trino-kafka/src/main/java/io/trino/plugin/kafka/DefaultKafkaConsumerFactory.java`
- `plugin/trino-kafka/src/main/java/io/trino/plugin/kafka/KafkaSplitManager.java`

### 2. Broaden `consumer_group_id` to default-mode reads

This is realized by `DefaultKafkaConsumerFactory.resolveReadPathGroupId(...)` from step 0. No additional change is needed beyond verifying that:

- A default-mode SELECT with `SET SESSION consumer_group_id = 'foo'` produces a consumer configured with `group.id = foo` (verified by unit test in §9.1).
- A default-mode SELECT with no session override still produces a consumer with `group.id = bigdata-spark-streaming` (or whatever the catalog `kafka.consumer-group-id` resolves to).
- A committed-read SELECT continues to require the session property and fail otherwise.

### 3. (Removed — folded into Step 0)

The procedure-specific factory method previously described here is now `configureForGroup(ConnectorSession, String)` from step 0. The procedure does **not** override `auto.offset.reset`; the Kafka client default (`latest`) applies. The procedure also does not set `client.id` or `group.instance.id`. `session.timeout.ms`, `heartbeat.interval.ms`, and `partition.assignment.strategy` are left at Kafka client defaults.

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

This delegates to the `getTopicPartitionOffsets(session, topicName, Optional.empty())` path (now metadata-safe per step 0) and returns a map keyed by partition for the requested partition set. If any requested partition does not exist in the topic's partition list, the method throws `TrinoException(KAFKA_SPLIT_ERROR, ...)` with a clear "Partition X does not exist for topic 'Y'" message.

This method is the only Kafka-side I/O performed by the validation path. Its cost is bounded by:
- one `Metadata` RPC (`partitionsFor`),
- one `ListOffsets` RPC per leader broker for `beginningOffsets`,
- one `ListOffsets` RPC per leader broker for `endOffsets`.

It does not scan messages.

### 5. Add the `Procedure` infrastructure to the Kafka connector

**New file:** `plugin/trino-kafka/src/main/java/io/trino/plugin/kafka/procedure/CommitOffsetsProcedure.java`

Pattern: model on `plugin/trino-iceberg/src/main/java/io/trino/plugin/iceberg/procedure/RollbackToSnapshotProcedure.java`. The implementing agent must inject `TypeManager` to construct the `MapType` for the `OFFSETS` argument, since `Procedure.Argument` requires a concrete `Type`.

Skeleton:

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
                        SqlMap.class,   // offsets (nullable)
                        Boolean.class));// allow_out_of_range (nullable)
    }

    private final KafkaConsumerFactory consumerFactory;
    private final KafkaOffsetBoundsService offsetBoundsService;
    private final MapType offsetsArgumentType;

    @Inject
    public CommitOffsetsProcedure(
            KafkaConsumerFactory consumerFactory,
            KafkaOffsetBoundsService offsetBoundsService,
            TypeManager typeManager)
    {
        this.consumerFactory = requireNonNull(consumerFactory, "consumerFactory is null");
        this.offsetBoundsService = requireNonNull(offsetBoundsService, "offsetBoundsService is null");
        this.offsetsArgumentType = (MapType) typeManager.getType(
                mapType(INTEGER.getTypeSignature(), BIGINT.getTypeSignature()));
    }

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
                        new Procedure.Argument("OFFSETS", offsetsArgumentType, false, null),
                        new Procedure.Argument("ALLOW_OUT_OF_RANGE", BOOLEAN, false, false)),
                COMMIT_OFFSETS.bindTo(this));
    }

    public void commitOffsets(...) { ... }
}
```

Notes for the implementing agent:
- The `Procedure.Argument` constructor used here is `(name, type, required, defaultValue)`. The literal types of the default values must match the argument type (`null` for nullable types; `false` `Boolean` for `BOOLEAN`). Verify against the actual `io.trino.spi.procedure.Procedure.Argument` constructors before writing tests; if the SPI exposes a different overload shape, adapt the call sites accordingly. The key point is that all five trailing arguments are declared optional with no required `null` defaults forced by the user.
- `mapType(INTEGER.getTypeSignature(), BIGINT.getTypeSignature())` returns a `TypeSignature` (from `io.trino.spi.type.TypeSignature`). Use the `TypeManager` to materialize it.

**Body of `commitOffsets(...)`** — execute in this order, all inside a `ThreadContextClassLoader`:

1. **Argument shape validation.**
   - Resolve `schemaName`, `tableName`. Reject blank/null.
   - Resolve `groupId`: explicit argument wins; else `KafkaSessionProperties.getConsumerGroupIdSessionProperty(session)`; else throw `TrinoException(INVALID_PROCEDURE_ARGUMENT, "group_id must be supplied either as a procedure argument or via session property 'consumer_group_id'")`.
   - Reject blank `groupId`.
   - **Form detection** — exactly one of the two forms must be specified. The truth table for `(partition, offset, offsets)`:
     - `(null, null, null)` → reject `INVALID_PROCEDURE_ARGUMENT` "must specify either partition+offset or offsets".
     - `(set, set, null)` → accept single-partition form.
     - `(null, null, set)` → accept map form.
     - `(set, null, null)` → reject `INVALID_PROCEDURE_ARGUMENT` "partition specified without offset; both must be supplied for the single-partition form".
     - `(null, set, null)` → reject `INVALID_PROCEDURE_ARGUMENT` "offset specified without partition; both must be supplied for the single-partition form".
     - any combination where `offsets` is set together with `partition` and/or `offset` → reject `INVALID_PROCEDURE_ARGUMENT` "must specify either partition+offset or offsets, not both".
   - If single-partition form: build `Map<Integer, Long> requested = Map.of(partition.intValue(), offset)`.
     - Reject `partition < 0`, `partition > Integer.MAX_VALUE`, `offset < 0`.
   - If map form: extract `Map<Integer, Long>` from the `SqlMap`. Reject empty map. Reject any null key/value, any key < 0, any key > Integer.MAX_VALUE, any value < 0.
   - `allowOutOfRange` defaults to `false` if null.

2. **Resolve the topic via the table description supplier.**
   - Construct `SchemaTableName(schemaName, tableName)`.
   - Call `offsetBoundsService.getTopicDescription(session, schemaTableName)`; throw `TableNotFoundException(schemaTableName)` if absent.
   - Extract `topicName` from the description.

3. **Access control.**
   - `accessControl.checkCanExecuteProcedure(null, new SchemaRoutineName("system", "commit_offsets"))`.
   - `accessControl.checkCanSelectFromColumns(null, schemaTableName, ImmutableSet.of())` (same shape as `OffsetBoundsTableFunction`).

4. **Bounds validation.**
   - Call `offsetBoundsService.getPartitionBounds(session, topicName, requested.keySet())`.
   - For every `(partition, offset)` in `requested`:
     - If the partition is missing in the result, propagate the "Partition X does not exist" error from the helper.
     - If `!allowOutOfRange` and `offset < bounds.logStart() || offset > bounds.logEnd()`, throw `TrinoException(INVALID_PROCEDURE_ARGUMENT, "Offset X for partition Y is outside the broker range [logStart, logEnd]; pass allow_out_of_range => true to override")`.

5. **Open a short-lived subscribe-mode consumer.**
   - Build properties via `consumerFactory.configureForGroup(session, groupId)`.
   - Construct `KafkaConsumer<byte[], byte[]>`.

6. **Subscribe and converge on partition ownership; pause; commit.**
   - `consumer.subscribe(Set.of(topicName))`.
   - Loop with deadline `now + MAX_WAIT_FOR_ASSIGNMENT`:
     - `consumer.poll(POLL_TIMEOUT)` (the poll drives the join/sync; we discard returned records).
     - If `consumer.assignment()` contains every requested partition (as `TopicPartition` instances on `topicName`), break the loop.
     - If `attempts < MAX_REJOIN_ATTEMPTS` and we have not yet rejoined, call `consumer.enforceRebalance()` once and continue polling; increment the attempt counter.
     - If the deadline is exceeded, throw `TrinoException(KAFKA_SPLIT_ERROR, "Could not acquire partitions [missing] for group ID 'X' on topic 'Y' within Zs; another consumer is likely a member of the same group")`.
   - **Immediately after assignment converges, call `consumer.pause(consumer.assignment())`.** This guarantees no subsequent `poll` triggers a fetch, eliminating any risk of `NoOffsetForPartitionException` for partitions with no committed offset for this group.
   - Build `Map<TopicPartition, OffsetAndMetadata>` from `requested`.
   - Call `consumer.commitSync(...)`.
   - On `KafkaException`, throw `TrinoException(KAFKA_SPLIT_ERROR, "Failed to commit Kafka offsets for group ID 'X' on topic 'Y'", e)`.

7. **Cleanup.**
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

The Multibinder must be created even if no procedures are bound, so that `Set<Procedure>` injection in `KafkaConnector` always succeeds.

**File:** `plugin/trino-kafka/src/main/java/io/trino/plugin/kafka/KafkaConnector.java`

- Add `private final Set<Procedure> procedures;` field.
- Inject `Set<Procedure> procedures` in the constructor; assign with `requireNonNull` and `ImmutableSet.copyOf`.
- Override `Set<Procedure> getProcedures()` to return the field.

### 7. Touch points and contract preservation

Re-verify after editing:

- `DefaultKafkaConsumerFactory.baseProperties(session)` still sets `enable.auto.commit=false`, deserializers, bootstrap, buffer size, key/value deserializer, and resource-file overrides — unchanged from the current `configure` body except that `GROUP_ID_CONFIG` is no longer set there.
- `DefaultKafkaConsumerFactory.configure(session)` (composed default) preserves the previous read-path semantics for both default and committed-read modes.
- `SslKafkaConsumerFactory.baseProperties(session)` overlays the SSL `map` on the delegate's base properties; the SSL overlay is now applied to every factory path (`configure`, `configureForGroup`, `configureForMetadata`) without per-method duplication.
- `SslKafkaConsumerFactory.configure(session)` (now the inherited default) produces the same property set as the previous explicit override.
- `KafkaSplitManager.getSplits(...)` still resolves the committed-read group ID via `KafkaSessionProperties.getRequiredCommittedReadGroupId(session)` after the rename (read pipeline unchanged).
- `KafkaCommittedReadRegistry.register(...)` still keys on the committed-read group ID and is unaffected by the rename.
- `KafkaRecordSet.close()` still gates commit on the same five conditions and uses `commitSync` on the same assign-mode consumer.
- `kafka.system.offsets` PTF (`OffsetBoundsFunction`) is unchanged at the call site; the underlying `getTopicPartitionOffsets` now uses the metadata-safe consumer path, which is a strict improvement.
- `KafkaFilterManager.validateNormalReadScope(...)` is unchanged.

### 8. Documentation update

**File:** `plugin/trino-kafka/README.md`

Add and update the following sections (alphabetize when reasonable, as per the project's style requirement):

- **Properties / Session properties:** rename `committed_read_group_id` to `consumer_group_id`. Update the description to note that it is required by committed-read mode and overrides the legacy default-mode group ID when set.
- **Effective Settings — Committed-read group ID resolution:** rename the property, otherwise unchanged.
- **Modes — Default mode:** add a bullet noting that the default-mode consumer group ID is `kafka.consumer-group-id` unless overridden by the session property `consumer_group_id`.
- **New section "Manual Offset Commit Procedure":**
  - Describe `kafka.system.commit_offsets` and its argument shapes (single-partition and map).
  - Document group ID resolution: explicit argument wins, then session property, else fail. Note explicitly that this is independent of `committed_read_enabled` and works even when no session group is set.
  - Document that the procedure uses subscribe-mode (joined-group) commit, not assign-mode commit, so broker ACL surface differs from default-mode reads.
  - Document the same-group concurrency caveat: a concurrent member in the same group will starve the procedure of partition ownership; the procedure fails fast with a clear message after `MAX_WAIT_FOR_ASSIGNMENT`.
  - Document the validation policy: offsets are rejected outside `[logStart, logEnd]` unless `allow_out_of_range => true`.
  - **New paragraph on `allow_out_of_range` interaction with `kafka.committed-read-missing-offset-policy`:**
    - With policy `ERROR` (default): a subsequent committed-read query against this group fails until the offset is reset, because the broker still holds an out-of-range value.
    - With policy `EARLIEST`: a subsequent committed-read query treats the out-of-range value as "no committed offset" and reads from `logStart`. The procedure-written value remains on the broker but is effectively ignored for planning.
    - With policy `LATEST`: a subsequent committed-read query produces a checkpoint-only split that snaps the committed offset to `clamp(filteredEnd, [logStart, logEnd])`, **overwriting** the procedure-written value silently.
    - Operator guidance: only use `allow_out_of_range => true` if you know your catalog policy and the next consumer's expectation. The procedure's value may not survive the next committed-read query.
  - Document idempotency: repeated calls with the same args produce the same broker state.
  - Provide three example calls: single-partition, multi-partition map, with `allow_out_of_range`.
- **Cross-references:** update "Recommended Usage" to note that the procedure is the recommended pattern for environments where assign-mode `OffsetCommit` is denied. Update "Modes" to cross-reference the procedure.
- **Compatibility rules:** update the existing Compatibility rules block to reflect the rename and the broadened scope.
- **README Scope:** unchanged; the section explicitly says this README is the source of truth for changes on the branch.

### 9. Testing

#### 9.1 Unit tests

##### 9.1.1 Procedure argument validation

**New file:** `plugin/trino-kafka/src/test/java/io/trino/plugin/kafka/procedure/TestCommitOffsetsProcedureValidation.java`

Tests are unit-only; they do not need a real Kafka broker. They construct the procedure with stubbed `KafkaConsumerFactory` and `KafkaOffsetBoundsService` collaborators and verify argument validation behavior. Use manual stubs (no mocking library, per the project style requirement).

Required cases — every case asserts on `TrinoException` error code and message substring:

- Schema name null / blank → `INVALID_PROCEDURE_ARGUMENT`.
- Table name null / blank → `INVALID_PROCEDURE_ARGUMENT`.
- Both `(partition, offset)` and `offsets` map supplied → `INVALID_PROCEDURE_ARGUMENT` mentioning "must specify either partition+offset or offsets, not both".
- `partition` set together with `offsets` (no `offset`) → `INVALID_PROCEDURE_ARGUMENT` mentioning "not both".
- `offset` set together with `offsets` (no `partition`) → `INVALID_PROCEDURE_ARGUMENT` mentioning "not both".
- Neither form supplied (all three null) → `INVALID_PROCEDURE_ARGUMENT` mentioning "must specify either partition+offset or offsets".
- `partition` set, `offset` null, `offsets` null → `INVALID_PROCEDURE_ARGUMENT` mentioning "partition specified without offset".
- `offset` set, `partition` null, `offsets` null → `INVALID_PROCEDURE_ARGUMENT` mentioning "offset specified without partition".
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
- **`group_id` argument set, `committed_read_enabled = true`, session `consumer_group_id` unset → procedure proceeds (does NOT fail with the committed-read-required error). This asserts that step 0's refactor isolates the procedure from the read-path session enforcement.**
- Schema/table not visible (table description supplier returns empty) → `TableNotFoundException`.
- Bounds validation: offset < `logStart` and `allow_out_of_range = false` → `INVALID_PROCEDURE_ARGUMENT`.
- Bounds validation: offset > `logEnd` and `allow_out_of_range = false` → `INVALID_PROCEDURE_ARGUMENT`.
- Bounds validation: offset = `logEnd` (caught up) → accepted.
- Bounds validation: offset = `logStart` → accepted.
- Bounds validation: offset out of range and `allow_out_of_range = true` → bypasses bounds check (verify the bounds service was consulted but did not reject).
- Bounds validation: offsets for a partition that does not exist → `KAFKA_SPLIT_ERROR` with "Partition X does not exist".

##### 9.1.2 Consumer factory group-ID resolution

**New file:** `plugin/trino-kafka/src/test/java/io/trino/plugin/kafka/TestDefaultKafkaConsumerFactoryGroupResolution.java`

Pure unit test against `DefaultKafkaConsumerFactory`. Uses a minimal `ConnectorSession` test double (or the existing one used by other Kafka unit tests) and asserts on the returned `Properties`.

Required cases:

- `committed_read_enabled = false`, no session `consumer_group_id` → `configure(session).getProperty(GROUP_ID_CONFIG)` equals the catalog `kafka.consumer-group-id` value.
- `committed_read_enabled = false`, session `consumer_group_id = 'override-group'` → `configure(session)` returns `group.id = 'override-group'`.
- `committed_read_enabled = true`, no session `consumer_group_id` → `configure(session)` throws `INVALID_SESSION_PROPERTY` (existing committed-read contract preserved).
- `committed_read_enabled = true`, session `consumer_group_id = 'cg'` → `configure(session)` returns `group.id = 'cg'`.
- `configureForGroup(session, 'explicit-group')`, `committed_read_enabled = true`, no session `consumer_group_id` → succeeds, returns `group.id = 'explicit-group'`. **This asserts step 0's invariant that procedure paths are independent of session-group enforcement.**
- `configureForMetadata(session)`, `committed_read_enabled = true`, no session `consumer_group_id` → succeeds, returns the placeholder group ID.
- `baseProperties(session)` does **not** include `GROUP_ID_CONFIG` regardless of session state.

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

7a. **Argument form: partition without offset.** Call with `partition => 0` and no `offset` and no `offsets`. Assert query fails with the expected message; broker shows no committed offset row.

7b. **Argument form: offset without partition.** Call with `offset => 100` and no `partition` and no `offsets`. Assert query fails; broker shows no committed offset row.

7c. **Argument form: mixed (partition+offsets).** Call with `partition => 0` and `offsets => MAP(...)`. Assert query fails; broker shows no committed offset row.

8. **Offset out of range, no override.** Call with `offset = logEnd + 1000`. Assert query fails; broker-side committed offset is unchanged from any prior state.

9. **Offset out of range with override → no consumer.** Call with `offset = logEnd + 1000` and `allow_out_of_range => true`. Assert broker shows the requested offset.

10. **Offset out of range with override × policy ERROR.** Catalog `kafka.committed-read-missing-offset-policy = ERROR`. Run scenario 9, then run a committed-read SELECT against the same group and topic. Assert the SELECT fails (out-of-range committed offset is treated as invalid by the read planner).

11. **Offset out of range with override × policy EARLIEST.** Same setup with policy `EARLIEST`. Run scenario 9, then run committed-read SELECT. Assert the SELECT succeeds and returns rows starting from `logStart`.

12. **Offset out of range with override × policy LATEST.** Same setup with policy `LATEST`. Run scenario 9, then run committed-read SELECT. Assert the SELECT produces a checkpoint-only split, returns 0 rows, and the broker-stored committed offset has been overwritten (no longer matches the procedure-written value).

13. **Offset for non-existent partition.** Call with a partition number greater than the topic's partition count. Assert query fails.

14. **Negative offset.** Call with `offset = -1`. Assert query fails.

15. **Caught-up commit.** Call with `offset = logEnd`. Assert success; broker shows the value.

16. **Empty partition, offset = 0.** Create topic with no rows. Call with `offset = 0`. Assert success; broker shows offset 0. **This explicitly exercises the first-time-commit case for a partition with no group history; it must succeed without `NoOffsetForPartitionException`.**

17. **First-time commit on a brand-new group.** Use a `group_id` that has never existed on the broker. Call with `offset = 0` on a non-empty topic. Assert success; broker shows the committed offset. This exercises the case where the consumer joins a group that has no prior offset metadata.

18. **Idempotent retry.** Call procedure twice with identical arguments. Assert second call succeeds; broker state matches a single call.

19. **After-the-fact commit followed by committed-read.** Run a default-mode SELECT against a topic. Call the procedure to commit `offset = logEnd`. Then run a committed-read SELECT with the same group ID; assert it returns 0 rows. Then append more messages to the topic and run committed-read SELECT again; assert it returns only the new messages.

20. **Procedure runs when committed-read mode is enabled and no session group is set.** Set `committed_read_enabled = true` (catalog or session) and ensure no `consumer_group_id` session property is set. Call the procedure with an explicit `group_id` argument. Assert the procedure succeeds. **This is the load-bearing assertion that step 0's refactor works end-to-end.**

21. **Concurrent external consumer in the same group.** Start an in-test Kafka consumer that calls `subscribe(topic)` and polls in a loop on the same group ID. Call the procedure. Assert the procedure fails fast with the "Could not acquire partitions" error message (not a hang, not longer than `MAX_WAIT_FOR_ASSIGNMENT + a margin`). Stop the external consumer in `@AfterEach` / `@AfterAll`.

22. **SSL/TLS path.** If the existing test fixture supports an SSL-enabled broker variant (check the `SslKafkaConsumerFactory` usage in `TestKafkaCommittedReadMode` or sibling tests), repeat scenario (1) over SSL to confirm the procedure picks up `kafka.config.resources` files. If no such fixture exists, skip — call this out in the test class's Javadoc.

23. **Access control: `checkCanExecuteProcedure` denies.** Use a test session whose access control denies execution. Assert the procedure call fails before any Kafka I/O is attempted (broker shows no committed offset row).

24. **Access control: `checkCanSelectFromColumns` denies.** Same shape, denying SELECT on the underlying table. Assert the procedure call fails.

#### 9.3 Regression coverage

Add or extend assertions to confirm pre-existing behavior is unchanged. These can live in existing test classes:

1. **Default-mode SELECT path — no commit, regardless of group ID.** Extend `TestKafkaCommittedReadMode` (or the closest existing default-mode test) with two cases:
   - Run a default-mode SELECT to completion with the legacy catalog group ID. Assert `Admin.listConsumerGroupOffsets(legacyGroupId)` does **not** contain a committed offset for the topic.
   - Run a default-mode SELECT to completion with `SET SESSION consumer_group_id = 'override-group'`. Assert `Admin.listConsumerGroupOffsets('override-group')` does **not** contain a committed offset for the topic. The session override changes the consumer's `group.id`, but default mode still does not commit.

   The override assertion at the broker level is **not** done via `describeConsumerGroups`. Manual-`assign(...)` consumers are not group members and do not register durable group metadata at the broker. The unit test in §9.1.2 is the load-bearing assertion that the override flows into `group.id`; the integration assertion here is the negative one (no commit).

2. **Default-mode SELECT path — falls back to legacy `kafka.consumer-group-id` when session unset.** Existing tests should already cover this; add an explicit `Admin.listConsumerGroupOffsets(legacyGroupId)` assertion if not.

3. **Committed-read SELECT — `consumer_group_id` is required.** Existing test should already assert this under the old name; rename and verify the error message references the new name.

4. **Committed-read SELECT — commit semantics unchanged.** Existing test in `TestKafkaCommittedReadMode` covers this; verify it still passes after the rename and the call-site updates.

5. **Committed-read SELECT — partial-read suppression unchanged.** Same; verify after rename.

6. **`kafka.system.offsets` PTF — unchanged.** Existing tests for `OffsetBoundsTableFunction` should pass without modification. Add one new assertion that the PTF works when `committed_read_enabled = true` and no session group is set (this exercises step 0's metadata-safe path).

7. **Read-scope enforcement — unchanged.** Existing tests pass without modification.

8. **SSL/TLS path for reads — unchanged.** Existing tests pass without modification.

#### 9.4 Test execution

Run the targeted suites:

```bash
./mvnw test -pl :trino-kafka -Dtest='TestKafkaCommittedReadMode,TestKafkaCommitOffsetsProcedure,TestCommitOffsetsProcedureValidation,TestDefaultKafkaConsumerFactoryGroupResolution,TestKafkaRecordSetCommittedRead,TestKafkaIntegration*'
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
3. The procedure succeeds when `committed_read_enabled = true` and no session `consumer_group_id` is set, as long as an explicit `group_id` argument is supplied (integration scenario 20 + unit scenario in §9.1.2 must pass).
4. The procedure succeeds for first-time commits on a brand-new group and for empty-partition cases (integration scenarios 16 and 17 must pass; no `NoOffsetForPartitionException` surfaces).
5. Out-of-range offsets are rejected by default and accepted when `allow_out_of_range => true`. The interaction between `allow_out_of_range` and each `kafka.committed-read-missing-offset-policy` value is documented in the README and verified by integration scenarios 10–12.
6. The procedure fails fast (without hanging past `MAX_WAIT_FOR_ASSIGNMENT`) when another consumer in the same group prevents partition ownership.
7. The procedure's consumer reuses the connector's existing SSL/TLS and bootstrap configuration; no new resource files are required. `SslKafkaConsumerFactory` is updated to override `baseProperties(...)` and `resolveReadPathGroupId(...)`, and the SSL overlay applies to every factory path (`configure`, `configureForGroup`, `configureForMetadata`) without per-path duplication.
8. The session property previously named `committed_read_group_id` is now `consumer_group_id`, and all references in code, tests, and the README use the new name.
9. The default-mode read path's consumer group ID resolves to the session property `consumer_group_id` when set, otherwise to the legacy catalog `kafka.consumer-group-id`. This is verified by `TestDefaultKafkaConsumerFactoryGroupResolution`.
10. Committed-read mode still requires `consumer_group_id` to be set (when committed-read mode is the read path); its error message references the new name. The procedure is exempt and works without it when given an explicit argument.
11. The `KafkaRecordSet` data-read path is byte-for-byte unchanged in behavior; existing committed-read tests pass without semantic modification (only string updates for the renamed property).
12. The `kafka.system.offsets` PTF is unchanged at the call site and now also works when committed-read mode is enabled with no session group.
13. Both `checkCanExecuteProcedure` and `checkCanSelectFromColumns` are enforced before any Kafka I/O.
14. The README contains a "Manual Offset Commit Procedure" section, cross-references from "Recommended Usage" and "Modes", reflects the property rename in "Properties", "Effective Settings", and "Compatibility rules", and documents the `allow_out_of_range` × policy interaction.
15. Targeted test suites pass; `./mvnw clean validate -pl :trino-kafka -am` passes.

## Risks To Watch

- **Step 0 scope.** The factory refactor touches the read path on every query. Run the full Kafka test suite before and after the refactor commit alone, before adding the procedure. If anything regresses, fix step 0 first; do not stack the procedure on top of a broken base.
- **SSL factory delegation.** Forgetting to override `baseProperties(...)` on `SslKafkaConsumerFactory` will cause a compile failure (because the interface method is abstract). Forgetting to override `resolveReadPathGroupId(...)` will silently route read-path group resolution through the delegate's logic — which is correct in the only current configuration but is surprising if a future variant overrides resolution differently. Override both explicitly even though `resolveReadPathGroupId` could fall back to a default delegating to the wrapped factory.
- **`enforceRebalance` semantics.** `KafkaConsumer.enforceRebalance()` is a hint, not a guarantee. If the test environment shows that one rejoin attempt is insufficient under load, increase `MAX_REJOIN_ATTEMPTS` rather than the per-poll timeout — multiple short rejoins converge faster than one long wait.
- **`pause(...)` timing.** `consumer.pause(consumer.assignment())` must be called after the assignment is fully populated; calling it before the join completes is a no-op. Verify by integration test that `commitSync` after `pause` does not regress (it should not — `pause` only affects fetch, not commit).
- **`Procedure.Argument` constructor surface.** The skeleton uses a `(name, type, required, defaultValue)` shape. If the actual SPI in this Trino version exposes `Procedure.Argument(String, Type)` plus separate optionality plumbing, adapt to the available shape. The implementing agent should grep for existing usages in the Trino codebase before writing the procedure (Iceberg's procedures are good references).
- **`SqlMap` decoding subtlety.** Trino `SqlMap` access requires reading the keys/values via the type-specific accessor on a raw block. Verify with a unit test that the decoder rejects negative keys and null entries cleanly. If `mapType(INTEGER, BIGINT)` is awkward to decode in a procedure body, fall back to `mapType(BIGINT, BIGINT)` and validate the integer range manually — this changes the user-facing argument type but keeps the user contract identical (Kafka partition IDs fit in `INTEGER` regardless).
- **Hard rename surface area.** Any operator script using `SET SESSION committed_read_group_id` will raise `INVALID_SESSION_PROPERTY`. Document loudly in the README and the commit message that this is a hard rename.
- **Behavioral change in default-mode reads.** Users with broker-side ACLs that grant `Group:Read` only on the legacy group ID may see default-mode reads fail with broker-side authorization errors when they SET SESSION `consumer_group_id`. This is intended behavior — the user took explicit action — but it should be called out in the README.
- **`ThreadContextClassLoader`.** The procedure must wrap the entire body in a `ThreadContextClassLoader` (see `RollbackToSnapshotProcedure.rollbackToSnapshot`). Forgetting this will cause Kafka client class-loading failures at runtime under the plugin classloader.
- **`getProcedures()` plumbing.** Adding `Set<Procedure>` injection to `KafkaConnector` is a real Guice change. Ensure the `Multibinder` is created (via `newSetBinder(binder, Procedure.class)`) so an empty set is injected even if no procedures are bound — necessary for tests that assemble a minimal connector.
- **`allow_out_of_range` policy interaction.** The override lets the user write a value the broker accepts but the connector's read planner cannot. Operators who use the override without understanding the catalog policy will be surprised. The README paragraph and integration scenarios 10–12 are the main mitigation.
- **`describeConsumerGroups` is not a regression signal for default-mode group-ID overrides.** Manual-`assign` consumers do not register as group members. The default-mode override is verified by unit test (`TestDefaultKafkaConsumerFactoryGroupResolution`) and by negative integration assertion ("no commit, regardless of group ID"), not by broker-side group inspection.

## Out Of Scope (explicitly deferred)

- Auto-commit-on-load in default mode.
- Automatic procedure invocation at end-of-query.
- Switching `KafkaRecordSet` from assign-mode `commitSync` to subscribe-mode `commitSync`.
- A dedicated Kafka identity (different cert/principal) for the procedure.
- Per-Trino concurrency protection between procedure callers.
- Deprecated alias for `committed_read_group_id`.
