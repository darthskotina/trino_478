# Kafka `client.rack` Session Property Implementation Plan

## Goal

Add a Kafka connector session property that controls Kafka consumer `client.rack`
per session. The setting must apply only to Kafka consumers created by the
connector, and must not affect Kafka admin or producer clients.

Kafka reference behavior: `client.rack` is the consumer client rack identifier
used by Kafka for rack-aware replica selection.

## Public Contract

- Add a catalog session property for the Kafka connector. Suggested name:
  `client_rack`.
- The session property maps to Kafka consumer config key `client.rack`
  (`ConsumerConfig.CLIENT_RACK_CONFIG`).
- Default session value should be `am1`.
- Supported values to document for users:
  - `am1` for `NL`
  - `am2` for `AGS`
  - `ld7` for `LD`
- The session property description should list those values so users can
  discover them through Trino session property metadata, for example via
  `SHOW SESSION`.
- With the default session value, Kafka consumers should use `client.rack=am1`
  unless the session property is explicitly set to another value or blank.
- If set to a non-blank string, the session value overrides any `client.rack`
  supplied by `kafka.config.resources`.
- If set to a blank string, the session property disables the setting for that
  session by removing `client.rack` from the consumer properties, including a
  value inherited from `kafka.config.resources`.
- The property applies to all connector consumer creation paths:
  - normal table reads
  - metadata consumers
  - explicit group consumers used by procedures
  - committed-read mode consumers
- The property must not be written into `DefaultKafkaAdminFactory` or
  `DefaultKafkaProducerFactory` properties.

## Relevant Existing Code

- `plugin/trino-kafka/src/main/java/io/trino/plugin/kafka/KafkaSessionProperties.java`
  owns Kafka catalog session property definitions and typed accessors.
- `plugin/trino-kafka/src/main/java/io/trino/plugin/kafka/DefaultKafkaConsumerFactory.java`
  builds base consumer properties from:
  - `kafka.config.resources`
  - connector-level bootstrap and serializer/deserializer settings
  - connector read buffer settings
- `plugin/trino-kafka/src/main/java/io/trino/plugin/kafka/KafkaConsumerFactory.java`
  derives all concrete consumer configurations from `baseProperties(session)`.
- `plugin/trino-kafka/src/main/java/io/trino/plugin/kafka/SslKafkaConsumerFactory.java`
  overlays SSL settings on the delegate consumer factory. Since SSL delegates to
  `baseProperties(session)`, the new setting should naturally flow through SSL.
- `DefaultKafkaAdminFactory` and `DefaultKafkaProducerFactory` also read
  `kafka.config.resources`; they should remain unchanged unless tests need to
  prove the consumer-only contract.

## Implementation Steps

1. In `KafkaSessionProperties`:
   - Add a private constant for the session property name, recommended
     `CLIENT_RACK = "client_rack"`.
   - Add a `stringProperty` entry to `sessionProperties`.
   - Use default value `"am1"`.
   - Use a description similar to:

     ```text
     Kafka consumer client.rack value. Supported values: am1 for NL, am2 for AGS, ld7 for LD. Empty string disables client.rack for the session.
     ```

   - Do not reject blank values; blank values are meaningful because they clear
     the inherited Kafka setting for the session.
   - Add an accessor such as:

     ```java
     public static Optional<String> getClientRack(ConnectorSession session)
     {
         return Optional.ofNullable(session.getProperty(CLIENT_RACK, String.class));
     }
     ```

2. In `DefaultKafkaConsumerFactory.baseProperties(ConnectorSession session)`:
   - Continue loading `configurationProperties` first.
   - Set all fixed connector consumer properties as before.
   - Apply the session override at the end of `baseProperties`, after all
     catalog/resource and fixed consumer properties. This makes override/removal
     semantics robust if future code adds a fixed `client.rack` assignment.

     ```java
     KafkaSessionProperties.getClientRack(session)
             .ifPresent(clientRack -> {
                 if (clientRack.isBlank()) {
                     properties.remove(CLIENT_RACK_CONFIG);
                 }
                 else {
                     properties.setProperty(CLIENT_RACK_CONFIG, clientRack);
                 }
             });
     ```

   - Import `org.apache.kafka.clients.consumer.ConsumerConfig.CLIENT_RACK_CONFIG`.
   - Place this immediately before returning the final `Properties`.

3. Do not change:
   - `DefaultKafkaAdminFactory`
   - `DefaultKafkaProducerFactory`
   - SSL admin/producer factories
   - Kafka client module bindings

4. Update documentation:
   - Add the new session property to the Kafka connector documentation in
     `docs/src/main/sphinx/connector/kafka.md`.
   - Document that it overrides `client.rack` from `kafka.config.resources` for
     consumers only, and that an empty string disables/removes the setting for
     the session.
   - Include catalog-qualified examples so users see this is a Kafka catalog
     session property, not a global session property:

     ```sql
     SET SESSION kafka.client_rack = 'ld7';
     SET SESSION kafka.client_rack = '';
     ```

   - If branch-local operator notes remain relevant, also update
     `plugin/trino-kafka/README.md`.

## Test Plan

Add focused unit tests rather than a full Kafka integration test. The behavior is
client property construction, not broker behavior.

Recommended tests:

1. Extend or add a test near
   `TestDefaultKafkaConsumerFactoryGroupResolution`.
   - Build a default `ConnectorSession` and assert
     `new DefaultKafkaConsumerFactory(config).baseProperties(session)` contains
     `client.rack = am1`.
   - Build a `ConnectorSession` with `client_rack = "rack-a"`.
   - Assert `new DefaultKafkaConsumerFactory(config).baseProperties(session)`
     contains `client.rack = rack-a`.
   - Assert `configure(session)`, `configureForGroup(session, "...")`, and
     `configureForMetadata(session)` also contain the same value.

2. Test override of `kafka.config.resources`.
   - Create a temporary properties file containing `client.rack=rack-from-file`.
   - Configure `KafkaConfig.setResourceConfigFiles(...)`.
   - With a default session, assert `baseProperties(session)` contains `am1`,
     proving the session default overrides the catalog resource value.
   - With `client_rack = "rack-from-session"`, assert the consumer properties
     contain `rack-from-session`.

3. Test blank session value clears inherited setting.
   - Use a resource config file with `client.rack=rack-from-file`.
   - Build a session with `client_rack = ""`.
   - Assert consumer properties do not contain `client.rack`.

4. Test whitespace-only session value clears inherited setting.
   - Use a resource config file with `client.rack=rack-from-file`.
   - Build a session with `client_rack = "   "` or `client_rack = "\t"`.
   - Assert consumer properties do not contain `client.rack`.

5. Test SSL consumer path.
   - Extend `TestSslKafkaConsumerFactoryGroupResolution` or add a new test.
   - Assert `SslKafkaConsumerFactory` preserves the session-derived
     `client.rack` while adding SSL properties.

6. Test consumer-only scope.
   - Add lightweight tests for `DefaultKafkaAdminFactory` and
     `DefaultKafkaProducerFactory` with the same session and a resource file.
   - Use a resource config file containing `client.rack=rack-from-file`.
   - With session `client_rack = "rack-from-session"`, assert admin and producer
     properties still contain `client.rack = rack-from-file`.
   - With session `client_rack = ""`, assert admin and producer properties still
     contain `client.rack = rack-from-file`.
   - This verifies the session property neither overrides nor removes
     `client.rack` for non-consumer Kafka clients. This is required because
     `DefaultKafkaAdminFactory` and `DefaultKafkaProducerFactory` load
     `kafka.config.resources` directly.

Suggested command:

```bash
./mvnw -pl plugin/trino-kafka test -Dtest='TestDefaultKafkaConsumerFactoryGroupResolution,TestSslKafkaConsumerFactoryGroupResolution'
```

If new test classes are introduced, include them in the `-Dtest` list.

## Edge Cases And Decisions

- Blank session string means clear/remove `client.rack`; do not validate it as
  an error.
- Whitespace-only strings should also clear/remove the setting because
  `String.isBlank()` treats them as blank.
- `am1` is the default session value and should therefore override a different
  catalog-level `client.rack` supplied by `kafka.config.resources`.
- The implementation should document `am1`, `am2`, and `ld7` in the property
  description, but the plan does not require hard validation to only those
  values unless product requirements change. Avoid blocking future Kafka rack
  names unnecessarily.
- Non-blank values should not be trimmed unless a broader Trino session property
  convention requires it. Preserve the exact configured value.
- The session property is intentionally named with an underscore for normal SQL
  usability. It maps to Kafka's dotted config key internally.
- Existing `kafka.config.resources` behavior remains catalog-wide and applies to
  all Kafka client types. The new session property only changes consumer
  properties.
- Because all consumer creation paths call `baseProperties(session)`, a single
  change in `DefaultKafkaConsumerFactory` should cover read, metadata, procedure,
  and committed-read consumers.
