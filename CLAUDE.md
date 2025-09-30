# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Build Commands

### Basic Build
```bash
# Build without tests (fastest)
./mvnw clean install -DskipTests

# Build with validation (includes checkstyle)
./mvnw clean validate

# Build specific module
./mvnw clean install -pl :module-name -am -DskipTests

# Sort pom.xml files
./mvnw sortpom:sort

# Generate license headers
./mvnw license:format
```

### Docker Build
```bash
# Build Docker image
docker build -f image/Dockerfile -t trino:local .

# Build core Docker image
docker build -f core/docker/Dockerfile -t trino-core:local .
```

## Testing Commands

### Unit Tests
```bash
# Run all tests
./mvnw test

# Run tests for specific module
./mvnw test -pl :module-name

# Run specific test class
./mvnw test -Dtest=TestClassName

# Run with JUnit parallel execution (configured by default)
./mvnw test -Djunit.jupiter.execution.parallel.enabled=true
```

### Product Tests
```bash
# Use the helper script from testing/bin
testing/bin/ptl

# Or use the full launcher path
testing/trino-product-tests-launcher/bin/run-launcher

# Run specific test suite
testing/bin/ptl suite run --suite <suite-name>
```

## Development Server

### Running in IDE (IntelliJ)

1. **TpchQueryRunner** (simplest for development):
   - Main Class: `io.trino.tests.tpch.TpchQueryRunner`
   - Module: varies by connector being tested

2. **DevelopmentServer** (full server):
   - Main Class: `io.trino.server.DevelopmentServer`
   - VM Options: `-ea -Dconfig=etc/config.properties -Dlog.levels-file=etc/log.properties -Djdk.attach.allowAttachSelf=true`
   - Working directory: `$MODULE_DIR$`
   - Module: `trino-server-dev`

### CLI Connection
```bash
# Connect to running server
client/trino-cli/target/trino-cli-*-executable.jar

# Basic queries
SELECT * FROM system.runtime.nodes;
SELECT * FROM tpch.tiny.region;
```

## High-Level Architecture

### Core Components

**Server Core** (`core/trino-main`, `core/trino-server-main`):
- Main server bootstrapping and initialization
- Query execution engine with distributed SQL processing
- Coordinator and worker node management
- Session and transaction management
- Security framework with authentication/authorization

**SPI** (`core/trino-spi`):
- Service Provider Interface for plugin development
- Defines contracts for connectors, functions, and system components
- Type system and metadata interfaces

**Parser** (`core/trino-parser`, `core/trino-grammar`):
- SQL parsing using ANTLR4 grammar
- AST generation and semantic analysis
- Query planning and optimization framework

### Plugin System

**Connectors** (`plugin/trino-*`):
- Each connector is a separate Maven module
- Implements SPI interfaces for data source integration
- Key connectors: hive, iceberg, delta-lake, jdbc-based (mysql, postgresql, etc.)
- Each has its own `*QueryRunner` class for testing

**Event Listeners** (`plugin/trino-*-event-listener`):
- HTTP, Kafka, MySQL event listeners for query monitoring
- Implements EventListener SPI

**Resource Management** (`plugin/trino-resource-group-managers`):
- Dynamic resource group configuration
- Query queue management

### Libraries (`lib/`)

**Storage** (`lib/trino-filesystem*`, `lib/trino-hdfs`):
- Filesystem abstractions for S3, Azure, GCS, HDFS
- ORC and Parquet file format support
- Cache management

**Metastore** (`lib/trino-metastore`):
- Hive metastore client
- Table and partition metadata management

### Testing Infrastructure

**Testing Framework** (`testing/trino-testing`):
- Base test classes and utilities
- Docker-based test environments
- Distributed query testing support

**Product Tests** (`testing/trino-product-tests`):
- End-to-end integration tests using Docker
- Tempto test harness integration
- Multi-node cluster testing

### Module Dependencies

- Core modules depend on SPI
- Plugins depend on core modules and SPI
- Libraries are shared across plugins
- Testing modules can depend on any component

### Key Design Patterns

1. **Plugin Architecture**: All connectors and extensions are plugins loaded dynamically
2. **Guice Dependency Injection**: Used throughout for module composition
3. **Distributed Execution**: Coordinator-worker architecture for query processing
4. **Type System**: Strong typing with custom type registry
5. **Optimizer Rules**: Rule-based query optimization with cost-based decisions

## Code Style Requirements

- Use IntelliJ with Airlift codestyle
- No mocking libraries - write manual mocks
- Prefer AssertJ for test assertions
- Avoid `var` keyword
- Use Guava immutable collections
- Alphabetize documentation sections
- Format with `./mvnw sortpom:sort` for pom.xml files
- No abbreviations except well-known ones (max, min, ttl)

## Custom Plugins Location

Custom plugin JARs from `image/other_plugins/` and `plugin/` directories are included in the Docker image build.