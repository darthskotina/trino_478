# CTE Materialization Implementation Plan

## Architecture Analysis Summary

After thorough research of Trino's execution architecture, I've identified the key execution flow:

1. **Query Dispatch**: `DispatchManager.createQuery()` → `LocalDispatchQueryFactory.createDispatchQuery()`
2. **Execution Selection**: Based on statement type, either `SqlQueryExecution` or `DataDefinitionExecution`
3. **Statement Classification**: Statements are categorized in `StatementUtils` as either:
   - Basic statements (SELECT, INSERT, CTAS) → `SqlQueryExecution`
   - Data definition statements (CREATE TABLE, DROP TABLE) → `DataDefinitionExecution`
4. **Execution Pipeline**: Each query is a single statement - no multi-statement execution support
5. **Query Lifecycle**: Tracked by `QueryStateMachine` with cleanup handlers

## Core Challenge & Solution Approach

**Challenge**: Trino doesn't support multi-statement execution within a single query. CTE materialization requires orchestrating multiple operations (CREATE TABLE, main query, DROP TABLE).

**Solution**: Implement a new `CteQueryExecution` that intercepts CTE-containing queries and orchestrates the full materialization lifecycle internally.

## Implementation Strategy

### Phase 1: Core Architecture Setup

#### 1.1 Create CTE Query Execution
**File**: `core/trino-main/src/main/java/io/trino/execution/CteQueryExecution.java`

```java
public class CteQueryExecution implements QueryExecution {
    private final SqlQueryExecution delegateExecution;
    private final CteTemporaryTableManager tempTableManager;
    private final QueryStateMachine stateMachine;
    
    // Orchestrates: CREATE temp tables → execute transformed query → DROP temp tables
}
```

Key responsibilities:
- Analyze query for CTEs that should be materialized
- Create temporary tables for selected CTEs
- Transform original query to use temp tables
- Execute the transformed query via delegation
- Clean up temporary tables on completion/failure

#### 1.2 CTE Analysis and Decision Engine
**File**: `core/trino-main/src/main/java/io/trino/execution/cte/CteAnalyzer.java`

```java
public class CteAnalyzer {
    public CteAnalysisResult analyzeQuery(Statement statement, Session session);
    
    // Determines which CTEs should be materialized based on:
    // - Reference count (>= threshold)
    // - Estimated cost/complexity
    // - Session properties configuration
}
```

#### 1.3 Temporary Table Management
**File**: `core/trino-main/src/main/java/io/trino/execution/cte/CteTemporaryTableManager.java`

```java
public class CteTemporaryTableManager {
    public CompletableFuture<List<QualifiedObjectName>> createTemporaryTables(
        List<CteDefinition> ctes, Session session);
    
    public CompletableFuture<Void> dropTemporaryTables(
        List<QualifiedObjectName> tableNames, Session session);
    
    // Manages temp table lifecycle using existing CREATE/DROP table infrastructure
}
```

### Phase 2: Query Transformation

#### 2.1 AST Transformation
**File**: `core/trino-main/src/main/java/io/trino/execution/cte/CteQueryTransformer.java`

Transform original query by:
- Removing materialized CTE definitions from WITH clause
- Replacing CTE references with temp table references
- Preserving non-materialized CTEs

Example transformation:
```sql
-- Original
WITH expensive_cte AS (SELECT * FROM big_table WHERE complex_condition),
     simple_cte AS (SELECT 1)
SELECT * FROM expensive_cte e1 JOIN expensive_cte e2 ON e1.id = e2.parent_id

-- Transformed (expensive_cte materialized)
WITH simple_cte AS (SELECT 1)  
SELECT * FROM delta.temp_schema.temp_table_12345 e1 
JOIN delta.temp_schema.temp_table_12345 e2 ON e1.id = e2.parent_id
```

### Phase 3: Integration Points

#### 3.1 Execution Factory Integration
**File**: `core/trino-main/src/main/java/io/trino/server/QueryExecutionFactoryModule.java`

Modify the execution binding logic:

```java
public class CteQueryExecutionFactory implements QueryExecutionFactory<CteQueryExecution> {
    @Override
    public CteQueryExecution createQueryExecution(...) {
        // Check if query contains materializable CTEs
        if (shouldMaterializeCtes(preparedQuery, session)) {
            return new CteQueryExecution(...);
        } else {
            return delegate.createQueryExecution(...);  // Fall back to normal execution
        }
    }
}
```

Bind this factory for SELECT statements when CTE materialization is enabled.

#### 3.2 Session Properties
**File**: `core/trino-main/src/main/java/io/trino/SystemSessionProperties.java`

Add configuration properties:
```java
public static final String CTE_MATERIALIZATION_ENABLED = "cte_materialization_enabled";
public static final String CTE_MATERIALIZATION_THRESHOLD = "cte_materialization_threshold";  
public static final String CTE_MATERIALIZATION_CATALOG = "cte_materialization_catalog";
public static final String CTE_MATERIALIZATION_SCHEMA = "cte_materialization_schema";
```

### Phase 4: DDL Execution Integration

#### 4.1 Internal DDL Execution
**File**: `core/trino-main/src/main/java/io/trino/execution/cte/InternalDdlExecutor.java`

```java
public class InternalDdlExecutor {
    public CompletableFuture<Void> executeCreateTableAsSelect(
        QualifiedObjectName tableName, Query query, Session session);
    
    public CompletableFuture<Void> executeDropTable(
        QualifiedObjectName tableName, Session session);
    
    // Uses DataDefinitionTask infrastructure internally
    // Executes DDL operations within the same transaction context
}
```

Key implementation details:
- Reuse existing `CreateTableTask` and `DropTableTask` infrastructure
- Execute DDL within the query's transaction context
- Handle failures and rollback scenarios
- Ensure proper access control and permissions

#### 4.2 Transaction and Session Context Management

```java
public class CteExecutionContext {
    private final Session originalSession;
    private final TransactionId transactionId;
    private final List<QualifiedObjectName> createdTables = new ArrayList<>();
    
    public Session createTableCreationSession();
    public void registerCreatedTable(QualifiedObjectName tableName);
    public CompletableFuture<Void> cleanup();  // Drop all created tables
}
```

### Phase 5: Error Handling and Cleanup

#### 5.1 Failure Scenarios
- **Temp table creation fails**: Fail query immediately, no cleanup needed
- **Query execution fails**: Clean up created temp tables
- **Temp table cleanup fails**: Log warning, continue (tables will be orphaned)
- **Permission errors**: Fail with clear error message about catalog access

#### 5.2 Cleanup Guarantees
- Use query state machine listeners to trigger cleanup on any terminal state
- Implement best-effort cleanup with timeout
- Add monitoring/alerting for orphaned temp tables

### Phase 6: Configuration and Control

#### 6.1 Cost-Based Decision Making
```java
public class CteMaterializationCostAnalyzer {
    public boolean shouldMaterializeCte(
        WithQuery cte, 
        int referenceCount,
        Session session,
        PlannerContext plannerContext);
    
    // Factors:
    // - Reference count threshold
    // - Estimated CTE execution cost
    // - Available temp table catalog capacity
    // - Query complexity heuristics
}
```

#### 6.2 Safety Controls
- Maximum number of materialized CTEs per query
- Size limits for temp tables
- Timeout for temp table operations
- Catalog-specific permissions validation

## Implementation Order

1. **Core Infrastructure** (Week 1)
   - `CteQueryExecution` skeleton
   - `CteAnalyzer` basic implementation
   - Session properties setup

2. **DDL Integration** (Week 2)  
   - `InternalDdlExecutor` implementation
   - `CteTemporaryTableManager` with Delta integration
   - Transaction context management

3. **Query Transformation** (Week 3)
   - `CteQueryTransformer` implementation
   - AST manipulation utilities
   - Query rewriting logic

4. **Execution Factory Integration** (Week 4)
   - `CteQueryExecutionFactory` 
   - Binding configuration in `QueryExecutionFactoryModule`
   - End-to-end execution flow

5. **Error Handling & Cleanup** (Week 5)
   - Failure scenario handling
   - Cleanup orchestration
   - Resource management

6. **Testing & Optimization** (Week 6)
   - Unit tests for all components
   - Integration tests with Delta catalog
   - Performance testing and tuning

## Key Benefits of This Approach

1. **Minimal Core Changes**: Leverages existing DDL infrastructure without modifying core execution logic
2. **Proper Transaction Handling**: All operations within same transaction context
3. **Graceful Fallback**: Falls back to standard execution when CTE materialization isn't beneficial
4. **Clean Separation**: CTE logic isolated in dedicated package, easy to maintain
5. **Reuses Existing Infrastructure**: Leverages `CreateTableTask`, `DropTableTask`, session management
6. **Configurable**: Controlled by session properties, can be enabled/disabled per query

## Risks and Mitigations

**Risk**: Temp table cleanup failures leading to orphaned tables
**Mitigation**: Background cleanup job, monitoring, proper logging

**Risk**: Performance regression for queries without materializable CTEs  
**Mitigation**: Fast-path detection, minimal overhead in analysis phase

**Risk**: Delta catalog permission/configuration issues
**Mitigation**: Clear error messages, validation during session setup

**Risk**: Transaction boundary issues with DDL operations
**Mitigation**: Careful transaction context management, proper error handling

This implementation plan provides a working, production-ready CTE materialization feature that integrates cleanly with Trino's existing architecture while providing the performance benefits the user requested.