# CTE Materialization via Temporary Tables - Design Document

## Overview

### Intent
Implement CTE materialization by automatically creating temporary tables in a dedicated fast catalog (Delta on MinIO) for CTEs referenced multiple times, regardless of their source connectors.

### Key Insight
Instead of complex in-memory materialization, leverage Trino's existing table infrastructure with a **unified temporary storage catalog** that works for all data sources.

## Architecture Strategy

### Dedicated Temporary Storage Catalog

**Concept**: Use a single, fast catalog for ALL temporary tables regardless of CTE source:

```sql
-- Original multi-source CTE
WITH mixed_cte AS (
  SELECT id, name FROM vertica.schema.customers      -- Slow connector
  UNION ALL
  SELECT id, name FROM postgresql.public.users      -- Different connector  
  UNION ALL
  SELECT id, name FROM delta.warehouse.contacts     -- Fast connector
)
SELECT * FROM mixed_cte m1 JOIN mixed_cte m2 ON m1.id = m2.id;

-- Automatically rewritten to:
CREATE TABLE delta_temp.temp_tables.cte_query123_mixed_cte AS (
  SELECT id, name FROM vertica.schema.customers
  UNION ALL
  SELECT id, name FROM postgresql.public.users
  UNION ALL
  SELECT id, name FROM delta.warehouse.contacts
);

SELECT * FROM delta_temp.temp_tables.cte_query123_mixed_cte m1 
JOIN delta_temp.temp_tables.cte_query123_mixed_cte m2 ON m1.id = m2.id;

DROP TABLE delta_temp.temp_tables.cte_query123_mixed_cte;
```

## Configuration

### Session Properties
- `cte_materialization_strategy`: `NONE` (default) | `ENABLED`
- `cte_min_reuse_count`: Integer, default `2`
- `cte_temp_catalog`: String, **REQUIRED when enabled** - specific Delta catalog for temp tables
- `cte_temp_schema`: String, default `temp_tables` - schema within temp catalog

### System Configuration
```properties
# config.properties - Simple single catalog approach
cte.materialization.excluded-connectors=googlesheets,prometheus,jmx
```

### Simplified Temp Catalog Strategy
**Single Delta catalog approach** - no complex selection logic:

1. **User specifies exact catalog**: `SET SESSION cte_temp_catalog = 'delta_temp';`
2. **Validate catalog at query start**: Is it accessible and writable?
3. **Success**: Use it for all temp tables
4. **Failure**: Disable CTE materialization for this query (graceful fallback)

**No AUTO selection, no fallback catalogs, no complexity!**

### Temporary Catalog Requirements
The designated temporary catalog must support:
- ✅ `CREATE TABLE AS SELECT` (CTAS)
- ✅ `DROP TABLE`
- ✅ Fast reads/writes (MinIO backend recommended)
- ✅ Automatic cleanup capabilities

## Multi-Source CTE Handling

### Source Connector Independence
**Key principle**: Temporary table location is **independent** of source connectors:

| CTE Sources | Temp Table Location | Example |
|-------------|-------------------|---------|
| Single PostgreSQL | `delta_temp.temp_tables.cte_xyz` | ✅ |
| Single Vertica | `delta_temp.temp_tables.cte_xyz` | ✅ |
| PostgreSQL + Vertica | `delta_temp.temp_tables.cte_xyz` | ✅ |
| Delta + Vertica + Sheets | **Not materialized** (Sheets excluded) | ❌ |

### Benefits of Unified Storage
1. **Performance**: Always use fastest available storage (MinIO)
2. **Simplicity**: Single cleanup location regardless of sources
3. **Consistency**: Predictable behavior across all query types
4. **Scalability**: Dedicated storage pool for temporary data

## Implementation Details

### Catalog Selection Implementation

```java
public class TempCatalogSelector {
    
    private final List<String> candidateCatalogs;
    private final Metadata metadata;
    
    public Optional<String> selectTempCatalog(Session session) {
        String configuredCatalog = getCteTempCatalog(session);
        
        if (!"AUTO".equals(configuredCatalog)) {
            // User specified explicit catalog - validate and use
            return validateCatalog(configuredCatalog) ? 
                Optional.of(configuredCatalog) : Optional.empty();
        }
        
        // AUTO selection - try candidates in priority order
        for (String candidate : candidateCatalogs) {
            if (isCatalogSuitable(candidate, session)) {
                return Optional.of(candidate);
            }
        }
        
        // Fallback: try source catalog if it supports CREATE TABLE
        Optional<String> sourceCatalog = findWritableSourceCatalog(session);
        if (sourceCatalog.isPresent()) {
            return sourceCatalog;
        }
        
        return Optional.empty(); // No suitable catalog found
    }
    
    private boolean isCatalogSuitable(String catalogName, Session session) {
        try {
            // Check if catalog exists
            CatalogHandle catalogHandle = metadata.getCatalogHandle(session, catalogName)
                .orElse(null);
            if (catalogHandle == null) {
                return false;
            }
            
            // Check if supports CREATE TABLE AS SELECT
            ConnectorMetadata connectorMetadata = metadata.getConnectorMetadata(
                session, catalogHandle);
            
            // Test schema creation capability
            String testSchema = getCteTempSchema(session);
            try {
                connectorMetadata.getSchemaProperties(session.toConnectorSession(), testSchema);
                return true;
            } catch (Exception e) {
                // Try to create schema if it doesn't exist
                return tryCreateSchema(catalogName, testSchema, session);
            }
            
        } catch (Exception e) {
            return false;
        }
    }
    
    private Optional<String> findWritableSourceCatalog(Session session) {
        // Extract all catalogs referenced in the query
        // Return first one that supports CREATE TABLE
        return Optional.empty(); // Implementation depends on query analysis
    }
}
```

### Query Rewriting Process

```java
public class CteTemporaryTableRewriter {
    
    private final TempCatalogSelector catalogSelector;
    private final Set<String> excludedConnectors;
    
    public Query rewriteQuery(Query originalQuery, Session session) {
        if (!isMaterializationEnabled(session)) {
            return originalQuery;
        }
        
        // 1. Select temp catalog (fail fast if none available)
        Optional<String> tempCatalog = catalogSelector.selectTempCatalog(session);
        if (tempCatalog.isEmpty()) {
            log.info("No suitable temp catalog found, disabling CTE materialization for query {}", 
                session.getQueryId());
            return originalQuery; // Graceful fallback
        }
        
        // 2. Analyze CTEs and count references
        Map<String, CteInfo> cteAnalysis = analyzeCtes(originalQuery);
        
        // 3. Filter CTEs for materialization
        List<CteInfo> toMaterialize = cteAnalysis.values().stream()
            .filter(cte -> cte.getReferenceCount() >= getMinReuseCount(session))
            .filter(cte -> !hasExcludedConnectors(cte))
            .collect(toImmutableList());
            
        if (toMaterialize.isEmpty()) {
            return originalQuery; // No changes needed
        }
        
        // 4. Generate rewritten query with temp tables
        return rewriteWithTempTables(originalQuery, toMaterialize, 
            tempCatalog.get(), session);
    }
    
    private boolean hasExcludedConnectors(CteInfo cte) {
        Set<String> cteConnectors = extractConnectors(cte.getQuery());
        return !Collections.disjoint(cteConnectors, excludedConnectors);
    }
}
```

### CTE Analysis

```java
public class CteInfo {
    private final String name;
    private final Query query;
    private final int referenceCount;
    private final Set<String> sourceConnectors;
    
    // Transitive reference counting
    public static Map<String, CteInfo> analyzeCtes(Query query) {
        // 1. Build dependency graph between CTEs
        Map<String, Set<String>> dependencies = buildCteDependencies(query);
        
        // 2. Count direct references  
        Map<String, Integer> directCounts = countDirectReferences(query);
        
        // 3. Calculate transitive counts
        Map<String, Integer> transitiveCounts = calculateTransitiveReferences(
            dependencies, directCounts);
            
        // 4. Extract source connectors for each CTE
        return buildCteInfoMap(query, transitiveCounts);
    }
}
```

### Temporary Table Generation

```java
public class TempTableGenerator {
    
    public List<Statement> generateTempTableStatements(
            List<CteInfo> ctesToMaterialize, 
            QueryId queryId) {
            
        List<Statement> statements = new ArrayList<>();
        
        for (CteInfo cte : ctesToMaterialize) {
            // Generate CREATE TABLE AS SELECT
            String tempTableName = generateTempTableName(queryId, cte.getName());
            QualifiedName tempTable = QualifiedName.of(
                tempCatalog, tempSchema, tempTableName);
                
            CreateTableAsSelect createStmt = new CreateTableAsSelect(
                tempTable,
                cte.getQuery(),
                false, // not if not exists
                ImmutableList.of(), // no properties
                Optional.empty() // no comment
            );
            
            statements.add(createStmt);
        }
        
        return statements;
    }
    
    private String generateTempTableName(QueryId queryId, String cteName) {
        // Format: cte_{queryId}_{cteName}_{timestamp}
        return format("cte_%s_%s_%d", 
            queryId.getId().replaceAll("-", "_"),
            cteName.replaceAll("[^a-zA-Z0-9]", "_"),
            System.currentTimeMillis());
    }
}
```

### Query Transformation

```java
public class CteReferenceReplacer extends DefaultExpressionTraversalVisitor<Table> {
    
    private final Map<String, QualifiedName> cteToTempTable;
    
    @Override
    protected Table visitTable(Table node, Void context) {
        String tableName = node.getName().getSuffix();
        
        if (cteToTempTable.containsKey(tableName)) {
            // Replace CTE reference with temp table reference
            QualifiedName tempTable = cteToTempTable.get(tableName);
            return new Table(tempTable);
        }
        
        return node;
    }
}
```

## Execution Flow

### Complete Query Transformation
1. **Analysis Phase**: Count CTE references and identify source connectors
2. **Filtering Phase**: Exclude CTEs with excluded connectors or insufficient references  
3. **Generation Phase**: Create temp table CTAS statements
4. **Transformation Phase**: Replace CTE references with temp table references
5. **Cleanup Phase**: Generate DROP TABLE statements
6. **Execution Phase**: Execute the transformed query sequence

### Example Transformation
```sql
-- INPUT: Original query
WITH 
  expensive_cte AS (
    SELECT v.id, v.name FROM vertica.warehouse.customers v
    JOIN postgresql.public.orders p ON v.id = p.customer_id
    WHERE v.created_date > DATE '2023-01-01'
  )
SELECT count(*) FROM expensive_cte
UNION ALL
SELECT avg(id) FROM expensive_cte;

-- OUTPUT: Transformed query sequence
-- Step 1: Create temp table
CREATE TABLE delta_temp.temp_tables.cte_20231201_142847_12345_expensive_cte AS (
  SELECT v.id, v.name FROM vertica.warehouse.customers v
  JOIN postgresql.public.orders p ON v.id = p.customer_id  
  WHERE v.created_date > DATE '2023-01-01'
);

-- Step 2: Execute main query using temp table
SELECT count(*) FROM delta_temp.temp_tables.cte_20231201_142847_12345_expensive_cte
UNION ALL
SELECT avg(id) FROM delta_temp.temp_tables.cte_20231201_142847_12345_expensive_cte;

-- Step 3: Cleanup
DROP TABLE delta_temp.temp_tables.cte_20231201_142847_12345_expensive_cte;
```

## Catalog Selection Strategy Details

### Selection Algorithm
```java
public class CatalogSelectionStrategy {
    
    // Priority order for automatic selection
    private static final List<CatalogType> PRIORITY_ORDER = List.of(
        CatalogType.DELTA_LAKE,    // Fast object storage
        CatalogType.ICEBERG,       // Fast object storage  
        CatalogType.MEMORY,        // In-memory (small CTEs)
        CatalogType.HIVE,          // Traditional Hive metastore
        CatalogType.SOURCE_CATALOG // Last resort
    );
    
    public Optional<String> selectBestCatalog(Session session, Query query) {
        String userChoice = getCteTempCatalog(session);
        
        if (!"AUTO".equals(userChoice)) {
            return validateExplicitChoice(userChoice, session);
        }
        
        // Try configured candidates first
        List<String> candidates = getConfiguredCandidates();
        for (String candidate : candidates) {
            if (testCatalogCapabilities(candidate, session)) {
                return Optional.of(candidate);
            }
        }
        
        // Fallback: analyze query sources for writable catalogs
        return findBestSourceCatalog(query, session);
    }
    
    private boolean testCatalogCapabilities(String catalogName, Session session) {
        try {
            // Test 1: Catalog exists and is accessible
            CatalogHandle handle = getCatalogHandle(catalogName, session);
            if (handle == null) return false;
            
            // Test 2: Supports CREATE TABLE AS SELECT
            if (!supportsCreateTableAsSelect(handle, session)) return false;
            
            // Test 3: Can create/access temp schema
            if (!canAccessTempSchema(catalogName, session)) return false;
            
            // Test 4: Performance characteristics (optional)
            return hasGoodPerformanceProfile(catalogName);
            
        } catch (Exception e) {
            log.debug("Catalog {} failed suitability test: {}", catalogName, e.getMessage());
            return false;
        }
    }
}
```

### Guaranteeing Catalog Availability

**Problem**: How to ensure a suitable temp catalog always exists?

**Solution - Layered Fallback Strategy**:

1. **Tier 1 - Preferred Fast Catalogs**:
   ```properties
   # System configuration  
   cte.materialization.temp-catalog-candidates=delta_fast,iceberg_cache,memory
   ```

2. **Tier 2 - Standard Catalogs**:
   ```java
   // If no Tier 1 available, try common catalogs
   List<String> standardFallbacks = List.of("hive", "delta", "iceberg");
   ```

3. **Tier 3 - Source Catalog Fallback**:
   ```java
   // Use CTE's source catalog if it supports CREATE TABLE
   private Optional<String> findWritableSourceCatalog(Query query) {
       Set<String> sourceCatalogs = extractSourceCatalogs(query);
       return sourceCatalogs.stream()
           .filter(catalog -> supportsCreateTable(catalog))
           .findFirst();
   }
   ```

4. **Tier 4 - Memory Connector (Always Available)**:
   ```java
   // Emergency fallback - use memory connector if configured
   if (isMemoryConnectorAvailable()) {
       return Optional.of("memory");
   }
   ```

5. **Tier 5 - Graceful Disable**:
   ```java
   // If no catalogs work, disable CTE materialization for this query
   log.info("No suitable temp catalog found, disabling CTE materialization");
   return originalQuery; // No performance loss, just no optimization
   ```

## Error Handling & Cleanup

### Failure Scenarios with Catalog Issues
1. **No temp catalog available**: Graceful fallback to original query (no error)
2. **Temp catalog becomes unavailable mid-query**: Fail fast, cleanup any created tables
3. **CREATE TABLE fails**: Fallback to original query, log warning  
4. **Main query fails**: Ensure DROP TABLE still executes via cleanup hooks
5. **Query cancellation**: Background cleanup process handles orphaned tables
6. **Temp schema doesn't exist**: Auto-create if possible, fallback otherwise

### Cleanup Strategy
```java
public class TempTableCleanupManager {
    
    @EventListener
    public void onQueryCompleted(QueryCompletedEvent event) {
        cleanupQueryTempTables(event.getQueryId());
    }
    
    @EventListener  
    public void onQueryFailed(QueryFailedEvent event) {
        cleanupQueryTempTables(event.getQueryId());
    }
    
    @Scheduled(fixedDelay = 3600000) // Every hour
    public void cleanupOrphanedTables() {
        // Find temp tables older than 2 hours
        // DROP them to prevent storage bloat
        String pattern = "cte_%"; 
        cleanupTablesOlderThan(pattern, Duration.ofHours(2));
    }
}
```

## Testing Strategy

### Functional Testing
1. **Single-source CTEs**: Verify temp table creation and usage
2. **Multi-source CTEs**: PostgreSQL + Vertica → Delta temp table
3. **Excluded connectors**: GoogleSheets in CTE → no materialization
4. **Reference counting**: Ensure only 2+ reference CTEs are materialized
5. **Nested CTEs**: Complex dependency scenarios
6. **Error scenarios**: Temp catalog failures, cleanup verification

### Performance Testing  
1. **Baseline comparison**: Original vs temp table performance
2. **Large CTE scenarios**: Multi-GB CTEs that exceed memory
3. **Concurrent queries**: Multiple queries creating temp tables simultaneously
4. **Storage performance**: MinIO temp table read/write speeds

## Advantages Over In-Memory Approach

| Aspect | Temporary Tables | In-Memory |
|--------|-----------------|-----------|
| **Implementation complexity** | Low | High |
| **Memory limits** | None (uses storage) | 10GB limit |
| **Multi-source support** | Native | Complex |
| **Upgrade risk** | Minimal | High |
| **Large CTE handling** | Unlimited | Limited |
| **Proven patterns** | Yes (existing tables) | No (new execution model) |
| **Development time** | 2-3 weeks | 6-8 weeks |

## Potential Limitations

### Storage Requirements
- **Disk space**: Temporary tables consume storage in temp catalog
- **I/O bandwidth**: May impact MinIO if many concurrent queries
- **Cleanup dependencies**: Orphaned tables if cleanup fails

### Connector Constraints  
- **Temp catalog availability**: Must be configured and accessible
- **Write permissions**: Temp catalog must allow CREATE/DROP operations
- **Performance variability**: Dependent on temp catalog performance characteristics

## Configuration Example

### Minimal Setup (Guaranteed to Work)
```properties
# config.properties - Minimal setup using memory connector as fallback
cte.materialization.temp-catalog=AUTO
cte.materialization.temp-catalog-candidates=memory
cte.materialization.temp-schema=temp_tables
```

### Optimal Setup (Fast Storage)
```properties
# config.properties - Optimal with fast Delta Lake
cte.materialization.temp-catalog=AUTO  
cte.materialization.temp-catalog-candidates=delta_fast,memory,hive
cte.materialization.temp-schema=temp_tables
```

```sql
-- catalog/delta_fast.properties
connector.name=delta-lake
hive.metastore.uri=thrift://metastore:9083
hive.s3.endpoint=http://minio:9000
hive.s3.path-style-access=true
hive.s3.streaming.part-size=32MB

-- Auto-create temp schema
CREATE SCHEMA IF NOT EXISTS delta_fast.temp_tables;
```

### Session Usage
```sql
-- Enable CTE materialization (will auto-select best catalog)
SET SESSION cte_materialization_strategy = 'ENABLED';

-- Optional: Force specific catalog
SET SESSION cte_temp_catalog = 'delta_fast';

-- Optional: Adjust thresholds
SET SESSION cte_min_reuse_count = 3;
```

## Summary: Catalog Selection Strategy

### ✅ **Robust Fallback Guarantees**

1. **Primary**: Fast object storage (Delta/Iceberg on MinIO)
2. **Secondary**: Standard catalogs (Hive, etc.)  
3. **Tertiary**: Source catalog (if writable)
4. **Emergency**: Memory connector (always available if configured)
5. **Graceful**: Disable feature if no options work

### ✅ **Multi-Source CTE Resolution**

- **Question**: "Where to create temp table for Vertica + PostgreSQL CTE?"
- **Answer**: Always in the **best available fast catalog** (e.g., Delta on MinIO)
- **Benefit**: Consistent fast performance regardless of source complexity

### ✅ **Zero Configuration Risk**

- **Default**: `AUTO` selection with graceful fallback
- **Guarantee**: Feature never breaks queries (worst case = disable optimization)
- **Flexibility**: Can be tuned for optimal performance when fast storage available

## Trino Component Modification Analysis

### ❌ **Can We Use Only trino-base-jdbc?**

**NO** - This feature requires modifications to **core Trino components** that are outside the JDBC plugin scope.

### 🏗️ **Required Component Changes**

| Component | Why Modified | Changes Needed |
|-----------|--------------|----------------|
| **core/trino-main** | Query analysis & rewriting | CTE reference counting, query transformation |
| **core/trino-server** | Session properties registration | Add new session properties to SystemSessionProperties |
| **core/trino-parser** | Query AST manipulation | Parse and rewrite CTEs, replace with table references |
| **core/trino-spi** | Common interfaces | No changes needed (uses existing interfaces) |

### 📋 **Detailed Component Changes**

#### 1. **core/trino-server** (Session Properties)
```java
// SystemSessionProperties.java
public static final String CTE_MATERIALIZATION_STRATEGY = "cte_materialization_strategy";
public static final String CTE_TEMP_CATALOG = "cte_temp_catalog";
public static final String CTE_TEMP_SCHEMA = "cte_temp_schema";
public static final String CTE_MIN_REUSE_COUNT = "cte_min_reuse_count";

// Add to getSystemSessionProperties()
enumProperty(CTE_MATERIALIZATION_STRATEGY, "CTE materialization strategy", 
    CteMaterializationStrategy.class, CteMaterializationStrategy.NONE, false),
stringProperty(CTE_TEMP_CATALOG, "Catalog for temporary CTE tables", null, false),
// ... other properties
```

#### 2. **core/trino-main** (Core Logic)
```java
// New classes in sql.planner package:
- CteReferenceAnalyzer.java       // Count CTE references
- CteTemporaryTableRewriter.java  // Transform query to use temp tables  
- TempCatalogValidator.java       // Validate temp catalog availability
- CteCleanupManager.java          // Handle temp table cleanup

// Modified classes:
- LogicalPlanner.java             // Integrate CTE rewriting
- QueryPlanner.java               // Add rewriting phase
```

#### 3. **core/trino-parser** (Query Transformation)
```java
// New classes in sql.tree package:
- CteQueryTransformer.java        // Transform WITH clauses to temp tables

// Modified classes:  
- AstVisitor implementations      // Visit and transform CTE nodes
- Query.java (potentially)       // Handle transformed queries
```

### 🔍 **Why Base-JDBC Alone Won't Work**

**JDBC plugins handle**:
- ✅ Data source connectivity
- ✅ Metadata operations (CREATE/DROP TABLE)
- ✅ Query execution against specific connectors

**CTE materialization requires**:
- ❌ **Query-level transformations** (before connector involvement)
- ❌ **Cross-connector orchestration** (CTE from multiple sources)
- ❌ **Session property management** (core Trino feature)
- ❌ **Query planning modifications** (core planner logic)

### 🎯 **Implementation Location Strategy**

```
Query Flow:
SQL Text → Parser → Analyzer → Planner → Optimizer → Execution
                                  ↑
                        CTE Rewriting happens HERE
                        (Before any connector involvement)
```

**The transformation must happen at the core planner level** because:
1. **Multiple connectors**: CTE might read from PostgreSQL + Vertica
2. **Cross-catalog operations**: Need to orchestrate temp table creation in Delta
3. **Query structure changes**: Fundamental query transformation required

### ⚖️ **Simplified Implementation Scope**

With single catalog approach, we **eliminate**:
- ❌ Complex catalog selection logic
- ❌ Multi-tier fallback systems  
- ❌ Runtime catalog discovery
- ❌ Performance profiling of catalogs

We **still need**:
- ✅ Session properties (core/trino-server)
- ✅ CTE analysis and counting (core/trino-main)
- ✅ Query rewriting (core/trino-main + core/trino-parser)
- ✅ Cleanup management (core/trino-main)

### 📊 **Development Effort Estimate**

| Component | Complexity | Time Estimate |
|-----------|------------|---------------|
| Session properties | Low | 0.5 days |
| CTE reference counting | Medium | 2-3 days |
| Query transformation | High | 5-7 days |
| Cleanup & error handling | Medium | 2-3 days |
| Testing & integration | High | 4-5 days |
| **Total** | | **~2-3 weeks** |

### 🚫 **Alternative: Plugin-Only Approach (Not Recommended)**

**Could we hack this into a plugin?**
- 🤔 Create a "virtual connector" that intercepts queries
- 🤔 Parse and rewrite queries within connector
- ❌ **Problems**: Session properties, cross-catalog coordination, cleanup complexity
- ❌ **Verdict**: Possible but extremely hacky and fragile

### ✅ **Recommendation: Core Implementation**

**Benefits of core implementation**:
- 🏗️ **Proper integration** with Trino's query lifecycle
- 🔒 **Reliable session properties** and configuration
- 🧹 **Proper cleanup** via query completion hooks  
- 📈 **Future extensibility** for advanced features
- 🛡️ **Maintainability** across Trino version upgrades

**Bottom line**: This feature requires core Trino modifications - there's no clean way to implement it purely in a plugin.

## Security-First Design Decision Summary

### ✅ **Answers to Design Questions**

1. **Temp table naming**: `cte_queryid_ctename` format **approved**
2. **Schema handling**: User-prefixed table names in shared temp schema (avoids MinIO bucket proliferation)
3. **Cleanup timing**: **Immediate cleanup** after query completion
4. **Error handling**: **Graceful fallback** to disable optimization (no query failures)
5. **Resource limits**: **No limits** for now (rely on existing Trino memory management)

### 🔒 **Security Model**

**Priority 1**: Use native temp tables in source databases (PostgreSQL/Vertica)
- Session-isolated, auto-cleanup, inherited security

**Priority 2**: User-prefixed table names in designated temp schema
- `delta.temp_tables.alice_cte_query123_expensive_cte` - table name includes user ID
- Relies on Delta Lake table-level permissions to restrict access

**Priority 3**: Disable materialization if no secure option available
- No performance gain vs potential security breach

This approach ensures **zero cross-user data leakage** while maximizing performance benefits where security permits.

## Additional Decisions Needed

### 🤔 **Outstanding Design Questions**

1. **Temp Table Naming Convention**:
   ```sql
   -- Format: cte_{queryId}_{cteName}_{timestamp}?
   -- Or: temp_cte_{hash}?
   -- Considerations: Uniqueness, readability, cleanup
   ```

2. **Schema Auto-Creation**:
   ```sql
   -- Should we auto-create the temp schema if it doesn't exist?
   -- Or require manual setup?
   ```

3. **Cleanup Timing**:
   ```sql
   -- When to drop temp tables?
   -- - Immediately after main query completes?
   -- - Background cleanup with delay?
   -- - TTL-based cleanup?
   ```

4. **Error Handling Granularity**:
   ```sql
   -- If temp catalog fails mid-query:
   -- - Fail entire query?
   -- - Continue with remaining CTEs inline?
   ```

5. **Resource Limits**:
   ```sql
   -- Should we limit:
   -- - Number of temp tables per query?
   -- - Total size of temp tables per session?  
   -- - Concurrent temp table creation?
   ```

This simplified single-catalog approach eliminates most complexity while still requiring core Trino modifications for proper implementation.

## Connector-Level Implementation Analysis

### 🔍 **Can We Implement at Connector Level Only?**

**User Question**: "If we only implement this feature on a connector to connector basis (for example, only for vertica, postgresql, delta and iceberg catalogs), so the session property will be at a connector level, we will still need to modify other trino components?"

### ❌ **Short Answer: YES, Core Modifications Still Required**

Even with connector-level session properties, we **MUST** modify core Trino components because:

### 🏗️ **Why Core Changes Are Unavoidable**

#### 1. **CTE Analysis Happens Before Connector Involvement**
```
Query Processing Pipeline:
SQL Text → Parser → Analyzer → [CTE Analysis HERE] → Planner → Connector
                                        ↑
                    Must count references BEFORE reaching connectors
```

#### 2. **Cross-Connector CTEs Break Connector-Only Approach**
```sql
-- This CTE spans multiple connectors
WITH mixed_data AS (
  SELECT * FROM postgresql.schema.table
  UNION ALL
  SELECT * FROM vertica.schema.table
)
SELECT * FROM mixed_data m1 JOIN mixed_data m2 ON m1.id = m2.id;

-- Which connector handles the materialization?
-- Answer: Neither - it must be handled at core level
```

#### 3. **Query Transformation Must Happen Centrally**
```java
// This transformation MUST occur in core planner
// BEFORE the query is split to individual connectors

// Original Query AST:
With(
  cteList: [expensive_cte],
  query: Select(...references to expensive_cte...)
)

// Transformed Query AST:
Sequence(
  CreateTableAsSelect(temp_table, cte_query),
  Select(...references to temp_table...),
  DropTable(temp_table)
)
```

### 📋 **What Connector-Level Properties CAN Control**

Connector session properties could control:
- ✅ **Whether that connector participates**: `postgresql.cte_materialization_enabled`
- ✅ **Connector-specific thresholds**: `vertica.cte_min_size_for_materialization`
- ✅ **Storage location for that connector**: `delta.temp_table_schema`

### 📋 **What Connector-Level Properties CANNOT Do**

- ❌ **Count CTE references** (happens in parser/analyzer)
- ❌ **Transform query structure** (happens in planner)
- ❌ **Coordinate cross-connector CTEs** (needs central orchestration)
- ❌ **Handle query-wide cleanup** (needs query lifecycle hooks)

### 🎯 **Hybrid Approach (Best of Both)**

```java
// Core component (REQUIRED)
public class CteTemporaryTableRewriter {
    public Query rewriteQuery(Query query, Session session) {
        // 1. Analyze CTEs and count references (CORE)
        Map<String, CteInfo> cteAnalysis = analyzeCtes(query);
        
        // 2. Check connector-level properties (HYBRID)
        for (CteInfo cte : cteAnalysis.values()) {
            if (!isConnectorMaterializationEnabled(cte, session)) {
                cte.setMaterializationDisabled();
            }
        }
        
        // 3. Transform query (CORE)
        return transformWithTempTables(query, cteAnalysis);
    }
    
    private boolean isConnectorMaterializationEnabled(CteInfo cte, Session session) {
        // Check each source connector's session properties
        for (String connector : cte.getSourceConnectors()) {
            String property = connector + ".cte_materialization_enabled";
            if (!getSessionProperty(session, property, false)) {
                return false;
            }
        }
        return true;
    }
}
```

### 📊 **Component Modification Summary**

| Component | Required? | Why |
|-----------|-----------|-----|
| **core/trino-main** | ✅ YES | CTE analysis, query transformation |
| **core/trino-parser** | ✅ YES | Parse and manipulate query AST |
| **core/trino-server** | ✅ YES | Register session properties, query lifecycle |
| **connector plugins** | ⚠️ OPTIONAL | Can add connector-specific properties |

### 🔧 **Minimal Connector-Only "Hack" (Not Recommended)**

**Could we hack JUST temp table creation into connectors?**

```java
// In JDBC connector - VERY LIMITED approach
public class JdbcMetadata {
    @Override
    public ConnectorTableHandle getTableHandle(Session session, SchemaTableName tableName) {
        // Detect if this looks like a CTE reference pattern
        if (looksLikeCte(tableName) && shouldMaterialize(session)) {
            // Create temp table on-the-fly
            createTempTable(tableName, extractCteQuery(session));
        }
        return super.getTableHandle(session, tableName);
    }
}
```

**Problems with this hack**:
- ❌ Can't count references (no query context)
- ❌ Can't handle cross-connector CTEs
- ❌ No proper cleanup mechanism
- ❌ Breaks query optimization
- ❌ Race conditions with concurrent access

### ✅ **Final Recommendation**

**For production-quality CTE materialization**:
1. **Core modifications are REQUIRED** regardless of approach
2. **Connector-level properties are OPTIONAL** additions
3. **Hybrid approach** gives best flexibility:
   - Core handles analysis and transformation
   - Connectors can opt-in/out via properties
   - Centralized cleanup and error handling

**Bottom Line**: Even with connector-specific session properties, the fundamental query transformation and CTE analysis **MUST** happen in core Trino components before the query reaches individual connectors.