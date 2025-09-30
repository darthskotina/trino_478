# CTE Materialization Feature Design Document

## Overview

### Intent
Implement CTE (Common Table Expression) materialization in Trino to optimize queries that reference the same CTE multiple times. Currently, Trino re-executes CTE queries for each reference, leading to unnecessary computation and resource usage.

### Scope
- **Phase 1**: Memory-only materialization with session-level control
- **Future**: Storage-based materialization for larger CTEs

## Functional Requirements

### Session Properties

#### Primary Control
- `cte_materialization_strategy`: `NONE` (default) | `ENABLED`
  - `NONE`: Disable CTE materialization (current behavior)
  - `ENABLED`: Enable automatic materialization for CTEs referenced 2+ times

#### Configuration Parameters  
- `cte_min_reuse_count`: Integer, default `2`
  - Minimum number of references required to trigger materialization
- `cte_max_memory_size`: DataSize, default `10GB`
  - Maximum memory size for CTE materialization
  - If CTE exceeds this limit, materialization is disabled for that CTE
- `cte_storage_mode`: `MEMORY_ONLY` (only option in Phase 1)
  - Reserved for future storage implementations

#### System Configuration
- `cte.materialization.default-strategy`: System-wide default (overrides `NONE`)
- `cte.materialization.excluded-connectors`: Comma-separated list of connector names
  - Example: `googlesheets,prometheus,jmx`
  - CTEs reading from these connectors will never be materialized

### Core Functionality

#### Materialization Decision Logic
1. **Reference Counting**: Count both direct and transitive CTE references
   ```sql
   WITH 
     b AS (SELECT * FROM expensive_table),
     a AS (SELECT * FROM b WHERE x > 10)
   SELECT * FROM a    -- A referenced once
   UNION ALL 
   SELECT * FROM a    -- A referenced twice  
   UNION ALL
   SELECT * FROM b;   -- B referenced three times (2 through A + 1 direct)
   ```
   Result: Both A (2 refs) and B (3 refs) should be materialized. **We materialize each CTE only once regardless of reference count.**

2. **Size Estimation**: Use Trino's existing cost estimation
   - If estimated size > `cte_max_memory_size`: Don't materialize
   - **Removed**: ~~Runtime size checking~~ - rely on Trino's memory management to prevent OOM

3. **Connector Filtering**: Skip materialization for CTEs involving **any** excluded connectors
   - Mixed connector CTEs (e.g., PostgreSQL + GoogleSheets): **Not materialized** if any connector is excluded

#### Execution Flow
1. **Analysis Phase**: Count all CTE references in the query
2. **Planning Phase**: Decide which CTEs to materialize before any execution begins  
3. **Execution Phase**: 
   - First reference to materialized CTE: Execute and store results in memory
   - Subsequent references: Read from materialized results
   - Non-materialized CTEs: Execute inline as before

## Implementation Architecture

### Core Components

#### Plan Node Design (Leveraging Existing Exchange Model)
**Answer to "How does execution flow work?"**: Trino uses `ExchangeNode` and `RemoteSourceNode` patterns for sharing data across plan fragments. We'll adapt this proven model:

```java
// Option 1: Use existing ExchangeNode with REPLICATE type
ExchangeNode cteExchange = new ExchangeNode(
    id, 
    ExchangeNode.Type.REPLICATE,  // Broadcast to all consumers
    ExchangeNode.Scope.LOCAL,     // Within same query
    ctePlan,
    partitioningScheme
);

// Multiple RemoteSourceNodes read from the same exchange
RemoteSourceNode cteConsumer1 = new RemoteSourceNode(id1, fragmentId, outputs, ...);
RemoteSourceNode cteConsumer2 = new RemoteSourceNode(id2, fragmentId, outputs, ...);
```

**Alternative**: Create new specialized nodes that follow same patterns:
```java
// Producer node - executes CTE once and stores results  
public class CteMaterializeNode extends PlanNode {
    private final PlanNode source;
    private final String cteId;
    private final List<Symbol> outputSymbols;
    // Uses same execution model as ExchangeNode
}

// Consumer node - reads from materialized CTE
public class CteConsumerNode extends PlanNode {
    private final String cteId; 
    private final List<Symbol> outputSymbols;
    // References materialized data via shared storage, not direct node reference
}
```

#### Storage Interface (Addressing Concurrency Issues)
**Answer to "How to handle multiple concurrent reads?"**: Store as `List<Page>` and provide independent iterators:

```java
public interface CteStorage {
    void store(String cteId, List<Page> pages);  // Changed from Iterator
    List<Page> read(String cteId);               // Return copy/view for concurrent access
    void cleanup(String cteId);
}

// Phase 1 implementation with concurrent access support
public class MemoryCteStorage implements CteStorage {
    private final Map<String, List<Page>> storage = new ConcurrentHashMap<>();
    
    @Override
    public void store(String cteId, List<Page> pages) {
        storage.put(cteId, ImmutableList.copyOf(pages)); // Immutable for safety
    }
    
    @Override  
    public List<Page> read(String cteId) {
        return storage.get(cteId); // Safe concurrent reads of immutable list
    }
    
    @Override
    public void cleanup(String cteId) {
        storage.remove(cteId);
    }
}
```

#### Reference Counting
```java
public class CteReferenceAnalyzer {
    // Two-pass analysis:
    // 1. Build CTE dependency graph
    // 2. Calculate transitive reference counts
    public Map<String, Integer> analyzeReferences(Query query);
}
```

### Integration Points

#### LogicalPlanner Enhancement
- Pre-analyze all CTE references before planning begins
- Make materialization decisions upfront
- Pass decisions to RelationPlanner

#### RelationPlanner Modifications  
- Insert CteMaterializeNode for first reference of materialized CTEs
- Replace subsequent references with CteConsumerNode
- Handle fallback to inline execution on materialization failure

#### Memory Management
- CTE materialization counts against existing query memory pools
- No additional cluster-wide limits (rely on existing Trino memory management)

## Error Handling & Fallback

### Failure Scenarios
1. **Memory Exhaustion**: If CTE exceeds memory limits during materialization
   - Action: Abort materialization, fallback to inline execution
   - Logging: INFO level with CTE details

2. **Estimation Errors**: If CTE larger than estimated
   - Action: Disable materialization for that CTE
   - Logging: DEBUG level with size comparison

### Logging Requirements

#### INFO Level
- CTE name and query ID
- Reference count
- Materialization strategy used (MEMORY/DISABLED/FAILED)
- Actual vs estimated CTE size
- Execution time information

#### DEBUG Level  
- Complete CTE query text
- Detailed size breakdowns
- Memory pool usage
- Fallback reasons

## Cross-Catalog Support

### Connector Agnostic Design
- Implementation at core planner level (trino-main)
- Works transparently with all connectors
- CTE materialization operates on PlanNodes, not connector-specific data

### Mixed Connector Scenarios
```sql
WITH 
  pg_data AS (SELECT * FROM postgresql.schema.table),
  delta_data AS (SELECT * FROM delta.schema.table),  
  combined AS (
    SELECT * FROM pg_data UNION ALL SELECT * FROM delta_data
  )
SELECT * FROM combined c1 JOIN combined c2 ON c1.id = c2.id;
```
- `combined` CTE references both PostgreSQL and Delta Lake
- Materialization works at the unified result level
- No per-connector implementation needed

## Testing Strategy

### Correctness Testing
1. **Result Equivalence**: Materialized vs non-materialized execution produces identical results
2. **Complex Query Scenarios**:
   - Nested CTEs with multiple references
   - Recursive CTEs (should not be materialized)
   - CTEs with window functions, aggregations, joins
   - Cross-catalog CTEs

### Functionality Testing
3. **Session Property Combinations**:
   - `strategy=ENABLED, min_reuse_count=2, max_memory_size=10GB`
   - `strategy=ENABLED, min_reuse_count=3, max_memory_size=100MB`
   - `strategy=NONE` with other properties (should be ignored)
   - Invalid values (negative counts, zero memory)

4. **Memory-Only Mode**:
   - CTEs under memory limit: Successfully materialized
   - CTEs over memory limit: Not materialized, inline execution
   - Memory exhaustion during materialization: Graceful fallback

5. **Reference Counting**:
   - Direct references (CTE used multiple times directly)
   - Transitive references (CTE A uses CTE B, both used multiple times)
   - Single-use CTEs: Never materialized

### Integration Testing
6. **Multi-Connector Scenarios**:
   - Same query accessing Delta Lake + PostgreSQL + other connectors
   - Excluded connectors (GoogleSheets, Prometheus): Never materialized
   - Mixed scenarios with some connectors excluded

7. **Error Scenarios**:
   - Memory pressure during materialization
   - Query cancellation during CTE materialization
   - Concurrent queries with large CTEs

### Query Plan Testing  
8. **Plan Visualization**:
   - `EXPLAIN` shows CteMaterializeNode and CteConsumerNode
   - Plan includes materialization strategy and CTE metadata
   - Verify plan correctness for complex nested scenarios

## Observability

### Query Plan Display
```
- CteMaterializeNode[cte=expensive_cte, strategy=MEMORY, refs=3]
  - [original CTE plan]
- JoinNode  
  ├── CteConsumerNode[cte=expensive_cte]
  └── CteConsumerNode[cte=expensive_cte]
```

### Logging Examples
```
INFO: CTE materialization: query=20231201_142847_12345, cte=expensive_cte, references=3, strategy=MEMORY, estimated_size=2.3GB, actual_size=2.1GB, materialize_time=1.2s

DEBUG: CTE materialization failed: query=20231201_142847_12346, cte=large_cte, reason=exceeded_memory_limit, estimated=5GB, limit=10GB, actual=12GB, fallback=inline
```

## Non-Goals (Future Phases)

### Phase 1 Limitations
- ❌ Storage-based materialization (MinIO/S3 spilling)
- ❌ CTE-level hints (`cte__materialize` naming conventions)  
- ❌ Cross-query CTE sharing/caching
- ❌ Materialized view integration
- ❌ Custom storage strategies per CTE

### Future Extensions
- **Phase 2**: Add storage-based materialization with configurable spilling
- **Phase 3**: CTE-level hints for fine-grained control
- **Phase 4**: Cross-query CTE caching for repeated queries

## Risk Mitigation

### Safety Measures
1. **Default Disabled**: Feature is OFF by default (`strategy=NONE`)
2. **Graceful Fallback**: All materialization failures fall back to current behavior
3. **Memory Bounds**: Strict memory limits prevent runaway memory usage
4. **Connector Exclusion**: Problematic connectors can be excluded
5. **Extensive Testing**: Comprehensive test suite ensures correctness

### Rollback Strategy
- Feature can be disabled via session property
- No changes to existing query execution paths when disabled
- Clean fallback ensures no query failures due to materialization issues

## Success Criteria

### Functional Goals
- ✅ CTEs referenced 2+ times are materialized when feature is enabled
- ✅ Single-reference CTEs are never materialized
- ✅ Results identical between materialized and non-materialized execution
- ✅ Graceful fallback on materialization failures
- ✅ Works across all connector combinations

### Performance Goals  
- ✅ No performance degradation when feature is disabled
- ✅ No performance degradation for single-reference CTEs when enabled
- ✅ Memory usage stays within configured limits
- ✅ Query planning overhead < 5% for typical queries

### Operational Goals
- ✅ Clear logging and observability
- ✅ Query plans show materialization decisions
- ✅ Safe rollout with session-level control

## Alternative: Temporary Tables Approach

### Concept
Instead of in-memory materialization, automatically create temporary tables for reused CTEs:

```sql
-- Original query
WITH expensive_cte AS (SELECT * FROM huge_table WHERE complex_condition)
SELECT * FROM expensive_cte e1 JOIN expensive_cte e2 ON e1.id = e2.id;

-- Automatically rewritten to:
CREATE TEMPORARY TABLE temp_cte_12345 AS (SELECT * FROM huge_table WHERE complex_condition);
SELECT * FROM temp_cte_12345 e1 JOIN temp_cte_12345 e2 ON e1.id = e2.id;
DROP TABLE temp_cte_12345;
```

### Advantages of Temporary Tables
✅ **Much simpler implementation**: Reuse existing CREATE/DROP TABLE infrastructure  
✅ **No memory limits**: Can handle arbitrarily large CTEs  
✅ **Automatic spilling**: Leverages connector storage (Delta, etc.)  
✅ **Proven reliability**: No new execution paths  
✅ **Easy upgrades**: Minimal Trino core changes  
✅ **Cross-query potential**: Could cache across queries in future  

### Disadvantages
❌ **Storage overhead**: Requires temp table storage in target catalog  
❌ **Catalog dependency**: Need writable catalog for temp tables  
❌ **Transaction complexity**: Must handle cleanup on query failure  
❌ **Connector support**: Not all connectors support CREATE TABLE  
❌ **Metadata overhead**: Creates actual catalog entries (even if temporary)  

### Implementation Complexity Comparison

| Aspect | In-Memory Materialization | Temporary Tables |
|--------|--------------------------|------------------|
| **New Plan Nodes** | CteMaterializeNode, CteConsumerNode | None needed |
| **Core Changes** | LogicalPlanner, RelationPlanner, Execution | Query rewriting only |
| **Memory Management** | Complex integration | None |
| **Storage Integration** | New storage interfaces | Reuse existing |
| **Failure Handling** | Custom fallback logic | Standard DROP TABLE |
| **Upgrade Risk** | High (deep core changes) | Low (query transformation) |

### Recommendation
For **Phase 1**, consider the temporary tables approach:
- **70% less implementation complexity**
- **Much lower upgrade/maintenance risk** 
- **Immediate support for large CTEs**
- **Can always add in-memory optimization later as Phase 2**

### Temporary Tables Implementation Sketch
```java
public class CteToTemporaryTableRewriter extends PlanVisitor<PlanNode, Void> {
    
    @Override 
    public PlanNode visitQuery(Query node, Void context) {
        if (!shouldMaterializeCtes(session)) {
            return node; // No changes
        }
        
        Map<String, Integer> cteReferenceCounts = countCteReferences(node);
        
        // For each CTE with 2+ references:
        // 1. Generate CREATE TEMP TABLE statement
        // 2. Replace CTE references with table references  
        // 3. Add DROP TABLE cleanup
        
        return rewriteQuery(node, cteReferenceCounts);
    }
}
```

**Key insight**: This transforms the query **before** normal planning, requiring minimal core changes while providing immediate value.