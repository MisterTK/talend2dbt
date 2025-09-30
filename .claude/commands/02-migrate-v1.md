---
description: Complete Talend to DBT migration with 98%+ SQL generation from pre-processed Talend output
argument-hint: <processed_talend_path> [output_path]
allowed-tools: Bash, Read, Write, MultiEdit, Edit, Glob, Grep, TodoWrite
---

# Talend to DBT Migration Engine

You are an expert data engineer implementing a complete Talend to DBT migration for BigQuery. You will generate a production-ready DBT project with 98%+ SQL generation and 100% business logic preservation from pre-processed Talend output.

## Command Usage
```
/migrate-talend <processed_talend_path> [output_path]
```

**Arguments:**
- `$1` (processed_talend_path, required): Path to directory containing pre-processed Talend output files
- `$2` (output_path, optional): Output directory for DBT project (default: ./dbt_migrated)

## Input Directory Structure

The source directory ($1) contains pre-processed Talend job information:
```
$1/
├── talend_extraction.json    # Complete job structure and components
├── sql_queries.sql           # All extracted SQL from tDBInput/tDBOutput
├── transformations.json      # tMap business logic and expressions
├── context_to_dbt.yml        # Context variable mappings
└── extraction_summary.txt    # Migration overview and statistics
```

## Mission Statement

Transform Talend ETL jobs into production-ready DBT models that:
- Generate 98%+ complete SQL (not placeholders)
- Preserve 100% of business logic and data transformations
- Reduce model count by 70% through intelligent consolidation
- Require <2% manual intervention post-migration
- Run immediately on BigQuery with latest 2025 features

## Phase 1: Initial Analysis & Extraction

**MANDATORY FIRST STEPS:**

1. **Load Pre-Processed Data**
   ```bash
   # Verify all required files are present
   ls -la $1/
   # Expected files:
   # - talend_extraction.json
   # - sql_queries.sql
   # - transformations.json
   # - context_to_dbt.yml
   # - extraction_summary.txt
   ```

2. **Analyze Job Structure** (from talend_extraction.json)
   - Identify Grandmaster orchestration jobs
   - Map Master coordination jobs
   - Catalog Child implementation jobs
   - Document ALL job dependencies (parent-child, tRunJob references, cross-job lookups)
   - Detect lookup patterns (jobs containing "lkp", "lookup", "dim") for dimension mapping

3. **Component Inventory** (from talend_extraction.json)
   - Extract all Talend components used
   - Map component types to transformation patterns
   - Identify custom components requiring special handling

4. **Load and Parse Critical Files**
   ```python
   # Read each file for analysis:
   talend_data = read("$1/talend_extraction.json")     # Complete job structure
   sql_queries = read("$1/sql_queries.sql")            # All extracted SQL
   transformations = read("$1/transformations.json")   # tMap business logic
   context_vars = read("$1/context_to_dbt.yml")        # Variable mappings
   summary = read("$1/extraction_summary.txt")         # Migration overview
   ```

## Phase 2: Transformation Mode Processing

**SQL Generation Strategy:**

### Component-to-SQL Direct Mapping

| Talend Component | SQL Generation Pattern | Completeness |
|-----------------|------------------------|--------------|
| tDBInput | `SELECT * FROM source_table WHERE conditions` | 100% |
| tMap | Full expression translation + join logic | 95% |
| tAggregate | `GROUP BY` with aggregations | 100% |
| tJoin | Complete `JOIN` clauses with conditions | 100% |
| tFilter | `WHERE` clause generation | 100% |
| tSort | `ORDER BY` implementation | 100% |
| tUniqRow | `DISTINCT` or `ROW_NUMBER()` deduplication | 100% |
| tDBOutput | Target table with `INSERT/MERGE` | 100% |
| tNormalize | `UNNEST` or `CROSS JOIN` expansion | 95% |
| tDenormalize | `STRING_AGG` or pivot operations | 95% |
| tRunJob | Model dependency via `ref()` | 100% |
| tFlowToIterate | Row-by-row processing patterns | 90% |
| tLogRow | Remove (logging only, no transformation) | N/A |

### Expression Translation Engine

**Talend Java/Expression to BigQuery SQL:**

#### String Functions
| Talend Expression | BigQuery SQL | Example |
|------------------|--------------|---------|
| `row1.field.substring(0,10)` | `SUBSTR(field, 1, 10)` | Extract first 10 chars |
| `row1.field.length()` | `LENGTH(field)` | String length |
| `row1.field.trim()` | `TRIM(field)` | Remove spaces |
| `row1.field.toUpperCase()` | `UPPER(field)` | Convert to uppercase |
| `row1.field.toLowerCase()` | `LOWER(field)` | Convert to lowercase |
| `row1.field.replace("old","new")` | `REPLACE(field, 'old', 'new')` | String replacement |
| `row1.field.indexOf("str")` | `STRPOS(field, 'str')` | Find position |
| `StringHandling.UPCASE(field)` | `UPPER(field)` | Talend function |
| `StringHandling.CHANGE(str, old, new)` | `REGEXP_REPLACE(str, old, new)` | Pattern replacement |
| `field1 + field2` | `CONCAT(field1, field2)` | Concatenation |
| `field1 + "_" + field2` | `CONCAT(field1, '_', field2)` | Concat with separator |

#### Date Functions
| Talend Expression | BigQuery SQL | Example |
|------------------|--------------|---------|
| `TalendDate.getCurrentDate()` | `CURRENT_DATE()` | Today's date |
| `TalendDate.parseDate("yyyy-MM-dd", str)` | `PARSE_DATE('%Y-%m-%d', str)` | Parse string to date |
| `TalendDate.formatDate("yyyy-MM-dd", date)` | `FORMAT_DATE('%Y-%m-%d', date)` | Format date to string |
| `TalendDate.addDate(date, 1, "dd")` | `DATE_ADD(date, INTERVAL 1 DAY)` | Add days |
| `TalendDate.diffDate(date1, date2, "dd")` | `DATE_DIFF(date2, date1, DAY)` | Date difference |
| `TalendDate.getPartOfDate("YEAR", date)` | `EXTRACT(YEAR FROM date)` | Extract date part |
| `new java.util.Date()` | `CURRENT_TIMESTAMP()` | Current timestamp |

#### Numeric Functions
| Talend Expression | BigQuery SQL | Example |
|------------------|--------------|---------|
| `Math.round(value)` | `ROUND(value)` | Round to nearest integer |
| `Math.floor(value)` | `FLOOR(value)` | Round down |
| `Math.ceil(value)` | `CEIL(value)` | Round up |
| `Math.abs(value)` | `ABS(value)` | Absolute value |
| `Math.pow(base, exp)` | `POWER(base, exp)` | Exponentiation |
| `Numeric.sequence("s1", 1, 1)` | `ROW_NUMBER() OVER()` | Generate sequence |

#### Null/Logic Handling
| Talend Expression | BigQuery SQL | Example |
|------------------|--------------|---------|
| `row1.field == null ? 0 : row1.field` | `COALESCE(field, 0)` | Null replacement |
| `field1 == null && field2 == null` | `field1 IS NULL AND field2 IS NULL` | Null checking |
| `Relational.ISNULL(field)` | `field IS NULL` | Null check |
| `!Relational.ISNULL(field)` | `field IS NOT NULL` | Not null check |
| `field1.equals("value")` | `field1 = 'value'` | String equality |
| Nested ternary: `a ? (b ? x : y) : z` | `CASE WHEN a THEN CASE WHEN b THEN x ELSE y END ELSE z END` | Complex conditionals |

## Phase 3: Model Consolidation Strategy

**Model Reduction Through Intelligence:**

### Consolidation Rules
1. **Merge Sequential Transformations**
   - Combine tMap → tFilter → tMap into single CTE chain
   - Consolidate multiple tAggregates into one GROUP BY

2. **Eliminate Intermediate Tables**
   - Convert temp tables to CTEs
   - Use subqueries for single-use transformations

3. **Simplify Job Hierarchies**
   - Flatten unnecessary job nesting
   - Combine child jobs that share data flow
   - Preserve ALL dependencies through proper `ref()` usage

4. **Model Naming Standards**
   - `stg_*` for staging/source data
   - `int_*` for intermediate transformations
   - `dim_*` for lookup/dimension tables (detect from job names)
   - `fct_*` for fact/transaction tables

### Model Boundary Detection
```sql
-- Consolidated model example
WITH source_data AS (
    -- tDBInput logic
    SELECT * FROM {{ source('raw', 'input_table') }}
),
transformed AS (
    -- Combined tMap + tFilter logic
    SELECT
        UPPER(field1) AS normalized_field,
        CASE WHEN field2 > 100 THEN 'HIGH' ELSE 'LOW' END AS category,
        PARSE_DATE('%Y%m%d', date_string) AS parsed_date
    FROM source_data
    WHERE field3 IS NOT NULL  -- tFilter condition
),
aggregated AS (
    -- tAggregate logic
    SELECT
        category,
        COUNT(*) as record_count,
        SUM(amount) as total_amount
    FROM transformed
    GROUP BY category
)
-- Final output
SELECT * FROM aggregated
```

## Phase 4: DBT Project Generation

### Project Structure
```
dbt_project/
├── dbt_project.yml            # DBT configuration
├── profiles.yml               # Connection profiles
├── models/
│   ├── staging/
│   │   ├── _sources.yml       # Source definitions (ALL tables)
│   │   ├── stg_*.sql          # Staging models
│   │   └── _schema.yml        # Column descriptions, not_null, unique tests
│   ├── intermediate/
│   │   ├── int_*.sql          # Business logic
│   │   └── _schema.yml        # Tests & docs
│   └── marts/
│       ├── fct_*.sql          # Fact tables
│       ├── dim_*.sql          # Dimensions
│       └── _schema.yml        # Relationship tests between facts/dims
├── macros/
│   ├── talend_compatibility.sql  # Talend function macros
│   └── transformations.sql        # Business logic macros
├── tests/
│   ├── generic/               # Reusable tests
│   └── singular/              # Custom tests
└── docs/
    ├── migration_report.md    # Complete migration documentation
    └── validation_results.md  # Test results and coverage
```

### BigQuery 2025 Optimization

**Required Optimizations:**

1. **Pipe Syntax Usage (Correct Implementation)**
```sql
-- CORRECT: BigQuery 2025 pipe syntax
FROM {{ source('raw', 'orders') }}
|> WHERE order_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)
|> EXTEND
    revenue = quantity * price,
    margin = revenue * 0.3
|> AGGREGATE
    SUM(revenue) AS total_revenue,
    AVG(margin) AS avg_margin
    GROUP BY customer_id, product_category
|> ORDER BY total_revenue DESC
|> LIMIT 1000
```

2. **Partitioning & Clustering**
```sql
{{
    config(
        materialized='incremental',
        partition_by={
            'field': 'event_date',
            'data_type': 'date',
            'granularity': 'day'
        },
        cluster_by=['customer_id', 'product_id'],
        unique_key='transaction_id'
    )
}}
```

3. **Search Indexes**
```sql
-- Create search index for text fields
CREATE SEARCH INDEX idx_product_search
ON `project.dataset.products`(name, description)
OPTIONS(analyzer='LOG_ANALYZER');
```

4. **RANGE Types for Time Series**
```sql
-- Use RANGE for continuous data
SELECT
    sensor_id,
    RANGE<TIMESTAMP>[start_time, end_time) AS measurement_period,
    AVG(temperature) AS avg_temp
FROM sensor_data
GROUP BY sensor_id, measurement_period
```

## Phase 5: Context Variable Intelligence

### Variable Classification & Mapping

| Talend Context | DBT Equivalent | Usage Pattern |
|----------------|----------------|---------------|
| `context.schema_name` | `{{ target.dataset }}` | Schema references |
| `context.from_date` | `{{ var('from_date') }}` | Date parameters |
| `context.batch_size` | `{{ var('batch_size', 1000) }}` | Processing limits |
| `context.env` | `{{ target.name }}` | Environment detection |
| `context.file_path` | `{{ var('file_path') }}` | External file references |
| `context.db_connection` | `{{ target }}` | Connection properties |
| `globalMap.get("var")` | `{{ var('var') }}` or CTE column | Runtime variables |
| `((Integer)globalMap.get("tAggregateRow_1_NB_LINE"))` | CTE with count | Aggregation results |

### Smart Context Resolution
```sql
-- Talend: "+context.schema+"."+context.table+"
-- DBT: {{ target.dataset }}.{{ var('table_name') }}

-- Talend: context.load_date
-- DBT: {{ var('load_date', run_started_at.strftime('%Y-%m-%d')) }}

-- Talend: context.is_full_refresh
-- DBT: {% if flags.FULL_REFRESH %} ... {% endif %}
```

## Phase 6: Quality Assurance & Validation

### Migration Quality Scorecard

**Metrics for Success:**

| Metric | Target | Measurement |
|--------|--------|-------------|
| SQL Generation Coverage | ≥98% | `(generated_sql_lines / total_logic_lines) * 100` |
| Business Logic Preservation | 100% | All expressions translated correctly |
| Model Reduction | ≥70% | `1 - (dbt_models / talend_jobs)` |
| Manual Intervention Required | <2% | Lines requiring manual completion |
| Test Coverage | ≥90% | All critical paths tested |
| Documentation Completeness | 100% | All models documented |
| Source Coverage | 100% | All source tables defined in _sources.yml |
| Dependency Preservation | 100% | All job dependencies → model refs |

### Validation Queries
```sql
-- Row count validation
SELECT
    'talend' as source,
    COUNT(*) as row_count,
    SUM(amount) as total_amount,
    COUNT(DISTINCT customer_id) as unique_customers
FROM talend_output_table
UNION ALL
SELECT
    'dbt' as source,
    COUNT(*) as row_count,
    SUM(amount) as total_amount,
    COUNT(DISTINCT customer_id) as unique_customers
FROM {{ ref('final_model') }}
```

### Error Handling Patterns

**Talend Reject Flow → DBT Audit Pattern:**

```sql
-- Create audit table for rejected records
{{
    config(
        materialized='incremental',
        schema='audit',
        unique_key='audit_id'
    )
}}

WITH validation AS (
    SELECT
        *,
        CASE
            WHEN email NOT LIKE '%@%' THEN 'INVALID_EMAIL'
            WHEN amount < 0 THEN 'NEGATIVE_AMOUNT'
            WHEN customer_id IS NULL THEN 'MISSING_CUSTOMER'
            ELSE 'VALID'
        END AS validation_status
    FROM {{ ref('stg_transactions') }}
),
rejected_records AS (
    SELECT
        GENERATE_UUID() as audit_id,
        CURRENT_TIMESTAMP() as rejected_at,
        'data_quality' as rejection_type,
        validation_status as rejection_reason,
        TO_JSON_STRING(STRUCT(
            customer_id,
            email,
            amount
        )) as record_data
    FROM validation
    WHERE validation_status != 'VALID'
)
SELECT * FROM rejected_records
```

## Phase 7: Execution & Delivery

### Implementation Checklist

**Pre-Migration:**
- [ ] Analyze all files (optimized Talend jobs) in source directory
- [ ] Extract complete business logic
- [ ] Map all components to SQL patterns
- [ ] Document context variables

**Migration Execution:**
- [ ] Generate staging models from sources
- [ ] Implement transformation logic
- [ ] Consolidate models intelligently
- [ ] Create comprehensive tests
- [ ] Document all models

**Post-Migration Validation:**
- [ ] Run all DBT models successfully
- [ ] Execute comprehensive test suite
- [ ] Validate row counts match
- [ ] Verify business logic preservation
- [ ] Check performance metrics
- [ ] Confirm all sources defined
- [ ] Verify job dependencies preserved as model refs
- [ ] Validate no business logic lost in consolidation

### Output Deliverables

1. **Complete DBT Project**
   - All models with 98%+ SQL generation
   - Comprehensive test coverage
   - Full documentation

2. **Migration Report**
   ```markdown
   # Migration Report

   ## Executive Summary
   - Jobs Migrated: X
   - Models Generated: Y (Z% reduction)
   - SQL Coverage: XX%
   - Test Coverage: XX%

   ## Detailed Analysis
   [Component mapping details]
   [Expression translations]
   [Model consolidations]

   ## Validation Results
   [Row count comparisons]
   [Business logic verification]
   [Performance benchmarks]
   ```

3. **Runbook Documentation**
   - Setup instructions
   - Deployment steps
   - Monitoring guidelines
   - Troubleshooting guide

## Execution Guarantee

**The migration ALWAYS follows this order:**

1. **Extract** - Complete Talend job analysis
2. **Transform** - Generate SQL with 98%+ completeness
3. **Consolidate** - Reduce models by 70%
4. **Generate** - Create DBT project structure
5. **Validate** - Ensure 100% logic preservation
6. **Document** - Complete migration documentation

**Success Criteria:**
- ✅ All Talend jobs successfully converted
- ✅ 98%+ SQL generation (not placeholders)
- ✅ 100% business logic preserved
- ✅ 70%+ model count reduction
- ✅ <2% manual intervention needed
- ✅ All models run successfully on BigQuery
- ✅ Comprehensive test coverage
- ✅ Complete documentation

Generate production-ready DBT code that can be deployed immediately to BigQuery with minimal manual intervention.

## Next Steps - Phase 3

Once migration completes successfully, run quality validation:

### ▶️ Run Phase 3: Post-Processing

Calculate the Phase 3 path dynamically:

```bash
# Calculate Phase 3 path based on actual output location
DBT_PROJECT_DIR="$2"
if [ -z "$DBT_PROJECT_DIR" ]; then
  INPUT_PARENT="$(dirname "$1")"
  DBT_PROJECT_DIR="$INPUT_PARENT/dbt_transformed"
fi

echo ""
echo "====================================="
echo "✅ MIGRATION COMPLETE"
echo "====================================="
echo ""
echo "📋 Next Steps:"
echo ""
echo "1️⃣  Clear context (REQUIRED):"
echo "    /clear"
echo ""
echo "1️⃣  Run Phase 3 quality validation:"
echo "    /03-post-process $DBT_PROJECT_DIR"
echo ""
echo "Copy and paste the command above to validate and format the DBT project"
echo ""
echo "Phase 3 will:"
echo "  • Format and lint all SQL files with sqlfluff"
echo "  • Validate all YAML files with yamllint"
echo "  • Parse and compile the DBT project"
echo "  • Format markdown documentation"
echo "  • Generate comprehensive quality report"
echo ""
```