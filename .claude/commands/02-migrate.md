---
description: Complete Talend to DBT migration with 98%+ SQL generation from pre-processed Talend output
argument-hint: <processed_talend_path> [output_path]
allowed-tools: Bash, Read, Write, Edit, Glob, Grep, TodoWrite
---

# Talend to DBT Migration Engine

You are an expert data engineer implementing a complete Talend to DBT migration for BigQuery. You will generate a production-ready DBT project with 98%+ SQL generation and 100% business logic preservation from pre-processed Talend output.

## Command Usage

```bash
/02-migrate-v2 <processed_talend_path> [output_path]
```

**Arguments:**
- `$1` (processed_talend_path, required): Path to directory containing pre-processed Talend output files
- `$2` (output_path, optional): Output directory for DBT project (default: `<processed_talend_path>/../dbt_migrated`)

## Input Directory Structure

The source directory (`$1`) contains pre-processed Talend job information:
```
$1/
├── talend_extraction.json    # Complete job structure and components
├── sql_queries.sql           # All extracted SQL from tDBInput/tDBOutput
├── transformations.json      # tMap business logic and expressions
├── context_to_dbt.yml        # Context variable mappings
└── extraction_summary.txt    # Migration overview and statistics
```

---

## 📖 CRITICAL: Migration Standards Reference

**Standards files are located in `/standards/` directory. Reference the appropriate file for each phase:**

- **Architecture & Layers:** `@standards/01_architecture.md`
- **Naming Conventions:** `@standards/02_naming_conventions.md`
- **Model Consolidation:** `@standards/03_model_consolidation.md`
- **SQL Generation:** `@standards/04_sql_generation.md`
- **Component Mapping:** `@standards/05_component_mapping.md`
- **Context Variables:** `@standards/06_context_variables.md`
- **File Organization:** `@standards/07_file_organization.md`
- **Materialization:** `@standards/08_materialization.md`
- **Dependencies:** `@standards/09_dependencies.md`
- **Quality & Validation:** `@standards/10_quality_validation.md`
- **Error Handling:** `@standards/11_error_handling.md`
- **Performance:** `@standards/12_performance.md`
- **Constraints:** `@standards/13_constraints.md`

**Load the relevant standards file when you reach each phase. All code generation, naming decisions, and consolidation logic MUST comply with these standards.**

---

## Mission Statement

Transform Talend ETL jobs into production-ready DBT models that:
- Generate **98%+ complete SQL** (not placeholders)
- Preserve **100% of business logic** and data transformations
- Reduce model count by **70-85%** through intelligent consolidation
- Require **<2% manual intervention** post-migration
- Run immediately on **BigQuery with latest 2025 features**

---

## Phase 1: Initial Analysis & Extraction

### Step 1: Verify Input Files

```bash
# List all files in the input directory
ls -la $1/

# Verify all 5 required files are present
[ -f "$1/talend_extraction.json" ] || echo "ERROR: Missing talend_extraction.json"
[ -f "$1/sql_queries.sql" ] || echo "ERROR: Missing sql_queries.sql"
[ -f "$1/transformations.json" ] || echo "ERROR: Missing transformations.json"
[ -f "$1/context_to_dbt.yml" ] || echo "ERROR: Missing context_to_dbt.yml"
[ -f "$1/extraction_summary.txt" ] || echo "ERROR: Missing extraction_summary.txt"
```

**If any file is missing, STOP and report error.**

### Step 2: Load and Parse Input Data

Read all input files to understand the migration scope:

```python
# Read each file for analysis
talend_data = read("$1/talend_extraction.json")       # Complete job structure
sql_queries = read("$1/sql_queries.sql")              # All extracted SQL
transformations = read("$1/transformations.json")     # tMap business logic
context_vars = read("$1/context_to_dbt.yml")          # Variable mappings
summary = read("$1/extraction_summary.txt")           # Migration overview
```

### Step 3: Analyze Job Structure

📖 **Standards Reference**: Load `@standards/03_model_consolidation.md` for complete consolidation strategy

**Classify all jobs using the consolidation decision tree:**

1. **Identify Orchestrator Jobs** (DO NOT CONSOLIDATE):
   - Jobs with `has_child_jobs = true` (contain tRunJob components)
   - Role: Grandmaster or Master
   - Action: Document orchestration in comments, generate NO SQL models

2. **Identify Transaction Groups** (PRESERVE AS ATOMIC MODELS):
   - Jobs with tDBConnection → operations → tDBCommit/tDBRollback
   - Action: One model per transaction group (atomic boundary)

3. **Identify Error/Audit Flows** (SEPARATE MODELS):
   - Jobs with reject flows (tFilterRow REJECT branch, tDie, tWarn)
   - Action: Create separate `[model_name]_rejects.sql` for audit

4. **Identify Processor Jobs** (SAFE TO CONSOLIDATE):
   - Child jobs with data transformations (tMap, tFilter, tAggregate, etc.)
   - No tRunJob calls, no transaction boundaries, no error flows
   - Action: Apply consolidation rules to reduce model count

**Document job hierarchy and dependencies:**
- Map ALL parent-child relationships (tRunJob references)
- Identify ALL cross-job lookups (dimension references)
- Preserve ALL dependencies as DBT `ref()` relationships

### Step 4: Extract Domains for Naming

📖 **Standards Reference**: Load `@standards/02_naming_conventions.md` for domain extraction and layer detection rules

**Apply domain extraction patterns to ALL jobs:**

1. **From Talend Job Names**:
   - Pattern: `[PROJECT_CODE]_[Entity]_[Type]_[Job]` → extract `project_code` as domain
   - Pattern: `[PROJECT]_[Module]_[Entity]` → extract `project_module` as domain
   - Pattern: `Master_[Domain]_[Action]` → extract `domain` as domain

2. **From Talend Folder Hierarchy**:
   - Path: `workspace/PROJECT_NAME/process/Jobs/job.item` → extract `project_name` as domain

3. **Domain Validation**:
   - Ensure all jobs have an extracted domain
   - If no pattern matches → flag as `domain: default` for manual review
   - Convert domain names to lowercase snake_case

**Create domain mapping table:**
```python
domains = {
    "job_name_1": "customer360",
    "job_name_2": "sales",
    "job_name_3": "finance",
    # ... all jobs
}
```

### Step 5: Detect Layer Assignments

📖 **Standards Reference**: Reference `@standards/02_naming_conventions.md` for layer detection patterns

**Assign each job to a layer (staging/intermediate/marts):**

1. **Staging Detection** (→ `stg_[domain]__[source].sql`):
   - Job name contains: "LOAD", "EXTRACT", "SOURCE", "RAW"
   - Job has only tDBInput + minimal tMap (type casting only)

2. **Intermediate Detection** (→ `int_[domain]__[transformation].sql`):
   - Job name contains: "TRANSFORM", "PROCESS", "ENRICH", "CLEANSE"
   - Job has tMap with complex expressions, joins, filters, aggregations

3. **Dimension Detection** (→ `dim_[entity].sql`):
   - Job name contains: "DIM", "DIMENSION", "LKP", "LOOKUP", "_D_"

4. **Fact Detection** (→ `fct_[entity].sql`):
   - Job name contains: "FACT", "TRN", "TRANSACTION", "_F_", "METRICS"

### Step 6: Pre-Generation Validation

📖 **Standards Reference**: Load `@standards/10_quality_validation.md` for pre-generation validation checks

**Run ALL validation checks before proceeding:**

```python
# Check 1: Input file completeness
assert exists("$1/talend_extraction.json"), "Missing talend_extraction.json"
assert exists("$1/sql_queries.sql"), "Missing sql_queries.sql"
assert exists("$1/transformations.json"), "Missing transformations.json"
assert exists("$1/context_to_dbt.yml"), "Missing context_to_dbt.yml"

# Check 2: Extraction quality
metadata = talend_data["extraction_metadata"]
assert metadata["total_jobs"] > 0, "No jobs found in extraction"
assert metadata["total_sql_queries"] > 0, "No SQL queries extracted"
assert metadata["unique_tables"] > 0, "No source tables identified"

# Check 3: Domain extraction
assert len(domains) > 0, "No domains identified for naming"
assert all(domain != "default" or flag_for_review for domain in domains.values()), "Some jobs have no domain match"

# Check 4: Consolidation feasibility
orchestrators = [job for job in jobs if job.get("has_child_jobs")]
processors = [job for job in jobs if not job.get("has_child_jobs")]
assert len(processors) > 0, "No processor jobs to consolidate"

print(f"✅ Validation passed:")
print(f"   - Total jobs: {len(jobs)}")
print(f"   - Orchestrators: {len(orchestrators)}")
print(f"   - Processors: {len(processors)}")
print(f"   - Domains identified: {len(set(domains.values()))}")
print(f"   - Source tables: {metadata['unique_tables']}")
```

**If any validation fails, STOP migration and report specific error.**

---

## Phase 2: DBT Project Generation

### Step 1: Create DBT Project Structure

📖 **Standards Reference**: Load `@standards/07_file_organization.md` for complete directory structure

**Create the complete directory structure:**

```bash
# Set output directory (use $2 if provided, otherwise default)
OUTPUT_DIR="${2:-$(dirname "$1")/dbt_migrated}"
mkdir -p "$OUTPUT_DIR"

# Create medallion architecture folders
mkdir -p "$OUTPUT_DIR/models/staging"
mkdir -p "$OUTPUT_DIR/models/intermediate"
mkdir -p "$OUTPUT_DIR/models/marts"
mkdir -p "$OUTPUT_DIR/macros"
mkdir -p "$OUTPUT_DIR/tests/generic"
mkdir -p "$OUTPUT_DIR/tests/singular"
mkdir -p "$OUTPUT_DIR/docs"

# Create domain-specific subfolders (one per unique domain)
for domain in "${DOMAINS[@]}"; do
    mkdir -p "$OUTPUT_DIR/models/staging/$domain"
    mkdir -p "$OUTPUT_DIR/models/intermediate/$domain"
    mkdir -p "$OUTPUT_DIR/models/marts/$domain"
done
```

### Step 2: Generate dbt_project.yml

Create the DBT project configuration file:

```yaml
# $OUTPUT_DIR/dbt_project.yml
name: 'talend_migration'
version: '1.0.0'
config-version: 2

profile: 'bigquery'

model-paths: ["models"]
analysis-paths: ["analyses"]
test-paths: ["tests"]
seed-paths: ["seeds"]
macro-paths: ["macros"]
snapshot-paths: ["snapshots"]

target-path: "target"
clean-targets:
  - "target"
  - "dbt_packages"

models:
  talend_migration:
    staging:
      +materialized: view
    intermediate:
      +materialized: table
    marts:
      +materialized: table
      +partition_by:
        field: created_date
        data_type: date
        granularity: day
```

### Step 3: Generate profiles.yml

Create the BigQuery connection profile:

```yaml
# $OUTPUT_DIR/profiles.yml
bigquery:
  target: dev
  outputs:
    dev:
      type: bigquery
      method: oauth
      project: "{{ env_var('GCP_PROJECT_ID', 'your-project-id') }}"
      dataset: "{{ env_var('GCP_DATASET', 'dbt_dev') }}"
      threads: 4
      timeout_seconds: 300
      location: US
      priority: interactive

    prod:
      type: bigquery
      method: oauth
      project: "{{ env_var('GCP_PROJECT_ID', 'your-project-id') }}"
      dataset: "{{ env_var('GCP_DATASET', 'dbt_prod') }}"
      threads: 8
      timeout_seconds: 600
      location: US
      priority: batch
```

---

## Phase 3: Staging Layer Generation (Bronze)

📖 **Standards Reference**: Load `@standards/01_architecture.md` for STAGING layer responsibilities

### Step 1: Generate _sources.yml

**Define ALL source tables from tDBInput components:**

📖 **Standards Reference**: Reference `@standards/07_file_organization.md` for _sources.yml format

```yaml
# $OUTPUT_DIR/models/staging/_sources.yml
version: 2

sources:
  - name: raw
    description: Raw source tables from Talend extraction
    database: "{{ target.project }}"
    schema: "{{ target.dataset }}"
    tables:
      # For each unique table found in tDBInput components:
      - name: <table_name>
        description: <table description from Talend component>
        columns:
          - name: <primary_key_column>
            description: <column description>
            tests:
              - not_null
              - unique
          - name: <other_columns>
            description: <column description>
            # Add tests as appropriate (not_null, unique, relationships, etc.)
```

**✅ CRITICAL**: ALL tables referenced in ANY tDBInput component MUST be defined in _sources.yml

### Step 2: Generate Staging Models

**For each source table, create a staging model:**

📖 **Standards Reference**: Load `@standards/04_sql_generation.md` for SQL generation standards and CTE patterns

**Naming**: `stg_[domain]__[table_name].sql`

**Template**:
```sql
-- $OUTPUT_DIR/models/staging/[domain]/stg_[domain]__[table_name].sql

{{
    config(
        materialized='view'
    )
}}

-- Talend Source: [Job Name] > tDBInput_[N] ([original_table_name])
-- Purpose: 1:1 staging layer for [table_name]

WITH source_data AS (
    SELECT
        -- List ALL columns with SAFE_CAST for type conversions
        SAFE_CAST(column1 AS STRING) AS column1,
        SAFE_CAST(column2 AS INT64) AS column2,
        SAFE_CAST(column3 AS DATE) AS column3,
        -- ... all columns from source table
    FROM {{ source('raw', '<table_name>') }}
    -- NO business logic
    -- NO joins
    -- NO filtering (except null key removal if critical)
)

SELECT * FROM source_data
```

**✅ Staging Layer Rules** (from standards):
- ✅ Load raw data from sources 1:1
- ✅ Basic type casting (SAFE_CAST)
- ✅ Renaming columns for consistency (snake_case)
- ❌ NO business logic
- ❌ NO joins or aggregations
- ❌ NO filtering (except null key removal)

### Step 3: Generate Staging _schema.yml

Create schema documentation for staging models:

```yaml
# $OUTPUT_DIR/models/staging/[domain]/_schema.yml
version: 2

models:
  - name: stg_[domain]__[table_name]
    description: Staging model for [table description]
    columns:
      - name: [primary_key]
        description: [description]
        tests:
          - not_null
          - unique
      - name: [other_columns]
        description: [description]
        tests:
          - not_null  # Add as appropriate
```

---

## Phase 4: Intermediate Layer Generation (Silver)

📖 **Standards Reference**: Load `@standards/01_architecture.md` for INTERMEDIATE layer responsibilities

### Step 1: Apply Consolidation Rules

📖 **Standards Reference**: Reference `@standards/03_model_consolidation.md` for consolidation rules and checklist

**For each processor job, determine if it should be consolidated:**

**✅ CONSOLIDATE if:**
1. Sequential single-path transformations (no branching)
2. Temporary tables with single use
3. Multiple aggregations on same grain
4. Small lookup tables (<10K rows, static)
5. Simple child jobs (<5 components, no dependencies)

**❌ DO NOT CONSOLIDATE if:**
1. Orchestrator jobs (has tRunJob)
2. Transaction boundaries (tDBConnection/Commit/Rollback)
3. Error/reject flows (separate audit model)
4. Reusable transformations (>3 dependencies)
5. Different materialization strategies
6. Different update cadences
7. Cross-domain jobs

**Pre-Consolidation Checklist** (from standards):
```
Before merging Job A + Job B into single model:
  ☐ Same data grain/granularity
  ☐ Same refresh schedule (hourly, daily, etc.)
  ☐ No transaction boundary between them
  ☐ No error handling separation needed
  ☐ Combined SQL <500 lines
  ☐ No loss of testability
  ☐ Preserves all ref() dependencies
  ☐ No circular dependency created
```

### Step 2: Translate Talend Components to SQL

📖 **Standards Reference**: Load `@standards/05_component_mapping.md` for complete component-to-SQL translation table

**Use the component translation table to convert each Talend component:**

| Talend Component | SQL Pattern | Reference |
|-----------------|-------------|-----------|
| tDBInput | `SELECT * FROM {{ source() }}` | Standards line 526 |
| tMap (expressions) | `CASE WHEN ... THEN ... END`, `CONCAT()`, `UPPER()` | Standards line 527 |
| tMap (joins) | `LEFT/INNER JOIN ON condition` | Standards line 528 |
| tFilterRow | `WHERE condition` | Standards line 529 |
| tAggregateRow | `GROUP BY ... HAVING ...` | Standards line 530 |
| tJoin | `JOIN ... ON ...` | Standards line 531 |
| tSortRow | `ORDER BY` | Standards line 532 |
| tUniqRow | `DISTINCT` or `ROW_NUMBER() OVER()` | Standards line 533 |
| tRunJob | `ref('child_model')` | Standards line 538 |

### Step 3: Translate Talend Expressions

📖 **Standards Reference**: Reference `@standards/05_component_mapping.md` for expression translation mappings

**For ALL tMap expressions, apply the translation mappings:**

**String Functions** (Standards lines 543-554):
- `row.field.substring(0,10)` → `SUBSTR(field, 1, 10)`
- `row.field.toUpperCase()` → `UPPER(field)`
- `row.field.trim()` → `TRIM(field)`
- `field1 + field2` → `CONCAT(field1, field2)`

**Date Functions** (Standards lines 557-566):
- `TalendDate.getCurrentDate()` → `CURRENT_DATE()`
- `TalendDate.parseDate("yyyy-MM-dd", str)` → `PARSE_DATE('%Y-%m-%d', str)`
- `TalendDate.addDate(date, 1, "dd")` → `DATE_ADD(date, INTERVAL 1 DAY)`

**Numeric Functions** (Standards lines 569-577):
- `Math.round(value)` → `ROUND(value)`
- `Math.floor(value)` → `FLOOR(value)`

**Null/Conditional Logic** (Standards lines 580-592):
- `row.field == null ? 0 : row.field` → `COALESCE(field, 0)`
- `a ? (b ? x : y) : z` → `CASE WHEN a THEN CASE WHEN b THEN x ELSE y END ELSE z END`

### Step 4: Translate Context Variables

📖 **Standards Reference**: Load `@standards/06_context_variables.md` for context variable mappings

**Apply context variable mappings from standards:**

| Talend Context | DBT Equivalent |
|---------------|----------------|
| `context.schema` | `{{ target.dataset }}` |
| `context.from_date` | `{{ var('from_date') }}` |
| `context.env` | `{{ target.name }}` |
| `context.project_id` | `{{ target.project }}` |
| `globalMap.get("var")` | `{{ var('var') }}` or CTE column |

### Step 5: Generate Intermediate Models

**Naming**: `int_[domain]__[transformation_description].sql`

**Template**:
```sql
-- $OUTPUT_DIR/models/intermediate/[domain]/int_[domain]__[transformation].sql

{{
    config(
        materialized='table'
        -- Use incremental if >10M rows expected
    )
}}

-- Talend Source: [Job Name]
-- Purpose: [Business logic description]
-- Components: [List of Talend components consolidated]

WITH source_data AS (
    -- Talend: tDBInput_1 or reference to staging model
    SELECT * FROM {{ ref('stg_[domain]__[source]') }}
),

transformed AS (
    -- Talend: tMap_1 ([description of transformations])
    SELECT
        -- Apply ALL expression translations from standards
        UPPER(TRIM(column1)) AS normalized_column1,
        CASE
            WHEN column2 > 100 THEN 'HIGH'
            WHEN column2 > 50 THEN 'MEDIUM'
            ELSE 'LOW'
        END AS category,
        COALESCE(column3, 0) AS column3_with_default,
        -- ... all transformed columns
    FROM source_data
),

filtered AS (
    -- Talend: tFilterRow_1 ([filter description])
    SELECT * FROM transformed
    WHERE [filter_conditions]
),

aggregated AS (
    -- Talend: tAggregateRow_1 ([aggregation description])
    SELECT
        grouping_column,
        COUNT(*) AS record_count,
        SUM(amount) AS total_amount,
        AVG(amount) AS avg_amount
    FROM filtered
    GROUP BY grouping_column
)

-- Final select
SELECT * FROM aggregated
```

**✅ Intermediate Layer Rules** (from standards):
- ✅ tMap transformations (expressions, CASE statements)
- ✅ Joins (INNER, LEFT, lookups)
- ✅ Filters (WHERE clauses)
- ✅ Aggregations (GROUP BY)
- ✅ Data quality rules
- ❌ NO direct source() references (use staging models via ref())

### Step 6: Generate Intermediate _schema.yml

Create schema documentation with tests:

```yaml
# $OUTPUT_DIR/models/intermediate/[domain]/_schema.yml
version: 2

models:
  - name: int_[domain]__[transformation]
    description: [Business logic description]
    columns:
      - name: [key_column]
        description: [description]
        tests:
          - not_null
          - unique
      - name: [category_column]
        description: [description]
        tests:
          - accepted_values:
              values: ['HIGH', 'MEDIUM', 'LOW']
```

---

## Phase 5: Marts Layer Generation (Gold)

📖 **Standards Reference**: Load `@standards/01_architecture.md` for MARTS layer responsibilities

### Step 1: Generate Dimension Models

**Naming**: `dim_[entity].sql` (NO domain prefix for marts)

📖 **Standards Reference**: Load `@standards/08_materialization.md` for dimension materialization strategy

**Template**:
```sql
-- $OUTPUT_DIR/models/marts/[domain]/dim_[entity].sql

{{
    config(
        materialized='table',
        cluster_by=['[primary_key]']
    )
}}

-- Talend Source: [Job Name with DIM/LOOKUP keywords]
-- Purpose: [Dimension description]

WITH source_data AS (
    SELECT * FROM {{ ref('int_[domain]__[transformation]') }}
),

dimension_attributes AS (
    SELECT
        [entity_key],
        [attribute_1],
        [attribute_2],
        -- Add surrogate key if needed
        {{ dbt_utils.generate_surrogate_key(['[natural_key_1]', '[natural_key_2]']) }} AS [entity_sk],
        -- Add SCD Type 2 columns if needed
        CURRENT_TIMESTAMP() AS effective_start_date,
        CAST(NULL AS TIMESTAMP) AS effective_end_date,
        TRUE AS is_current
    FROM source_data
)

SELECT * FROM dimension_attributes
```

### Step 2: Generate Fact Models

**Naming**: `fct_[entity].sql` (NO domain prefix for marts)

📖 **Standards Reference**: Reference `@standards/08_materialization.md` and `@standards/12_performance.md` for fact table configuration

**Template**:
```sql
-- $OUTPUT_DIR/models/marts/[domain]/fct_[entity].sql

{{
    config(
        materialized='incremental',
        partition_by={
            'field': '[date_column]',
            'data_type': 'date',
            'granularity': 'day'
        },
        cluster_by=['[dimension_key_1]', '[dimension_key_2]'],
        unique_key='[fact_primary_key]'
    )
}}

-- Talend Source: [Job Name with FACT/TRANSACTION keywords]
-- Purpose: [Fact description]

WITH fact_data AS (
    SELECT * FROM {{ ref('int_[domain]__[facts]') }}
),

dimension_lookups AS (
    -- Join with dimensions to enrich fact
    SELECT
        f.*,
        d1.[dimension_attribute] AS [enriched_attribute_1],
        d2.[dimension_attribute] AS [enriched_attribute_2]
    FROM fact_data f
    LEFT JOIN {{ ref('dim_[entity_1]') }} d1
        ON f.[fk_entity_1] = d1.[entity_1_key]
    LEFT JOIN {{ ref('dim_[entity_2]') }} d2
        ON f.[fk_entity_2] = d2.[entity_2_key]
)

SELECT * FROM dimension_lookups

{% if is_incremental() %}
    -- Incremental logic: only load new/changed records
    WHERE [date_column] > (SELECT MAX([date_column]) FROM {{ this }})
{% endif %}
```

**✅ Marts Layer Rules** (from standards):
- ✅ Dimensional models (star schema)
- ✅ Facts (transactions, events, metrics)
- ✅ Dimensions (customers, products, dates)
- ✅ Surrogate keys, SCD Type 2 if needed
- ❌ NO raw data (only from intermediate)
- ❌ NO cross-mart dependencies (facts should not reference other facts)

### Step 3: Generate Marts _schema.yml

Create schema documentation with relationship tests:

```yaml
# $OUTPUT_DIR/models/marts/[domain]/_schema.yml
version: 2

models:
  - name: dim_[entity]
    description: [Dimension description]
    columns:
      - name: [entity_key]
        description: [Primary key description]
        tests:
          - not_null
          - unique

  - name: fct_[entity]
    description: [Fact description]
    columns:
      - name: [fact_primary_key]
        description: [Primary key description]
        tests:
          - not_null
          - unique
      - name: [fk_dimension]
        description: [Foreign key description]
        tests:
          - not_null
          - relationships:
              to: ref('dim_[entity]')
              field: [entity_key]
```

---

## Phase 6: Supporting Files Generation

### Step 1: Generate Talend Compatibility Macros

Create macros for any Talend-specific functions not directly translatable:

```sql
-- $OUTPUT_DIR/macros/talend_compatibility.sql

{% macro talend_date_format(date_column, format_pattern) %}
    FORMAT_DATE('{{ format_pattern }}', {{ date_column }})
{% endmacro %}

{% macro talend_string_handling_upcase(string_column) %}
    UPPER({{ string_column }})
{% endmacro %}

-- Add other custom macros as needed for complex Talend patterns
```

### Step 2: Generate Migration Report

Create comprehensive documentation of the migration:

```markdown
<!-- $OUTPUT_DIR/docs/migration_report.md -->

# Talend to DBT Migration Report

## Executive Summary

- **Migration Date**: [Date]
- **Source**: [Talend project name/path]
- **Jobs Migrated**: [X total jobs]
- **Models Generated**: [Y total models] ([Z]% reduction from jobs)
- **SQL Coverage**: [XX]%
- **Test Coverage**: [XX]%

## Migration Metrics

### Quality Scorecard

| Metric | Target | Actual | Status |
|--------|--------|--------|--------|
| SQL Generation Coverage | ≥98% | [XX]% | [✅/❌] |
| Business Logic Preservation | 100% | [XX]% | [✅/❌] |
| Model Reduction | 70-85% | [XX]% | [✅/❌] |
| Manual Intervention Required | <2% | [XX]% | [✅/❌] |
| Test Coverage | ≥90% | [XX]% | [✅/❌] |
| Source Coverage | 100% | [XX]% | [✅/❌] |
| Dependency Preservation | 100% | [XX]% | [✅/❌] |
| Naming Compliance | 100% | [XX]% | [✅/❌] |

## Job Classification

### Orchestrators (No SQL Generated)
- [Job Name 1] - [Reason: has tRunJob calls]
- [Job Name 2] - [Reason: pure orchestration]

### Consolidated Models
- [Model Name] consolidates: [Job A, Job B, Job C]
  - Reason: [Sequential transformations, same grain]

### Separate Models (Transaction/Audit Boundaries)
- [Model Name] - [Reason: Transaction boundary]
- [Model Name_rejects] - [Reason: Audit/error flow]

## Domain Mapping

| Talend Job | Extracted Domain | DBT Model | Layer |
|-----------|------------------|-----------|-------|
| [Job Name] | [domain] | [model_name].sql | [staging/intermediate/marts] |

## Component Translation Summary

- **tDBInput**: [X] components → [Y] source definitions + [Z] staging models
- **tMap**: [X] components → [Y] CTEs with expressions
- **tFilterRow**: [X] components → [Y] WHERE clauses
- **tAggregateRow**: [X] components → [Y] GROUP BY statements
- **tRunJob**: [X] components → [Y] ref() dependencies

## Context Variable Mappings

| Talend Context Variable | DBT Equivalent | Usage Count |
|------------------------|----------------|-------------|
| [context.schema] | {{ target.dataset }} | [X] |
| [context.from_date] | {{ var('from_date') }} | [Y] |

## Validation Results

### Pre-Generation Validation
- ✅ All input files present
- ✅ [X] jobs extracted
- ✅ [Y] domains identified
- ✅ [Z] source tables found

### Post-Generation Validation
- ✅ SQL completeness check: [X] models, [Y] placeholders ([Z]%)
- ✅ Naming compliance: [X] models compliant
- ✅ No circular dependencies detected
- ✅ All sources defined in _sources.yml

## Manual Review Required

### Constraint Violations
[List any models that could not be automatically generated due to constraint violations]

⚠️ CONSTRAINT VIOLATION:
Model: [model_name]
Constraint: [which constraint]
Talend Pattern: [description]
Recommendation: [suggested solution]
Status: SKIPPED - requires manual review

### Flagged Items
- [Item 1]: [Description of issue]
- [Item 2]: [Description of issue]

## Next Steps

1. Review all models flagged for manual review
2. Run `dbt parse --no-partial-parse` to validate project structure
3. Run `dbt run` to execute all models
4. Run `dbt test` to validate data quality
5. Review migration report for any warnings or errors
6. Proceed to Phase 3 post-processing: `/03-post-process $OUTPUT_DIR`
```

---

## Phase 7: Quality Validation

📖 **Standards Reference**: Load `@standards/10_quality_validation.md` for quality metrics and validation

### Step 1: Calculate Migration Quality Scorecard

**Calculate ALL metrics from standards:**

```python
# SQL Generation Coverage
total_models = count_all_sql_files(OUTPUT_DIR)
complete_models = count_models_without_placeholders(OUTPUT_DIR)
sql_coverage = (complete_models / total_models) * 100
assert sql_coverage >= 98, f"SQL coverage {sql_coverage}% below target 98%"

# Model Reduction
talend_jobs = metadata["total_jobs"]
dbt_models = total_models
consolidation_score = (dbt_models / talend_jobs)
assert 0.15 <= consolidation_score <= 0.30, f"Consolidation score {consolidation_score} outside target range 0.15-0.30"
model_reduction_pct = (1 - consolidation_score) * 100
assert model_reduction_pct >= 70, f"Model reduction {model_reduction_pct}% below target 70%"

# Source Coverage
tdb_input_tables = count_unique_tables_in_tdb_input(talend_data)
sources_defined = count_sources_in_sources_yml(OUTPUT_DIR)
source_coverage = (sources_defined / tdb_input_tables) * 100
assert source_coverage == 100, f"Source coverage {source_coverage}% - missing {tdb_input_tables - sources_defined} source definitions"

# Naming Compliance
all_models = find_all_sql_files(OUTPUT_DIR)
compliant_models = [m for m in all_models if matches_naming_pattern(m)]
naming_compliance = (len(compliant_models) / len(all_models)) * 100
assert naming_compliance == 100, f"Naming compliance {naming_compliance}% - {len(all_models) - len(compliant_models)} files non-compliant"

# Dependency Preservation
trun_job_count = count_trun_job_components(talend_data)
ref_count = count_ref_calls_in_models(OUTPUT_DIR)
dependency_preservation = (ref_count / trun_job_count) * 100 if trun_job_count > 0 else 100
assert dependency_preservation >= 95, f"Dependency preservation {dependency_preservation}% - some tRunJob calls not converted to ref()"
```

### Step 2: Run Post-Generation Validation

📖 **Standards Reference**: Reference `@standards/10_quality_validation.md` for post-generation validation checks

**Run ALL validation checks from standards:**

```bash
# Change to output directory
cd "$OUTPUT_DIR"

# Check 1: SQL completeness (no placeholders)
echo "=== SQL Completeness Check ==="
PLACEHOLDER_COUNT=$(grep -r "TODO\|PLACEHOLDER\|FIXME" models/*.sql 2>/dev/null | wc -l)
echo "Placeholders found: $PLACEHOLDER_COUNT"
if [ "$PLACEHOLDER_COUNT" -gt 0 ]; then
    echo "⚠️  WARNING: Found $PLACEHOLDER_COUNT placeholder comments"
    grep -r "TODO\|PLACEHOLDER\|FIXME" models/*.sql | head -20
fi

# Check 2: Naming compliance
echo ""
echo "=== Naming Compliance Check ==="
NON_COMPLIANT=$(find models -name "*.sql" -not -path "*/macros/*" | grep -v -E "(stg_|int_|fct_|dim_)[a-z0-9_]+\.sql" | wc -l)
echo "Non-compliant file names: $NON_COMPLIANT"
if [ "$NON_COMPLIANT" -gt 0 ]; then
    echo "❌ ERROR: Found $NON_COMPLIANT non-compliant file names"
    find models -name "*.sql" | grep -v -E "(stg_|int_|fct_|dim_)[a-z0-9_]+\.sql"
    exit 1
fi

# Check 3: Circular dependency check
echo ""
echo "=== Circular Dependency Check ==="
if command -v dbt &> /dev/null; then
    dbt list --select "+fct_*" --output json > /dev/null 2>&1
    if [ $? -eq 0 ]; then
        echo "✅ No circular dependencies detected"
    else
        echo "❌ ERROR: Circular dependency or invalid ref() detected"
        dbt list --select "+fct_*"
        exit 1
    fi
else
    echo "⚠️  WARNING: dbt not available, skipping circular dependency check"
fi

# Check 4: Source coverage
echo ""
echo "=== Source Coverage Check ==="
SOURCE_COUNT=$(grep -c "name:" models/staging/_sources.yml 2>/dev/null || echo 0)
echo "Sources defined: $SOURCE_COUNT"
if [ "$SOURCE_COUNT" -eq 0 ]; then
    echo "❌ ERROR: No sources defined in _sources.yml"
    exit 1
fi
```

### Step 3: Standards Compliance Self-Check

**Verify compliance with ALL absolute constraints from standards:**

📖 **Standards Reference**: Load `@standards/13_constraints.md` for absolute constraints verification

```python
# Constraint Check 1: NO placeholder SQL
placeholder_violations = find_placeholder_sql(OUTPUT_DIR)
if len(placeholder_violations) > 0:
    print("❌ CONSTRAINT VIOLATION: Placeholder SQL found")
    for violation in placeholder_violations:
        print(f"   - {violation}")

# Constraint Check 2: NO naming violations
naming_violations = find_naming_violations(OUTPUT_DIR)
if len(naming_violations) > 0:
    print("❌ CONSTRAINT VIOLATION: Naming violations found")
    for violation in naming_violations:
        print(f"   - {violation}")

# Constraint Check 3: NO over-consolidation (orchestrators preserved)
orchestrator_violations = find_orchestrator_consolidations(OUTPUT_DIR, talend_data)
if len(orchestrator_violations) > 0:
    print("❌ CONSTRAINT VIOLATION: Orchestrator jobs were consolidated")
    for violation in orchestrator_violations:
        print(f"   - {violation}")

# Constraint Check 4: NO circular dependencies
circular_deps = find_circular_dependencies(OUTPUT_DIR)
if len(circular_deps) > 0:
    print("❌ CONSTRAINT VIOLATION: Circular dependencies detected")
    for violation in circular_deps:
        print(f"   - {violation}")

# Constraint Check 5: NO undefined sources
undefined_sources = find_undefined_sources(OUTPUT_DIR)
if len(undefined_sources) > 0:
    print("❌ CONSTRAINT VIOLATION: Undefined sources referenced")
    for violation in undefined_sources:
        print(f"   - {violation}")

# Summary
all_violations = placeholder_violations + naming_violations + orchestrator_violations + circular_deps + undefined_sources
if len(all_violations) == 0:
    print("✅ ALL CONSTRAINT CHECKS PASSED")
else:
    print(f"❌ {len(all_violations)} CONSTRAINT VIOLATIONS FOUND - REVIEW REQUIRED")
```

---

## Phase 8: Final Summary & Next Steps

### Migration Summary

Display comprehensive migration summary:

```bash
echo ""
echo "========================================="
echo "✅ TALEND TO DBT MIGRATION COMPLETE"
echo "========================================="
echo ""
echo "📊 Migration Statistics:"
echo "   - Talend Jobs Analyzed: [X]"
echo "   - DBT Models Generated: [Y]"
echo "   - Model Reduction: [Z]%"
echo "   - SQL Coverage: [XX]%"
echo "   - Source Tables: [N]"
echo "   - Domains Identified: [M]"
echo ""
echo "📁 Output Location:"
echo "   $OUTPUT_DIR"
echo ""
echo "📋 Files Generated:"
echo "   - Staging models: [X]"
echo "   - Intermediate models: [Y]"
echo "   - Marts models (facts/dims): [Z]"
echo "   - Source definitions: [N] tables"
echo "   - Schema files: [M]"
echo "   - Migration report: docs/migration_report.md"
echo ""
echo "✅ Quality Checks:"
echo "   - SQL completeness: [XX]%"
echo "   - Naming compliance: [XX]%"
echo "   - Source coverage: [XX]%"
echo "   - No circular dependencies: [✅/❌]"
echo ""
echo "========================================="
echo "📋 NEXT STEPS"
echo "========================================="
echo ""
echo "1️⃣  Review the migration report:"
echo "    cat $OUTPUT_DIR/docs/migration_report.md"
echo ""
echo "2️⃣  Address any manual review items (if flagged)"
echo ""
echo "3️⃣  Clear context (REQUIRED before Phase 3):"
echo "    /clear"
echo ""
echo "4️⃣  Run Phase 3 quality validation:"
echo "    /03-post-process $OUTPUT_DIR"
echo ""
echo "Phase 3 will:"
echo "  • Format and lint all SQL files with sqlfluff"
echo "  • Validate all YAML files with yamllint"
echo "  • Parse and compile the DBT project (dbt parse)"
echo "  • Format markdown documentation"
echo "  • Generate comprehensive quality report"
echo ""
echo "========================================="
```

---

## Constraint Violation Protocol

📖 **Standards Reference**: Reference `@standards/13_constraints.md` for constraint violation protocol

**IF any constraint cannot be followed during migration:**

1. ⏸️ **STOP generation** for that specific model (not entire migration)

2. 📝 **Document the conflict** in migration report:
   ```
   ⚠️ CONSTRAINT VIOLATION:
   Model: [model_name]
   Constraint: [which constraint from standards]
   Talend Pattern: [description of what was found in Talend]
   Recommendation: [suggested manual solution]
   Status: SKIPPED - requires manual review
   ```

3. ✅ **Continue with remaining models** (don't let one violation stop entire migration)

4. 🚩 **Include violation summary** in final migration report

**Common constraint violations:**
- Cannot extract domain from job name → Flag for manual domain assignment
- Complex tMap expression has no direct SQL translation → Flag for manual SQL writing
- Circular tRunJob dependency detected → Flag for dependency refactoring
- Context variable not in mapping table → Flag for manual variable handling

---

## Success Criteria Checklist

📖 **Standards Reference**: Reference `@standards/13_constraints.md` for migration success criteria

**✅ Migration is successful when ALL criteria are met:**

- [ ] SQL Generation Coverage ≥98%
- [ ] Business Logic Preservation = 100% (all tMap expressions translated)
- [ ] Model Reduction = 70-85%
- [ ] All models follow naming standards (stg_*/int_*/fct_*/dim_* pattern)
- [ ] All tDBInput tables defined in _sources.yml (100% source coverage)
- [ ] All tRunJob dependencies converted to ref() (100% dependency preservation)
- [ ] No circular dependencies detected (dbt list passes)
- [ ] No placeholder SQL (TODO/PLACEHOLDER/FIXME removed or <2%)
- [ ] All constraints followed (or violations documented)
- [ ] Consolidation score in target range (0.15 ≤ score ≤ 0.30)
- [ ] dbt parse completes successfully (project structure valid)
- [ ] Test coverage ≥90% (tests defined for all critical models)

**If any criterion is NOT met:**
- Document the gap in migration report
- Flag for manual review
- Provide recommendation for resolution

---

## Execution Guarantee

**The migration ALWAYS follows this order:**

1. **Reference Standards** - Load relevant standards files from `/standards/` directory as needed for each phase
2. **Verify Inputs** - Validate all 5 input files present and complete
3. **Analyze** - Classify jobs, extract domains, detect layers
4. **Validate** - Run pre-generation validation checks
5. **Generate Structure** - Create DBT project folders and config files
6. **Generate Staging** - Create sources.yml + staging models (Bronze layer)
7. **Generate Intermediate** - Apply consolidation + create transformation models (Silver layer)
8. **Generate Marts** - Create facts + dimensions (Gold layer)
9. **Generate Support** - Create macros, tests, documentation
10. **Validate** - Run post-generation quality checks
11. **Report** - Generate migration report with metrics
12. **Summary** - Display completion summary with next steps

**At each step, reference standards for HOW to implement the step.**

---

## End of Migration Command

Generate production-ready DBT code that can be deployed immediately to BigQuery with minimal manual intervention.