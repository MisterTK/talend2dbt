______________________________________________________________________

## description: Complete Talend to DBT migration with 98%+ SQL generation from pre-processed Talend output argument-hint: \<processed_talend_path> [output_path] [--merge-into \<existing_project>] allowed-tools: Bash, Read, Write, Edit, Glob, Grep, TodoWrite

# Talend to DBT Migration Engine

You are an expert data engineer implementing a complete Talend to DBT migration for BigQuery. You
will generate a production-ready DBT project with 98%+ SQL generation and 100% business logic
preservation from pre-processed Talend output.

**🆕 RECENT UPDATES (v3.2.0):**

- **Python Helper Integration**: File operations, validation, and metrics now handled by
  `dbt_generator` Python package
- **Merge Mode Support**: Can now add models to existing DBT projects with `--merge-into` flag
- **Hybrid Architecture**: Python handles infrastructure (file ops, validation, metrics), LLM
  handles intelligence (SQL generation, standards interpretation)
- **Workload Classification**: Migration classifies workloads as Analytical (with marts) or
  Operational (without marts)
- **Marts Structural Inference**: If no DIM/FACT keywords found, attempts to infer from SQL patterns
- **Enhanced Validation**: Python validates file structure, LLM validates against standards

**🔧 Architecture Philosophy:**

- **Python Helper (`dbt_generator`)**: File I/O, directory structure, validation, metrics
  calculation (no standards interpretation)
- **LLM (this command)**: Job classification, layer detection, consolidation decisions, SQL
  generation, standards compliance
- **Standards (`/standards/`)**: Single source of truth - LLM interprets them, Python never
  hardcodes them

## Command Usage

```bash
# Create new DBT project:
/02-migrate <processed_talend_path> [output_path]

# OR merge into existing DBT project:
/02-migrate <processed_talend_path> --merge-into <existing_dbt_project_path>
```

**Arguments:**

- `$1` (processed_talend_path, required): Path to directory containing pre-processed Talend output
  files
- `$2` (output_path, optional): Output directory for NEW DBT project (default:
  `<processed_talend_path>/../dbt_migrated`)
- `$2` or `$3` (--merge-into, optional): Flag to merge into existing project
- `$3` or `$4` (existing_project_path, optional): Path to existing DBT project for merge mode

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

______________________________________________________________________

## 📖 CRITICAL: Migration Standards Reference

**Standards files are located in `/standards/` directory. Reference the appropriate file for each
phase:**

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

**Load the relevant standards file when you reach each phase. All code generation, naming decisions,
and consolidation logic MUST comply with these standards.**

______________________________________________________________________

## Mission Statement

Transform Talend ETL jobs into production-ready DBT models that:

- Generate **98%+ complete SQL** (not placeholders)
- Preserve **100% of business logic** and data transformations
- Reduce model count by **70-85%** through intelligent consolidation
- Require **\<2% manual intervention** post-migration
- Run immediately on **BigQuery with latest 2025 features**

______________________________________________________________________

## Phase 0: Mode Detection & Validation (Python Helper)

### Step 1: Determine Migration Mode

```bash
# Check if merge mode is enabled
if [[ "$2" == "--merge-into" && -n "$3" ]]; then
    MIGRATION_MODE="merge"
    EXISTING_PROJECT_PATH="$3"
    OUTPUT_DIR="$3"
    echo "🔀 MERGE MODE: Adding models to existing project at $EXISTING_PROJECT_PATH"
elif [[ "$3" == "--merge-into" && -n "$4" ]]; then
    MIGRATION_MODE="merge"
    EXISTING_PROJECT_PATH="$4"
    OUTPUT_DIR="$4"
    echo "🔀 MERGE MODE: Adding models to existing project at $EXISTING_PROJECT_PATH"
else
    MIGRATION_MODE="new"
    OUTPUT_DIR="${2:-$(dirname "$1")/dbt_migrated}"
    echo "🆕 NEW PROJECT MODE: Creating fresh DBT project at $OUTPUT_DIR"
fi

echo "Migration mode: $MIGRATION_MODE"
echo "Input directory: $1"
echo "Output directory: $OUTPUT_DIR"
```

### Step 2: Validate Input Files (Python Helper)

```bash
# Use Python helper to validate inputs
python -m dbt_generator.generator validate --input "$1" --mode inputs > /tmp/input_validation.json

# Check validation results
VALIDATION_RESULT=$(cat /tmp/input_validation.json)
IS_VALID=$(echo "$VALIDATION_RESULT" | jq -r '.valid')

if [[ "$IS_VALID" != "true" ]]; then
    echo "❌ ERROR: Input validation failed"
    echo "$VALIDATION_RESULT" | jq -r '.missing[]' | while read file; do
        echo "  Missing: $file"
    done
    exit 1
fi

echo "✅ All input files present"
```

### Step 3: Validate Existing Project (If Merge Mode)

```bash
if [[ "$MIGRATION_MODE" == "merge" ]]; then
    # Use Python helper to validate existing project
    python -m dbt_generator.generator validate --output "$OUTPUT_DIR" --mode project > /tmp/project_validation.json

    PROJECT_VALID=$(cat /tmp/project_validation.json | jq -r '.valid')

    if [[ "$PROJECT_VALID" != "true" ]]; then
        echo "❌ ERROR: Not a valid DBT project"
        cat /tmp/project_validation.json | jq -r '.error'
        exit 1
    fi

    EXISTING_PROJECT_NAME=$(cat /tmp/project_validation.json | jq -r '.project_name')
    echo "✅ Validated existing project: $EXISTING_PROJECT_NAME"

    # Backup existing _sources.yml
    python -m dbt_generator.generator backup-sources --output "$OUTPUT_DIR" > /tmp/backup_result.json
    BACKUP_PATH=$(cat /tmp/backup_result.json | jq -r '.backup_path')

    if [[ "$BACKUP_PATH" != "null" ]]; then
        echo "📦 Backed up _sources.yml to: $BACKUP_PATH"
    fi
fi
```

______________________________________________________________________

## Phase 1: Initial Analysis & Extraction

### Step 1: Load Talend Data (Python Helper)

```bash
# Use Python helper to load all Talend files
python -m dbt_generator.generator load --input "$1" --output /tmp/talend_data.json

# Load into context for LLM
TALEND_DATA=$(cat /tmp/talend_data.json)

echo "✅ Loaded Talend data files"
```

### Step 2: Parse Talend Data (LLM Analysis)

**The Python helper has loaded all files. Now LLM analyzes the structure:**

```python
# Data is already loaded from Python helper
talend_extraction = TALEND_DATA['extraction']         # Complete job structure
sql_queries = TALEND_DATA['sql_queries']               # All extracted SQL
transformations = TALEND_DATA['transformations']       # tMap business logic
context_vars = TALEND_DATA['context_vars']             # Variable mappings
summary = TALEND_DATA['summary']                       # Migration overview

# Now proceed with LLM analysis...
```

### Step 3: Analyze Job Structure

📖 **Standards Reference**: Load `@standards/03_model_consolidation.md` for complete consolidation
strategy

**Classify all jobs using the consolidation decision tree:**

1. **Identify Orchestrator Jobs** (DO NOT CONSOLIDATE):

   - Jobs with `has_child_jobs = true` (contain tRunJob components)
   - Role: Grandmaster or Master
   - Action: Document orchestration in comments, generate NO SQL models

1. **Identify Transaction Groups** (PRESERVE AS ATOMIC MODELS):

   - Jobs with tDBConnection → operations → tDBCommit/tDBRollback
   - Action: One model per transaction group (atomic boundary)

1. **Identify Error/Audit Flows** (SEPARATE MODELS):

   - Jobs with reject flows (tFilterRow REJECT branch, tDie, tWarn)
   - Action: Create separate `[model_name]_rejects.sql` for audit

1. **Identify Processor Jobs** (SAFE TO CONSOLIDATE):

   - Child jobs with data transformations (tMap, tFilter, tAggregate, etc.)
   - No tRunJob calls, no transaction boundaries, no error flows
   - Action: Apply consolidation rules to reduce model count

**Document job hierarchy and dependencies:**

- Map ALL parent-child relationships (tRunJob references)
- Identify ALL cross-job lookups (dimension references)
- Preserve ALL dependencies as DBT `ref()` relationships

**Create classification tracking structure:**

```python
# Initialize job classification storage
job_classifications = {
    "orchestrators": [],      # Jobs with tRunJob (no SQL generated)
    "processors": [],         # Jobs eligible for consolidation
    "transactions": [],       # Jobs with transaction boundaries
    "error_handlers": []      # Jobs with reject/audit flows
}

# Populate based on analysis
for job in all_jobs:
    if job.get("has_child_jobs"):
        job_classifications["orchestrators"].append(job)
    elif has_transaction_boundary(job):
        job_classifications["transactions"].append(job)
    elif has_error_flows(job):
        job_classifications["error_handlers"].append(job)
    else:
        job_classifications["processors"].append(job)

print(f"Job Classification Summary:")
print(f"  Orchestrators: {len(job_classifications['orchestrators'])}")
print(f"  Processors: {len(job_classifications['processors'])}")
print(f"  Transactions: {len(job_classifications['transactions'])}")
print(f"  Error Handlers: {len(job_classifications['error_handlers'])}")
```

### Step 4: Extract Domains for Naming

📖 **Standards Reference**: Load `@standards/02_naming_conventions.md` for domain extraction and
layer detection rules

**Apply domain extraction patterns to ALL jobs:**

1. **From Talend Job Names**:

   - Pattern: `[PROJECT_CODE]_[Entity]_[Type]_[Job]` → extract `project_code` as domain
   - Pattern: `[PROJECT]_[Module]_[Entity]` → extract `project_module` as domain
   - Pattern: `Master_[Domain]_[Action]` → extract `domain` as domain

1. **From Talend Folder Hierarchy**:

   - Path: `workspace/PROJECT_NAME/process/Jobs/job.item` → extract `project_name` as domain

1. **Domain Validation**:

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

📖 **Standards Reference**: Reference `@standards/02_naming_conventions.md` for layer detection
patterns

**Assign each PROCESSOR job to a layer (staging/intermediate/marts):**

**IMPORTANT: Only processor jobs generate models. Orchestrators are documented as comments only.**

```python
# Initialize layer assignment storage
layer_assignments = {
    "staging": [],           # Source table 1:1 mappings
    "intermediate": [],      # Business logic transformations
    "marts_dimensions": [],  # Dimensional lookup tables
    "marts_facts": []        # Fact/transaction tables
}

# Assign ONLY processor jobs to layers
for job in job_classifications["processors"]:
    job_name_upper = job.name.upper()

    # Priority 1: Marts detection (highest specificity)
    if any(keyword in job_name_upper for keyword in ["DIM", "DIMENSION", "LKP", "LOOKUP", "_D_"]):
        layer_assignments["marts_dimensions"].append(job)
        print(f"  → {job.name} assigned to MARTS (dimension)")

    elif any(keyword in job_name_upper for keyword in ["FACT", "TRN", "TRANSACTION", "_F_", "METRICS"]):
        layer_assignments["marts_facts"].append(job)
        print(f"  → {job.name} assigned to MARTS (fact)")

    # Priority 2: Staging detection
    elif any(keyword in job_name_upper for keyword in ["LOAD", "EXTRACT", "SOURCE", "RAW"]):
        # Additional check: only tDBInput + minimal tMap (type casting)
        if is_simple_extraction(job):
            layer_assignments["staging"].append(job)
            print(f"  → {job.name} assigned to STAGING")
        else:
            # Complex transformation despite "LOAD" keyword
            layer_assignments["intermediate"].append(job)
            print(f"  → {job.name} assigned to INTERMEDIATE (complex load)")

    # Priority 3: Intermediate detection (default for unmatched)
    else:
        layer_assignments["intermediate"].append(job)
        print(f"  → {job.name} assigned to INTERMEDIATE (default)")

print(f"\nLayer Assignment Summary:")
print(f"  Staging: {len(layer_assignments['staging'])}")
print(f"  Intermediate: {len(layer_assignments['intermediate'])}")
print(f"  Marts Dimensions: {len(layer_assignments['marts_dimensions'])}")
print(f"  Marts Facts: {len(layer_assignments['marts_facts'])}")

# CRITICAL: Check for marts candidates
marts_count = len(layer_assignments['marts_dimensions']) + len(layer_assignments['marts_facts'])
if marts_count == 0:
    print("\n⚠️  WARNING: No marts candidates found based on naming patterns")
    print("Migration will use structural inference in Phase 5 to identify dimensional models")
    print("If no marts are inferred, all models will remain in staging/intermediate layers")
    print("This is VALID for operational workloads (reconciliation, data quality, ETL)")
```

### Step 6: Save Analysis Results & Pre-Generation Validation

📖 **Standards Reference**: Load `@standards/10_quality_validation.md` for pre-generation validation
checks

**Save analysis results for Python helper to use:**

```bash
# Create analysis results JSON for Python helper
cat > /tmp/analysis_results.json <<EOF
{
  "job_classifications": $(echo "$job_classifications" | python3 -c "import sys, json; print(json.dumps(eval(sys.stdin.read())))"),
  "domains": $(echo "$domains" | python3 -c "import sys, json; print(json.dumps(eval(sys.stdin.read())))"),
  "layer_assignments": $(echo "$layer_assignments" | python3 -c "import sys, json; print(json.dumps(eval(sys.stdin.read())))")
}
EOF

ANALYSIS_RESULTS=$(cat /tmp/analysis_results.json)
echo "✅ Analysis results saved"
```

**Run validation checks (LLM interprets against standards):**

```python
# Check 1: Input files (already validated by Python in Phase 0)
# TALEND_DATA is loaded and validated

# Check 2: Extraction quality
metadata = talend_extraction["extraction_metadata"]
total_jobs = metadata["total_jobs"]
total_sql = metadata["total_sql_queries"]
unique_tables = metadata["unique_tables"]

if total_jobs == 0:
    raise ValueError("No jobs found in extraction")
if total_sql == 0:
    raise ValueError("No SQL queries extracted")
if unique_tables == 0:
    raise ValueError("No source tables identified")

# Check 3: Domain extraction
unique_domains = set(domains.values())
if len(unique_domains) == 0:
    raise ValueError("No domains identified for naming")

# Check 4: Consolidation feasibility
orchestrators_count = len(job_classifications["orchestrators"])
processors_count = len(job_classifications["processors"])

if processors_count == 0:
    raise ValueError("No processor jobs to consolidate")

# Check 5: Marts feasibility (WARNING only, not blocking)
marts_count = len(layer_assignments["marts_dimensions"]) + len(layer_assignments["marts_facts"])
if marts_count == 0:
    print("⚠️  WARNING: No marts candidates identified - this is VALID for operational workloads")

print(f"✅ Pre-generation validation passed:")
print(f"   - Total jobs: {total_jobs}")
print(f"   - Orchestrators: {orchestrators_count}")
print(f"   - Processors: {processors_count}")
print(f"   - Marts candidates: {marts_count}")
print(f"   - Domains identified: {len(unique_domains)}")
print(f"   - Source tables: {unique_tables}")
```

**If any validation fails, STOP migration and report specific error.**

______________________________________________________________________

## Phase 2: DBT Project Generation

### Step 1: Create DBT Project Structure (Python Helper)

📖 **Standards Reference**: Load `@standards/07_file_organization.md` for complete directory
structure

**Use Python helper to create directory structure:**

```bash
# OUTPUT_DIR and MIGRATION_MODE are already set from Phase 0

# Extract unique domains from analysis
UNIQUE_DOMAINS=$(echo "$ANALYSIS_RESULTS" | jq -r '.domains | unique | .[]' | tr '\n' ' ')

# Use Python helper to create/enhance directory structure
python -m dbt_generator.generator setup \
    --input "$1" \
    --output "$OUTPUT_DIR" \
    --mode "$MIGRATION_MODE" \
    --domains $UNIQUE_DOMAINS > /tmp/setup_result.json

# Check setup results
FOLDERS_CREATED=$(cat /tmp/setup_result.json | jq -r '.folders_created | length')
echo "✅ Created $FOLDERS_CREATED folders"

if [[ "$MIGRATION_MODE" == "merge" ]]; then
    echo "📁 Enhanced existing project with new domain folders"
else
    echo "📁 Created new DBT project structure"
fi
```

### Step 2: Generate dbt_project.yml (LLM - Only if New Mode)

**Skip this step if MIGRATION_MODE is 'merge' - don't overwrite existing dbt_project.yml**

```bash
if [[ "$MIGRATION_MODE" == "new" ]]; then
    echo "Creating dbt_project.yml for new project..."
else
    echo "⏭️  Skipping dbt_project.yml (merge mode - preserving existing)"
    # Skip to next step
fi
```

**If creating new project, generate:**

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

______________________________________________________________________

## Phase 3: Staging Layer Generation (Bronze)

📖 **Standards Reference**: Load `@standards/01_architecture.md` for STAGING layer responsibilities

### Step 1: Generate \_sources.yml

**Define ALL source tables from tDBInput components:**

📖 **Standards Reference**: Reference `@standards/07_file_organization.md` for \_sources.yml format

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

**✅ CRITICAL**: ALL tables referenced in ANY tDBInput component MUST be defined in \_sources.yml

### Step 2: Generate Staging Models

**For each source table, create a staging model:**

📖 **Standards Reference**: Load `@standards/04_sql_generation.md` for SQL generation standards and
CTE patterns

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

### Step 3: Generate Staging \_schema.yml

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

______________________________________________________________________

## Phase 4: Intermediate Layer Generation (Silver)

📖 **Standards Reference**: Load `@standards/01_architecture.md` for INTERMEDIATE layer
responsibilities

### Step 1: Apply Consolidation Rules

📖 **Standards Reference**: Reference `@standards/03_model_consolidation.md` for consolidation rules
and checklist

**For each processor job, determine if it should be consolidated:**

**✅ CONSOLIDATE if:**

1. Sequential single-path transformations (no branching)
1. Temporary tables with single use
1. Multiple aggregations on same grain
1. Small lookup tables (\<10K rows, static)
1. Simple child jobs (\<5 components, no dependencies)

**❌ DO NOT CONSOLIDATE if:**

1. Orchestrator jobs (has tRunJob)
1. Transaction boundaries (tDBConnection/Commit/Rollback)
1. Error/reject flows (separate audit model)
1. Reusable transformations (>3 dependencies)
1. Different materialization strategies
1. Different update cadences
1. Cross-domain jobs

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

📖 **Standards Reference**: Load `@standards/05_component_mapping.md` for complete component-to-SQL
translation table

**Use the component translation table to convert each Talend component:**

| Talend Component   | SQL Pattern                                         | Reference          |
| ------------------ | --------------------------------------------------- | ------------------ |
| tDBInput           | `SELECT * FROM {{ source() }}`                      | Standards line 526 |
| tMap (expressions) | `CASE WHEN ... THEN ... END`, `CONCAT()`, `UPPER()` | Standards line 527 |
| tMap (joins)       | `LEFT/INNER JOIN ON condition`                      | Standards line 528 |
| tFilterRow         | `WHERE condition`                                   | Standards line 529 |
| tAggregateRow      | `GROUP BY ... HAVING ...`                           | Standards line 530 |
| tJoin              | `JOIN ... ON ...`                                   | Standards line 531 |
| tSortRow           | `ORDER BY`                                          | Standards line 532 |
| tUniqRow           | `DISTINCT` or `ROW_NUMBER() OVER()`                 | Standards line 533 |
| tRunJob            | `ref('child_model')`                                | Standards line 538 |

### Step 3: Translate Talend Expressions

📖 **Standards Reference**: Reference `@standards/05_component_mapping.md` for expression translation
mappings

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

| Talend Context         | DBT Equivalent                   |
| ---------------------- | -------------------------------- |
| `context.schema`       | `{{ target.dataset }}`           |
| `context.from_date`    | `{{ var('from_date') }}`         |
| `context.env`          | `{{ target.name }}`              |
| `context.project_id`   | `{{ target.project }}`           |
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

### Step 6: Generate Intermediate \_schema.yml

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

______________________________________________________________________

## Phase 5: Marts Layer Generation (Gold)

📖 **Standards Reference**: Load `@standards/01_architecture.md` for MARTS layer responsibilities

**CRITICAL: Check if marts candidates were identified in Phase 1, Step 5**

```python
marts_dimensions = layer_assignments["marts_dimensions"]
marts_facts = layer_assignments["marts_facts"]
marts_count = len(marts_dimensions) + len(marts_facts)

if marts_count == 0:
    print("⚠️  No marts candidates found from keyword detection")
    print("Attempting structural inference to identify dimensional models...")

    # FALLBACK: Infer dimensions from intermediate model structure
    for model in layer_assignments["intermediate"]:
        model_sql = get_generated_sql(model)  # SQL content from Phase 4

        # Dimension pattern detection
        has_surrogate_key = "generate_surrogate_key" in model_sql.lower()
        has_scd_columns = any(col in model_sql.lower() for col in [
            "effective_start_date", "effective_end_date", "is_current",
            "valid_from", "valid_to", "current_flag"
        ])
        is_lookup = count_references_to_model(model) >= 3  # Referenced by 3+ models
        has_no_measures = not any(agg in model_sql.upper() for agg in [
            "SUM(", "AVG(", "COUNT(*)", "COUNT(DISTINCT"
        ])

        if has_surrogate_key or has_scd_columns or (is_lookup and has_no_measures):
            marts_dimensions.append(model)
            print(f"  ✅ Inferred dimension: {model.name}")

    # FALLBACK: Infer facts from intermediate model structure
    for model in layer_assignments["intermediate"]:
        if model in marts_dimensions:
            continue  # Already classified as dimension

        model_sql = get_generated_sql(model)

        # Fact pattern detection
        join_count = model_sql.upper().count("JOIN")
        has_fk_joins = join_count >= 2  # Multiple foreign keys
        has_measures = any(agg in model_sql.upper() for agg in [
            "SUM(", "AVG(", "COUNT(", "MIN(", "MAX("
        ])
        is_incremental = "materialized='incremental'" in model_sql
        is_large_volume = "partition_by" in model_sql  # Partitioned tables = high volume

        if has_fk_joins and has_measures:
            marts_facts.append(model)
            print(f"  ✅ Inferred fact: {model.name}")

    # Summary after inference
    marts_count = len(marts_dimensions) + len(marts_facts)
    print(f"\nMarts Inference Results:")
    print(f"  Dimensions inferred: {len(marts_dimensions)}")
    print(f"  Facts inferred: {len(marts_facts)}")

    if marts_count == 0:
        print("\n⚠️  No marts inferred from structure")
        print("This is VALID for operational workloads:")
        print("  - Reconciliation jobs (matching, deduplication)")
        print("  - Data quality pipelines (validation, cleansing)")
        print("  - ETL orchestration (loading, staging)")
        print("\nAll models will remain in STAGING and INTERMEDIATE layers")
        print("Skipping Phase 5 (no marts to generate)")
        # SKIP to Phase 6 - do not generate marts models
        SKIP_MARTS_GENERATION = True
    else:
        print(f"\n✅ {marts_count} marts candidates identified via structural inference")
        SKIP_MARTS_GENERATION = False
else:
    print(f"✅ {marts_count} marts candidates identified from keyword detection")
    SKIP_MARTS_GENERATION = False
```

### Step 1: Generate Dimension Models

**ONLY execute if `SKIP_MARTS_GENERATION = False`**

**Naming**: `dim_[entity].sql` (NO domain prefix for marts)

📖 **Standards Reference**: Load `@standards/08_materialization.md` for dimension materialization
strategy

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

**ONLY execute if `SKIP_MARTS_GENERATION = False`**

**Naming**: `fct_[entity].sql` (NO domain prefix for marts)

📖 **Standards Reference**: Reference `@standards/08_materialization.md` and
`@standards/12_performance.md` for fact table configuration

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

### Step 3: Generate Marts \_schema.yml

**ONLY execute if `SKIP_MARTS_GENERATION = False`**

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

______________________________________________________________________

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

## Workload Classification

**Workload Type**: [Analytical / Operational / Hybrid]

**Explanation**:
- **Analytical**: Contains dimensional models (facts/dims) for analytics and reporting
- **Operational**: Reconciliation, data quality, ETL orchestration (no marts needed)
- **Hybrid**: Mix of analytical and operational jobs

**Marts Generation**: [Enabled / Skipped]
- **If Skipped**: No marts keywords detected and no structural patterns identified
- **Reason**: This is a valid operational workload focused on [reconciliation/data quality/ETL]

## Domain Mapping

| Talend Job | Extracted Domain | DBT Model | Layer |
|-----------|------------------|-----------|-------|
| [Job Name] | [domain] | [model_name].sql | [staging/intermediate/marts] |

## Layer Distribution

- **Staging Models**: [X] models
- **Intermediate Models**: [Y] models
- **Marts Models**: [Z] models (dimensions: [A], facts: [B])

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

______________________________________________________________________

## Phase 7: Quality Validation

📖 **Standards Reference**: Load `@standards/10_quality_validation.md` for quality metrics and
validation

### Step 1: Calculate Migration Quality Scorecard (Python Helper)

**Use Python helper to calculate metrics:**

```bash
# Get total Talend jobs from metadata
TOTAL_TALEND_JOBS=$(echo "$TALEND_DATA" | jq -r '.extraction.extraction_metadata.total_jobs')

# Use Python helper to count models and calculate metrics
python -m dbt_generator.generator metrics \
    --project "$OUTPUT_DIR" \
    --talend-jobs "$TOTAL_TALEND_JOBS" \
    --output /tmp/migration_metrics.json

# Load metrics for LLM interpretation
METRICS=$(cat /tmp/migration_metrics.json)

echo "📊 Migration Metrics:"
echo "$METRICS" | jq '{
    total_dbt_models,
    complete_models,
    models_with_placeholders,
    sql_coverage_pct,
    model_reduction_pct,
    consolidation_ratio
}'
```

**LLM interprets metrics against standards:**

📖 **Standards Reference**: Load `@standards/10_quality_validation.md` for quality thresholds

```python
# Parse metrics from Python helper
sql_coverage = METRICS['sql_coverage_pct']
model_reduction = METRICS['model_reduction_pct']
consolidation_ratio = METRICS['consolidation_ratio']

# Validate against standards (LLM checks these thresholds)
if sql_coverage < 98:
    print(f"⚠️  WARNING: SQL coverage {sql_coverage}% below target 98%")

if model_reduction < 70:
    print(f"⚠️  WARNING: Model reduction {model_reduction}% below target 70%")
    print(f"   This may be valid for operational workloads")

if not (0.15 <= consolidation_ratio <= 0.30):
    print(f"⚠️  WARNING: Consolidation ratio {consolidation_ratio} outside target range 0.15-0.30")
```

### Step 2: Run Post-Generation Validation

📖 **Standards Reference**: Reference `@standards/10_quality_validation.md` for post-generation
validation checks

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

______________________________________________________________________

## Phase 8: Final Summary & Next Steps

### Migration Summary

Display comprehensive migration summary:

```bash
# Load metrics from Python helper
METRICS_DATA=$(cat /tmp/migration_metrics.json)

# Extract values
TALEND_JOBS=$(echo "$METRICS_DATA" | jq -r '.total_talend_jobs')
DBT_MODELS=$(echo "$METRICS_DATA" | jq -r '.total_dbt_models')
MODEL_REDUCTION=$(echo "$METRICS_DATA" | jq -r '.model_reduction_pct')
SQL_COVERAGE=$(echo "$METRICS_DATA" | jq -r '.sql_coverage_pct')
COMPLETE_MODELS=$(echo "$METRICS_DATA" | jq -r '.complete_models')
STAGING_COUNT=$(echo "$METRICS_DATA" | jq -r '.models_by_layer.staging')
INTERMEDIATE_COUNT=$(echo "$METRICS_DATA" | jq -r '.models_by_layer.intermediate')
MARTS_COUNT=$(echo "$METRICS_DATA" | jq -r '.models_by_layer.marts')

# Extract source count
SOURCE_COUNT=$(python -m dbt_generator.generator count-sources --project "$OUTPUT_DIR" 2>/dev/null || echo "0")

# Unique domains count
DOMAINS_COUNT=$(echo "$ANALYSIS_RESULTS" | jq -r '.domains | unique | length')

echo ""
echo "========================================="
echo "✅ TALEND TO DBT MIGRATION COMPLETE"
echo "========================================="
echo ""
echo "🔧 Migration Mode: $MIGRATION_MODE"
if [[ "$MIGRATION_MODE" == "merge" ]]; then
    echo "   Merged into: $EXISTING_PROJECT_NAME"
    echo "   Project path: $OUTPUT_DIR"
else
    echo "   New project: $OUTPUT_DIR"
fi
echo ""
echo "📊 Migration Statistics:"
echo "   - Talend Jobs Analyzed: $TALEND_JOBS"
echo "   - DBT Models Generated: $DBT_MODELS"
echo "   - Model Reduction: ${MODEL_REDUCTION}%"
echo "   - SQL Coverage: ${SQL_COVERAGE}%"
echo "   - Source Tables: $SOURCE_COUNT"
echo "   - Domains Identified: $DOMAINS_COUNT"
echo ""
echo "📋 Files Generated:"
echo "   - Staging models: $STAGING_COUNT"
echo "   - Intermediate models: $INTERMEDIATE_COUNT"
if [[ "$MARTS_COUNT" == "0" ]]; then
    echo "   - Marts models: 0 (operational workload)"
    echo ""
    echo "ℹ️  Workload Classification: OPERATIONAL"
    echo "   No marts generated - this is VALID for:"
    echo "   • Reconciliation workflows"
    echo "   • Data quality pipelines"
    echo "   • ETL orchestration"
else
    echo "   - Marts models: $MARTS_COUNT"
fi
echo "   - Source definitions: $SOURCE_COUNT tables"
echo "   - Migration report: docs/migration_report.md"
echo ""
echo "✅ Quality Metrics:"
echo "   - SQL completeness: ${SQL_COVERAGE}%"
echo "   - Complete models: $COMPLETE_MODELS / $DBT_MODELS"
echo "   - Model reduction: ${MODEL_REDUCTION}%"
if [[ "$MIGRATION_MODE" == "merge" ]]; then
    echo "   - Backup created: $BACKUP_PATH"
fi
echo ""
echo "========================================="
echo "📋 NEXT STEPS"
echo "========================================="
echo ""
if [[ "$MIGRATION_MODE" == "merge" ]]; then
    echo "1️⃣  Review merged models in existing project:"
    echo "    ls -la $OUTPUT_DIR/models/staging/\$DOMAIN/"
    echo "    ls -la $OUTPUT_DIR/models/intermediate/\$DOMAIN/"
    echo ""
    echo "2️⃣  Check for conflicts with existing models:"
    echo "    # Look for duplicate model names"
    echo "    find $OUTPUT_DIR/models -name '*.sql' | sort | uniq -d"
    echo ""
    echo "3️⃣  Review merged _sources.yml:"
    echo "    cat $OUTPUT_DIR/models/staging/_sources.yml"
    echo "    # Backup available at: $BACKUP_PATH"
    echo ""
    echo "4️⃣  Validate DBT project (all models):"
    echo "    cd $OUTPUT_DIR"
    echo "    dbt parse --no-partial-parse"
    echo ""
    echo "5️⃣  Run only new models:"
    echo "    dbt run --select \$DOMAIN.*  # Replace \$DOMAIN with actual domain"
    echo ""
    echo "6️⃣  Clear context and run Phase 3:"
    echo "    /clear"
    echo "    /03-post-process $OUTPUT_DIR"
else
    echo "1️⃣  Review the migration report:"
    echo "    cat $OUTPUT_DIR/docs/migration_report.md"
    echo ""
    echo "2️⃣  Address any manual review items (if flagged)"
    echo ""
    echo "3️⃣  Configure profiles.yml with your BigQuery credentials:"
    echo "    export GCP_PROJECT_ID='your-project-id'"
    echo "    export GCP_DATASET='dbt_dev'"
    echo ""
    echo "4️⃣  Clear context (REQUIRED before Phase 3):"
    echo "    /clear"
    echo ""
    echo "5️⃣  Run Phase 3 quality validation:"
    echo "    /03-post-process $OUTPUT_DIR"
    echo ""
    echo "Phase 3 will:"
    echo "  • Format and lint all SQL files with sqlfluff"
    echo "  • Validate all YAML files with yamllint"
    echo "  • Parse and compile the DBT project (dbt parse)"
    echo "  • Format markdown documentation"
    echo "  • Generate comprehensive quality report"
fi
echo ""
echo "========================================="
```

______________________________________________________________________

## Constraint Violation Protocol

📖 **Standards Reference**: Reference `@standards/13_constraints.md` for constraint violation
protocol

**IF any constraint cannot be followed during migration:**

1. ⏸️ **STOP generation** for that specific model (not entire migration)

1. 📝 **Document the conflict** in migration report:

   ```
   ⚠️ CONSTRAINT VIOLATION:
   Model: [model_name]
   Constraint: [which constraint from standards]
   Talend Pattern: [description of what was found in Talend]
   Recommendation: [suggested manual solution]
   Status: SKIPPED - requires manual review
   ```

1. ✅ **Continue with remaining models** (don't let one violation stop entire migration)

1. 🚩 **Include violation summary** in final migration report

**Common constraint violations:**

- Cannot extract domain from job name → Flag for manual domain assignment
- Complex tMap expression has no direct SQL translation → Flag for manual SQL writing
- Circular tRunJob dependency detected → Flag for dependency refactoring
- Context variable not in mapping table → Flag for manual variable handling

______________________________________________________________________

## Success Criteria Checklist

📖 **Standards Reference**: Reference `@standards/13_constraints.md` for migration success criteria

**✅ Migration is successful when ALL applicable criteria are met:**

**Core Requirements (ALWAYS APPLY):**

- [ ] SQL Generation Coverage ≥98%
- [ ] Business Logic Preservation = 100% (all tMap expressions translated)
- [ ] All models follow naming standards (stg\_*/int\_*/fct\_*/dim\_* pattern)
- [ ] All tDBInput tables defined in \_sources.yml (100% source coverage)
- [ ] All tRunJob dependencies converted to ref() (100% dependency preservation)
- [ ] No circular dependencies detected (dbt list passes)
- [ ] No placeholder SQL (TODO/PLACEHOLDER/FIXME removed or \<2%)
- [ ] All constraints followed (or violations documented)
- [ ] dbt parse completes successfully (project structure valid)
- [ ] Test coverage ≥90% (tests defined for all critical models)

**Consolidation Requirements (CONDITIONAL):**

- [ ] Model Reduction = 70-85% (0.15 ≤ score ≤ 0.30)
  - **EXCEPTION**: Operational workloads with no marts may have lower reduction (\<70%)
  - **Valid if**: Jobs are legitimately independent (transactions, error handlers, separate domains)

**Marts Requirements (CONDITIONAL):**

- [ ] Marts generated IF analytical workload detected
  - **Marts OPTIONAL for**: Reconciliation, data quality, ETL orchestration workloads
  - **Marts REQUIRED for**: Star schema, dimensional modeling, analytics use cases
  - **Documentation REQUIRED**: If marts skipped, document workload type in migration report

**If any CORE criterion is NOT met:**

- Document the gap in migration report
- Flag for manual review
- Provide recommendation for resolution

**If CONDITIONAL criterion is not met:**

- Validate the exception applies
- Document justification in migration report
- Continue migration (not blocking)

______________________________________________________________________

## Execution Guarantee

**The migration ALWAYS follows this order:**

1. **Reference Standards** - Load relevant standards files from `/standards/` directory as needed
   for each phase
1. **Verify Inputs** - Validate all 5 input files present and complete
1. **Analyze** - Classify jobs (orchestrators/processors), extract domains, detect layers
1. **Validate** - Run pre-generation validation checks (warning if no marts candidates)
1. **Generate Structure** - Create DBT project folders and config files
1. **Generate Staging** - Create sources.yml + staging models (Bronze layer)
1. **Generate Intermediate** - Apply consolidation + create transformation models (Silver layer)
1. **Generate Marts** - Attempt structural inference if no keywords found; skip if operational
   workload
1. **Generate Support** - Create macros, tests, documentation
1. **Validate** - Run post-generation quality checks
1. **Report** - Generate migration report with workload classification and metrics
1. **Summary** - Display completion summary with next steps

**At each step, reference standards for HOW to implement the step.**

**Key Change from Previous Version:**

- Phase 5 (Marts) is now CONDITIONAL based on workload type
- Operational workloads (reconciliation, DQ, ETL) legitimately have no marts
- Structural inference attempts to identify dimensional patterns even without keywords
- Empty marts folder is VALID and documented, not an error

______________________________________________________________________

## End of Migration Command

Generate production-ready DBT code that can be deployed immediately to BigQuery with minimal manual
intervention.
