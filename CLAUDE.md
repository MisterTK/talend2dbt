# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this
repository.

## Project Overview

This is a **Talend-to-DBT migration tool** that automates the conversion of Talend ETL jobs into
production-ready DBT models for BigQuery. The migration follows a 3-phase pipeline with
LLM-optimized parsing to achieve 98%+ SQL generation coverage.

**Target Platform:** BigQuery 2025 (latest features) **DBT Version:** 1.10+ **Python Version:**
3.11+

## Common Commands

### Initial Setup (Run Once)

```bash
# Bootstrap the Python environment and install all dependencies
/00-bootstrap

# After bootstrap completes, clear context:
/clear
```

### Migration Pipeline (Run in Sequence)

**Phase 1: Pre-process Talend Jobs**

```bash
/01-pre-process <path/to/talend/jobs> [output_path]

# Example:
/01-pre-process ~/talend_workspace/jobs
```

**Phase 2: Generate DBT Project**

```bash
# After Phase 1 completes, ALWAYS clear context first:
/clear

# Create new DBT project:
/02-migrate <processed_talend_path> [dbt_output_path]

# OR merge into existing DBT project:
/02-migrate <processed_talend_path> --merge-into <existing_dbt_project_path>

# Examples:
/02-migrate ~/talend_workspace/talend_processed ~/talend_workspace/dbt_migrated
/02-migrate ~/talend_workspace/talend_processed --merge-into ~/existing_dbt
```

**Phase 3: Quality Validation**

```bash
# After Phase 2 completes, ALWAYS clear context first:
/clear

/03-post-process <dbt_project_path>

# Example:
/03-post-process ~/talend_workspace/dbt_migrated
```

### Testing DBT Output

```bash
cd <dbt_project_path>

# Parse and validate project structure
export GCP_PROJECT_ID="your-project-id"
dbt parse --no-partial-parse

# Run all models
dbt run

# Run tests
dbt test

# Build everything
dbt build
```

### Code Quality

```bash
# SQL linting with sqlfluff (BigQuery dialect)
sqlfluff lint models/ --dialect bigquery

# Auto-fix linting issues
sqlfluff fix models/ --dialect bigquery --force

# YAML validation
yamllint models/ dbt_project.yml profiles.yml
```

## High-Level Architecture

### Three-Phase Pipeline

**Phase 1: Pre-Processing (`/01-pre-process`)**

- **Input:** Raw Talend .item XML files
- **Output:** LLM-optimized structured JSON, SQL, and YAML files
- **Purpose:** Extract and optimize Talend job metadata for token efficiency
- **Key Script:** `talend_parser/talend_parser.py`
- **Output Files:**
  - `talend_extraction.json` - Complete job structure and metadata
  - `sql_queries.sql` - Extracted SQL with annotations
  - `transformations.json` - tMap business logic
  - `context_to_dbt.yml` - Context variable mappings
  - `extraction_summary.txt` - Human-readable summary
  - `token_statistics.txt` - Token reduction analysis

**Phase 2: Migration (`/02-migrate`)**

- **Input:** Pre-processed Talend output from Phase 1
- **Output:** Complete DBT project with medallion architecture
- **Purpose:** Generate production-ready DBT models with 98%+ SQL coverage
- **Architecture:** Medallion (Bronze → Silver → Gold)
  - **Staging (Bronze):** 1:1 source table mappings (`stg_*`)
  - **Intermediate (Silver):** Business logic and transformations (`int_*`)
  - **Marts (Gold):** Dimensional models (`fct_*`, `dim_*`)
- **Key Features:**
  - Intelligent model consolidation (70-85% reduction in model count)
  - Support for merging into existing DBT projects (`--merge-into` flag)
- **Python Helper:** `dbt_generator/generator.py` handles file operations, LLM handles SQL
  generation

**Phase 3: Post-Processing (`/03-post-process`)**

- **Input:** Generated DBT project from Phase 2
- **Output:** Formatted, linted, and validated DBT project
- **Purpose:** Ensure production code quality
- **Actions:**
  - SQL formatting and linting (sqlfluff)
  - YAML validation (yamllint)
  - DBT project parsing and compilation
  - Markdown documentation formatting

### Migration Standards (`/standards/`)

All migration logic must comply with 13 modular standards files:

| File                        | Purpose                               | Used In                       |
| --------------------------- | ------------------------------------- | ----------------------------- |
| `01_architecture.md`        | Medallion layer definitions           | All phases                    |
| `02_naming_conventions.md`  | Domain extraction and naming patterns | Phase 2 job analysis          |
| `03_model_consolidation.md` | Consolidation decision tree           | Phase 2 intermediate layer    |
| `04_sql_generation.md`      | SQL completeness and CTE standards    | Phase 2 all layers            |
| `05_component_mapping.md`   | Talend component → SQL translation    | Phase 2 transformations       |
| `06_context_variables.md`   | Context variable → DBT var mapping    | Phase 2 variable translation  |
| `07_file_organization.md`   | Directory structure and schema files  | Phase 2 project setup         |
| `08_materialization.md`     | View/table/incremental strategy       | Phase 2 model configuration   |
| `09_dependencies.md`        | ref() and source() patterns           | Phase 2 dependency resolution |
| `10_quality_validation.md`  | Validation metrics and checks         | Phase 2 quality validation    |
| `11_error_handling.md`      | Error flow and audit patterns         | Phase 2 reject flows          |
| `12_performance.md`         | Partitioning and clustering           | Phase 2 large models          |
| `13_constraints.md`         | Absolute migration constraints        | All phases                    |

**Critical Rule:** When working on Phase 2 migration, **ALWAYS reference the appropriate standards
file** for each phase. The standards enforce 98%+ SQL generation and proper consolidation.

### Python Packages

#### Talend Parser (`talend_parser/talend_parser.py`)

Phase 1 extraction - comprehensive Python script (2,000+ lines) that:

- Parses Talend .item XML files using ElementTree
- Extracts SQL queries from tDBInput/tDBOutput components
- Parses tMap transformations and expressions
- Detects job hierarchy (orchestrators vs processors)
- Identifies transaction boundaries (tDBConnection/Commit/Rollback)
- Analyzes error handling patterns (tDie, tWarn, reject flows)
- Classifies context variables by type
- Calculates token statistics using tiktoken
- Outputs LLM-optimized JSON structures

**Key Classes:**

- `ExtractedSQL` - Clean SQL extraction with metadata
- `TMapExpression` - tMap transformation logic
- `TalendParser` - Main parser orchestrating all extraction

**Usage:**

```bash
python -m talend_parser.talend_parser <input_dir> <output_dir>
```

#### DBT Generator (`dbt_generator/generator.py`)

Phase 2 infrastructure - minimal Python helper (~500 lines) that handles:

- **File Operations:** Create/enhance directory structure, backup files
- **Data Loading:** Load JSON/YAML/SQL files (no interpretation)
- **Validation:** Check file existence, validate project structure
- **Metrics Calculation:** Count models, calculate coverage (pure math)

**Philosophy:** Python handles infrastructure, LLM interprets standards and generates SQL.

**Key Methods:**

- `load_talend_data()` - Load all pre-processed files
- `setup_project_structure(domains)` - Create/merge directory structure
- `validate_inputs()` - Check required files exist
- `validate_existing_project()` - Validate existing DBT project (for merge mode)
- `backup_sources_yml()` - Backup before merge
- `calculate_migration_metrics()` - Calculate quality scorecard
- `count_models_and_placeholders()` - Count complete vs incomplete models

**Usage:**

```bash
# Load Talend data
python -m dbt_generator.generator load --input <dir> --output talend_data.json

# Setup project structure
python -m dbt_generator.generator setup --input <dir> --output <dir> --mode new --domains sales finance

# Validate inputs
python -m dbt_generator.generator validate --input <dir> --mode inputs

# Calculate metrics
python -m dbt_generator.generator metrics --project <dir> --talend-jobs 100 --output metrics.json
```

### Custom Slash Commands

The repository includes 4 custom slash commands defined in `.claude/commands/`:

- `/00-bootstrap` - Python environment setup
- `/01-pre-process` - Talend extraction and optimization
- `/02-migrate` - DBT project generation
- `/03-post-process` - Quality validation and formatting

Each command has a markdown file with:

- `description` - One-line summary
- `argument-hint` - Usage pattern
- `allowed-tools` - Tool restrictions
- Detailed execution steps with bash/Python code blocks

## Key Conventions and Patterns

### DBT Naming Standards

**Staging Models:** `stg_[domain]__[source].sql`

- Example: `stg_customer360__orders.sql`
- Materialized as views
- 1:1 mapping to source tables

**Intermediate Models:** `int_[domain]__[transformation].sql`

- Example: `int_sales__daily_aggregates.sql`
- Materialized as tables or incremental
- Contains business logic from tMap/tFilter/tAggregate

**Marts Models:** `fct_[entity].sql` or `dim_[entity].sql`

- Example: `fct_orders.sql`, `dim_customer.sql`
- Materialized as partitioned/clustered tables
- No domain prefix for marts

### Consolidation Decision Tree

**DO NOT CONSOLIDATE:**

- Orchestrator jobs (has tRunJob components)
- Transaction boundaries (tDBConnection/Commit/Rollback)
- Error/reject flows (separate audit models)
- Reusable transformations (>3 dependencies)
- Cross-domain jobs

**SAFE TO CONSOLIDATE:**

- Sequential single-path transformations
- Temporary tables with single use
- Multiple aggregations on same grain
- Small lookup tables (\<10K rows, static)
- Simple child jobs (\<5 components, no dependencies)

### Talend Component Mappings

| Talend Component | DBT/SQL Pattern                              |
| ---------------- | -------------------------------------------- |
| tDBInput         | `SELECT * FROM {{ source('raw', 'table') }}` |
| tDBOutput        | Final SELECT in model                        |
| tMap             | CTEs with CASE/CONCAT/UPPER expressions      |
| tFilterRow       | WHERE clause                                 |
| tAggregateRow    | GROUP BY ... HAVING                          |
| tJoin            | LEFT/INNER JOIN                              |
| tSortRow         | ORDER BY                                     |
| tUniqRow         | DISTINCT or ROW_NUMBER()                     |
| tRunJob          | `{{ ref('child_model') }}`                   |

### Context Variable Mappings

| Talend Context         | DBT Equivalent                   |
| ---------------------- | -------------------------------- |
| `context.schema`       | `{{ target.dataset }}`           |
| `context.project_id`   | `{{ target.project }}`           |
| `context.env`          | `{{ target.name }}`              |
| `context.from_date`    | `{{ var('from_date') }}`         |
| `globalMap.get("var")` | `{{ var('var') }}` or CTE column |

## Migration Quality Targets

The migration enforces strict quality metrics:

| Metric                      | Target | Validation                           |
| --------------------------- | ------ | ------------------------------------ |
| SQL Generation Coverage     | ≥98%   | No TODO/PLACEHOLDER comments         |
| Business Logic Preservation | 100%   | All tMap expressions translated      |
| Model Reduction             | 70-85% | Intelligent consolidation            |
| Manual Intervention         | \<2%   | Minimal post-migration fixes         |
| Source Coverage             | 100%   | All tDBInput tables in \_sources.yml |
| Dependency Preservation     | 100%   | All tRunJob → ref() conversions      |
| Naming Compliance           | 100%   | All files follow standards           |

## Important Constraints

**NEVER VIOLATE THESE:**

1. **NO placeholder SQL** - Generate complete SQL or document as constraint violation
1. **NO naming violations** - All files must match `stg_*`, `int_*`, `fct_*`, `dim_*` patterns
1. **NO orchestrator consolidation** - Jobs with tRunJob must remain separate (documented as
   orchestration logic)
1. **NO circular dependencies** - Validate with `dbt list` before completion
1. **NO undefined sources** - All tables must be defined in `_sources.yml`

**If a constraint cannot be followed:**

1. STOP generation for that specific model
1. Document the violation in migration report
1. Continue with remaining models
1. Flag for manual review

## Development Workflow

### Working on Talend Parser

```bash
# Activate virtual environment
source venv/bin/activate

# Test parser on sample Talend jobs
python -m talend_parser.talend_parser ~/sample_jobs ~/test_output

# Verify output files
ls -lh ~/test_output/
cat ~/test_output/extraction_summary.txt
```

### Working on DBT Generator

```bash
# Test data loading
python -m dbt_generator.generator load --input ~/test_output --output /tmp/talend_data.json

# Test project setup (new mode)
python -m dbt_generator.generator setup \
    --input ~/test_output \
    --output ~/test_dbt \
    --mode new \
    --domains sales finance

# Test project setup (merge mode)
python -m dbt_generator.generator setup \
    --input ~/test_output \
    --output ~/existing_dbt \
    --mode merge \
    --domains customer360

# Validate inputs
python -m dbt_generator.generator validate --input ~/test_output --mode inputs

# Calculate metrics
python -m dbt_generator.generator metrics --project ~/test_dbt --talend-jobs 50
```

### Testing Migration Output

```bash
# Generate test DBT project (new)
/02-migrate ~/test_output ~/test_dbt

# OR merge into existing project
/02-migrate ~/test_output --merge-into ~/existing_dbt

# Validate structure
cd ~/test_dbt
dbt parse --no-partial-parse

# Check for naming compliance
find models -name "*.sql" | grep -v -E "(stg_|int_|fct_|dim_)"

# Check for placeholders
grep -r "TODO\|PLACEHOLDER\|FIXME" models/

# Count models using Python helper
python -m dbt_generator.generator metrics --project . --output metrics.json
cat metrics.json
```

### Modifying Standards

When updating standards files in `/standards/`:

1. Keep each file focused on a single topic
1. Maintain version numbers and update dates
1. Update `standards/README.md` if adding new files
1. Test impact on migration output

## Context Management

**CRITICAL:** Due to large file sizes in Talend XML and generated DBT projects:

- **ALWAYS run `/clear`** between migration phases
- Phase 1 loads large XML files into context
- Phase 2 generates many DBT models
- Phase 3 validates all generated code
- Clearing context prevents token overflow and ensures clean execution

## Virtual Environment

The project uses a Python virtual environment in `venv/`:

- **Python 3.11+** required (auto-installed by bootstrap)
- **Key dependencies:** pyyaml, tiktoken, dbt-core, dbt-bigquery, sqlfluff, yamllint, mdformat
- **Bootstrap handles:** venv creation, pip upgrades, dependency installation, verification
- **Manual activation:** `source venv/bin/activate` (only needed for direct Python script execution)

## File Organization

```
talend2dbt/
├── .claude/
│   └── commands/          # Custom slash command definitions
│       ├── 00-bootstrap.md
│       ├── 01-pre-process.md
│       ├── 02-migrate.md
│       └── 03-post-process.md
├── talend_parser/         # Phase 1: Talend extraction (Python package)
│   ├── __init__.py
│   └── talend_parser.py   # Core extraction logic (2000+ lines)
├── dbt_generator/         # Phase 2: DBT generation helpers (Python package)
│   ├── __init__.py
│   └── generator.py       # Infrastructure & utilities (~500 lines)
├── standards/             # Modular migration standards (13 files)
│   ├── 01_architecture.md
│   ├── 02_naming_conventions.md
│   ├── ...
│   └── README.md
├── venv/                  # Python virtual environment (managed by bootstrap)
├── pyproject.toml         # Modern Python project configuration
├── README.md
└── CLAUDE.md              # This file
```

## Tips for Development

- **Standards are source of truth** - All migration decisions reference `/standards/` files (LLM
  interprets them)
- **Python for infrastructure, LLM for intelligence** - Python handles file ops, LLM interprets
  standards and generates SQL
- **Token efficiency matters** - Pre-processing optimizes Talend XML for LLM context
- **Consolidation is critical** - Reducing 100 Talend jobs to 15-30 DBT models is expected
- **SQL completeness is non-negotiable** - 98%+ SQL generation target (no placeholders)
- **BigQuery 2025 syntax** - Use latest features (SAFE_CAST, CURRENT_DATE(), etc.)
- **Always clear context** - Between phases to prevent token overflow
- **Reference standards files** - Load appropriate file for each migration phase (never hardcode in
  Python)
- **Document violations** - If a constraint can't be met, document it and continue
- **Merge mode support** - Can add models to existing DBT projects with `--merge-into` flag
- Do NOT add gemini.md or .gemini to my repo, keep them ignored