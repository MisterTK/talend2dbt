# GEMINI.md

This file provides guidance to Gemini when working with code in this repository.

---

## Project Overview

This is a **Talend-to-DBT migration tool** that automates the conversion of Talend ETL jobs into production-ready DBT models for BigQuery. The migration follows a 3-phase pipeline with LLM-optimized parsing to achieve 98%+ SQL generation coverage.

**Target Platform:** BigQuery 2025 (latest features)
**DBT Version:** 1.10+
**Python Version:** 3.11+

---

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

/02-migrate <processed_talend_path> [dbt_output_path]

# Example:
/02-migrate ~/talend_workspace/talend_processed ~/talend_workspace/dbt_migrated
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

---

## High-Level Architecture

### Three-Phase Pipeline

#### Phase 1: Pre-Processing (`/01-pre-process`)

- **Input:** Raw Talend .item XML files
- **Output:** LLM-optimized structured JSON, SQL, and YAML files
- **Purpose:** Extract and optimize Talend job metadata for token efficiency
- **Key Script:** `talend_parser/talend_parser.py`

#### Phase 2: Migration (`/02-migrate`)

- **Input:** Pre-processed Talend output from Phase 1
- **Output:** Complete DBT project with medallion architecture
- **Purpose:** Generate production-ready DBT models with 98%+ SQL coverage
- **Architecture:** Medallion (Bronze → Silver → Gold)

#### Phase 3: Post-Processing (`/03-post-process`)

- **Input:** Generated DBT project from Phase 2
- **Output:** Formatted, linted, and validated DBT project
- **Purpose:** Ensure production code quality

### Migration Standards (`/standards/`)

All migration logic must comply with 13 modular standards files. When working on Phase 2 migration, **ALWAYS reference the appropriate standards file** for each phase.

### Talend Parser (`talend_parser/talend_parser.py`)

The parser is a comprehensive Python script that parses Talend .item XML files and extracts relevant information for the migration.

---

## Development Conventions

### DBT Naming Standards

- **Staging Models:** `stg_[domain]__[source].sql`
- **Intermediate Models:** `int_[domain]__[transformation].sql`
- **Marts Models:** `fct_[entity].sql` or `dim_[entity].sql`

### Consolidation Decision Tree

The project follows a specific decision tree to determine which Talend jobs can be consolidated into a single dbt model.

### Talend Component Mappings

The project has a defined mapping from Talend components to dbt/SQL patterns.

### Context Variable Mappings

The project has a defined mapping from Talend context variables to dbt equivalents.

---

**For complete details, refer to the standards files in `/standards/` directory.**
