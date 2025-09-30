# Talend-to-DBT Migration Standards

**Version:** 3.0.0
**Target Platform:** BigQuery 2025
**Framework:** DBT 1.10+

---

## Overview

This directory contains modular standards files for the Talend-to-DBT migration process. Each file covers a specific topic and is referenced by the `/02-migrate-v2` command at the appropriate phase.

## Standards Files

### Core Architecture & Design

1. **`01_architecture.md`** - Medallion Architecture (Bronze/Silver/Gold)
   - Three-layer structure and responsibilities
   - Layer-specific rules and constraints
   - Reference during: Project setup, layer generation

2. **`02_naming_conventions.md`** - File and Model Naming Standards
   - Naming pattern format and rules
   - Domain extraction from Talend jobs
   - Layer detection patterns
   - Reference during: Job analysis, model generation

3. **`03_model_consolidation.md`** - Model Consolidation Strategy
   - Decision tree for consolidation
   - Rules for what to consolidate vs preserve
   - Consolidation validation and scoring
   - Reference during: Job classification, intermediate layer generation

### SQL Generation

4. **`04_sql_generation.md`** - SQL Generation Standards
   - SQL completeness requirements (98%+)
   - BigQuery 2025 syntax standards
   - CTE structure and naming conventions
   - Reference during: All model generation phases

5. **`05_component_mapping.md`** - Talend Component Translation
   - Component-to-SQL mapping table
   - Expression translation (string, date, numeric, conditional)
   - Reference during: Transformation logic generation

6. **`06_context_variables.md`** - Context Variable Mapping
   - Talend context → DBT variable translations
   - Variable classification (schema, date, environment, batch)
   - Reference during: Context variable translation

### Project Structure

7. **`07_file_organization.md`** - File Organization & Structure
   - Directory layout
   - _sources.yml format
   - _schema.yml format
   - Reference during: Project setup, documentation generation

8. **`08_materialization.md`** - Materialization Strategy
   - Materialization by layer (view/table/incremental)
   - Configuration examples
   - Reference during: Model configuration

9. **`09_dependencies.md`** - Dependency Management
   - ref() vs source() usage patterns
   - Talend tRunJob → DBT ref() mapping
   - Forbidden dependency patterns
   - Reference during: Model generation, dependency resolution

### Quality & Validation

10. **`10_quality_validation.md`** - Quality Standards & Validation
    - Migration quality scorecard metrics
    - Pre-generation and post-generation validation checks
    - Reference during: Pre-migration validation, post-migration quality checks

11. **`11_error_handling.md`** - Error Handling & Audit Patterns
    - Reject flow patterns (Talend → DBT)
    - Audit model generation
    - Reference during: Error flow handling

12. **`12_performance.md`** - Performance Optimization
    - Partitioning guidelines
    - Clustering guidelines
    - Reference during: Fact table generation, large model optimization

13. **`13_constraints.md`** - Absolute Constraints (NEVER VIOLATE)
    - SQL generation constraints
    - Naming constraints
    - Consolidation constraints
    - Dependency constraints
    - Constraint violation protocol
    - Migration success criteria
    - Reference during: All phases, final validation

## Usage in Migration

The `/02-migrate-v2` command references these files contextually:

```
Phase 1: Analysis
  → Load: 03_model_consolidation.md, 02_naming_conventions.md, 10_quality_validation.md

Phase 2: Project Generation
  → Load: 07_file_organization.md

Phase 3: Staging Layer
  → Load: 01_architecture.md, 04_sql_generation.md, 07_file_organization.md

Phase 4: Intermediate Layer
  → Load: 01_architecture.md, 03_model_consolidation.md, 04_sql_generation.md,
          05_component_mapping.md, 06_context_variables.md

Phase 5: Marts Layer
  → Load: 01_architecture.md, 08_materialization.md, 12_performance.md

Phase 7: Quality Validation
  → Load: 10_quality_validation.md, 13_constraints.md
```

## Benefits of Modular Structure

1. **Reduced Context Size**: Each file is 30-200 lines vs 1,238 lines for the monolithic standard
2. **Targeted Loading**: Only load relevant standards for current phase
3. **Easier Maintenance**: Update specific topics without affecting others
4. **Better Organization**: Clear separation of concerns
5. **Improved LLM Performance**: Smaller context windows = better accuracy

## Migration from Original standards.md

The original `talend_parser/standards.md` has been split into 13 focused files. All content has been preserved and reorganized for optimal LLM context management.

**Previous Reference Pattern:**
```
@talend_parser/standards.md Section "Architecture > STAGING (Bronze)"
```

**New Reference Pattern:**
```
@standards/01_architecture.md
```

---

**Last Updated:** 2025-01-29