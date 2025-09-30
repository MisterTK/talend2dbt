# Quality Standards & Validation

**Version:** 3.0.0

---

## Migration Quality Scorecard

| Metric | Target | Measurement | Action if Missed |
|--------|--------|-------------|------------------|
| SQL Generation Coverage | ≥98% | `(complete_sql_models / total_models) * 100` | FLAG for review |
| Business Logic Preservation | 100% | All tMap expressions translated | STOP migration |
| Model Reduction | 70-85% | `1 - (dbt_models / talend_jobs)` | Review consolidation |
| Manual Intervention | <2% | Lines needing human completion | Acceptable |
| Test Coverage | ≥90% | Models with tests / total models | Add tests |
| Dependency Preservation | 100% | All tRunJob → ref() | STOP migration |
| Source Coverage | 100% | All tDBInput tables in _sources.yml | Add missing |
| Naming Compliance | 100% | Files matching [prefix]_[domain]__[entity].sql | Fix names |

## Pre-Generation Validation Checks

**✅ MUST PASS before generating code:**

```python
# Check 1: Input file completeness
assert exists("talend_extraction.json")
assert exists("sql_queries.sql")
assert exists("transformations.json")
assert exists("context_to_dbt.yml")

# Check 2: Extraction quality
metadata = load("talend_extraction.json")["extraction_metadata"]
assert metadata["total_jobs"] > 0, "No jobs found"
assert metadata["total_sql_queries"] > 0, "No SQL extracted"
assert metadata["unique_tables"] > 0, "No tables identified"

# Check 3: Domain extraction
domains = extract_domains(jobs_summary)
assert len(domains) > 0, "No domains identified for naming"

# Check 4: Consolidation feasibility
orchestrators = count_orchestrators(job_hierarchy)
processors = count_processors(job_hierarchy)
assert processors > 0, "No processor jobs to consolidate"
```

## Post-Generation Validation

**✅ MUST VERIFY after generating code:**

```bash
# SQL completeness check
grep -r "TODO\|PLACEHOLDER\|FIXME" models/*.sql
# Should return 0 matches (or <2% of files)

# Naming compliance check
find models -name "*.sql" | grep -v -E "^(stg|int|fct|dim)_.*\.sql$"
# Should return 0 matches

# Circular dependency check
dbt list --select "+model_name" --output json
# Should complete without errors

# Source coverage check
dbt ls --select source:* --output json
# All sources should be found
```