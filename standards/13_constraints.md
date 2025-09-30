# Absolute Constraints (NEVER VIOLATE)

**Version:** 3.0.0

---

## ⛔ SQL Generation

❌ **NEVER generate placeholder SQL**
```sql
-- FORBIDDEN:
-- TODO: Add logic here
SELECT 1 as placeholder
```

✅ **ALWAYS generate complete SQL**

---

## ⛔ Naming

❌ **NEVER use:**
- `staging_*` (use `stg_`)
- `intermediate_*` (use `int_`)
- `fact_*` (use `fct_`)
- Single underscore between domain and entity: `stg_customer_profile.sql`
- Uppercase: `STG_CUSTOMER__PROFILE.SQL`

✅ **ALWAYS use:**
- `stg_[domain]__[source].sql`
- `int_[domain]__[transformation].sql`
- `fct_[entity].sql`
- `dim_[entity].sql`

---

## ⛔ Consolidation

❌ **NEVER consolidate:**
- Orchestrator jobs (has_child_jobs = true)
- Jobs in different transaction groups
- Jobs with different materialization needs
- Cross-domain jobs

✅ **ALWAYS preserve boundaries:**
- Transaction atomicity
- Error/audit separation
- Reusable transformations

---

## ⛔ Dependencies

❌ **NEVER create:**
- Circular dependencies (A → B → A)
- Marts referencing other marts
- Intermediate models using source() directly

✅ **ALWAYS maintain DAG:**
- Staging → source()
- Intermediate → ref(staging)
- Marts → ref(intermediate)

---

## ⛔ Sources

❌ **NEVER reference undefined sources:**
```sql
FROM {{ source('raw', 'undefined_table') }}
```

✅ **ALWAYS define in _sources.yml first**

---

## Constraint Violation Protocol

**IF a constraint cannot be followed:**

1. ⏸️ **STOP generation** for that model
2. 📝 **Document the conflict:**
   ```
   ⚠️ CONSTRAINT VIOLATION:
   Model: [model_name]
   Constraint: [which constraint]
   Talend Pattern: [description]
   Recommendation: [suggested solution]
   Status: SKIPPED - requires manual review
   ```
3. ✅ **Continue with remaining models**
4. 🚩 **Include violation summary in migration report**

---

## Migration Success Criteria

**✅ Migration is successful when:**

- [ ] SQL Generation Coverage ≥98%
- [ ] Business Logic Preservation = 100%
- [ ] Model Reduction = 70-85%
- [ ] All models follow naming standards
- [ ] All tDBInput tables in _sources.yml
- [ ] All tRunJob dependencies → ref()
- [ ] No circular dependencies
- [ ] No placeholder SQL
- [ ] All constraints followed
- [ ] Consolidation score in target range
- [ ] dbt parse completes successfully
- [ ] Test coverage ≥90%