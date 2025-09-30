# Error Handling & Audit Patterns

**Version:** 3.0.0

---

## Reject Flow Pattern (Talend → DBT)

**Talend Pattern:**
```
tMap_1 → tFilterRow_1
         ├─MAIN→ tDBOutput_1 (valid records)
         └─REJECT→ tLogRow_1 → tDBOutput_2 (audit table)
```

**DBT Pattern:**
```sql
-- Main model: int_domain__entity.sql
WITH validated AS (
    SELECT
        *,
        CASE
            WHEN email NOT LIKE '%@%' THEN 'INVALID_EMAIL'
            WHEN amount < 0 THEN 'NEGATIVE_AMOUNT'
            WHEN customer_id IS NULL THEN 'MISSING_CUSTOMER'
            ELSE 'VALID'
        END AS validation_status
    FROM {{ ref('stg_domain__entity') }}
)

SELECT *
FROM validated
WHERE validation_status = 'VALID'

-- Separate audit model: int_domain__entity_rejects.sql
{{
    config(
        materialized='incremental',
        schema='audit',
        unique_key='audit_id'
    )
}}

WITH validated AS (
    SELECT
        *,
        CASE
            WHEN email NOT LIKE '%@%' THEN 'INVALID_EMAIL'
            WHEN amount < 0 THEN 'NEGATIVE_AMOUNT'
            WHEN customer_id IS NULL THEN 'MISSING_CUSTOMER'
            ELSE 'VALID'
        END AS validation_status
    FROM {{ ref('stg_domain__entity') }}
),

rejected_records AS (
    SELECT
        GENERATE_UUID() as audit_id,
        CURRENT_TIMESTAMP() as rejected_at,
        'data_quality' as rejection_type,
        validation_status as rejection_reason,
        TO_JSON_STRING(STRUCT(*)) as record_data
    FROM validated
    WHERE validation_status != 'VALID'
)

SELECT * FROM rejected_records
```