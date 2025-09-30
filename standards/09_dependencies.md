# Dependency Management

**Version:** 3.0.0

---

## ref() vs source() Usage

**✅ CORRECT Dependency Patterns:**

```sql
-- Staging models ONLY use source()
WITH raw_data AS (
    SELECT * FROM {{ source('raw', 'table_name') }}
)

-- Intermediate models use ref() to staging
WITH staging_data AS (
    SELECT * FROM {{ ref('stg_domain__table') }}
)

-- Marts use ref() to intermediate or staging (never other marts)
WITH enriched_data AS (
    SELECT * FROM {{ ref('int_domain__transformation') }}
)

-- Facts reference dimensions via ref()
WITH fact_data AS (
    SELECT * FROM {{ ref('int_domain__facts') }}
),
dimension_lookup AS (
    SELECT * FROM {{ ref('dim_customer') }}
)
SELECT
    f.*,
    d.customer_name
FROM fact_data f
LEFT JOIN dimension_lookup d ON f.customer_id = d.customer_id
```

**❌ FORBIDDEN Patterns:**

```sql
-- ❌ Intermediate model using source() directly (skip staging)
SELECT * FROM {{ source('raw', 'table') }}  -- Should use ref('stg_...')

-- ❌ Circular dependency
-- model_a.sql:
SELECT * FROM {{ ref('model_b') }}
-- model_b.sql:
SELECT * FROM {{ ref('model_a') }}  -- ERROR: circular!

-- ❌ Mart referencing another mart
-- fct_orders.sql:
SELECT * FROM {{ ref('fct_customers') }}  -- Should ref intermediate instead
```

## Talend tRunJob → DBT ref() Mapping

**Talend Pattern:**
```
Master_Load_Customer360
  ├─ tRunJob_1 → Child_Load_Accounts
  ├─ tRunJob_2 → Child_Load_Profiles
  └─ tRunJob_3 → Child_Load_Segments
```

**DBT Pattern:**
```sql
-- No model for Master_Load_Customer360 (pure orchestration)

-- dim_account.sql (from Child_Load_Accounts)
SELECT * FROM {{ ref('stg_customer360__accounts') }}

-- dim_profile.sql (from Child_Load_Profiles)
SELECT * FROM {{ ref('stg_customer360__profiles') }}

-- dim_segment.sql (from Child_Load_Segments)
SELECT * FROM {{ ref('stg_customer360__segments') }}

-- fct_customer_360.sql (orchestration in DBT via refs)
WITH accounts AS (
    SELECT * FROM {{ ref('dim_account') }}
),
profiles AS (
    SELECT * FROM {{ ref('dim_profile') }}
),
segments AS (
    SELECT * FROM {{ ref('dim_segment') }}
)
SELECT
    a.*,
    p.profile_data,
    s.segment_code
FROM accounts a
LEFT JOIN profiles p ON a.customer_id = p.customer_id
LEFT JOIN segments s ON a.customer_id = s.customer_id
```