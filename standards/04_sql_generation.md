# SQL Generation Standards

**Version:** 3.0.0
**Target Platform:** BigQuery 2025

---

## SQL Completeness Target: 98%+

**✅ REQUIRED - Complete SQL:**
```sql
-- Every model must have 100% functional SQL
{{
    config(
        materialized='table',
        partition_by={'field': 'event_date', 'data_type': 'date'}
    )
}}

WITH source_data AS (
    SELECT
        customer_id,
        order_id,
        order_date,
        amount
    FROM {{ source('raw', 'orders') }}
    WHERE order_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 90 DAY)
),

enriched AS (
    SELECT
        s.customer_id,
        s.order_id,
        s.order_date,
        s.amount,
        UPPER(TRIM(c.customer_name)) AS customer_name,
        CASE
            WHEN s.amount >= 1000 THEN 'HIGH_VALUE'
            WHEN s.amount >= 100 THEN 'MEDIUM_VALUE'
            ELSE 'LOW_VALUE'
        END AS order_tier
    FROM source_data s
    LEFT JOIN {{ ref('dim_customer') }} c
        ON s.customer_id = c.customer_id
)

SELECT * FROM enriched
```

**❌ FORBIDDEN - Placeholder SQL:**
```sql
-- NEVER generate incomplete SQL like this:
WITH source_data AS (
    -- TODO: Add source query here
    SELECT * FROM table_name  -- ❌ Which table?
),

transformed AS (
    -- TODO: Add transformation logic  -- ❌ Placeholder
    SELECT 1 as dummy  -- ❌ Not real logic
)

SELECT * FROM transformed
```

## BigQuery 2025 SQL Standards

**✅ Use Modern BigQuery Syntax:**

```sql
-- ✅ Qualified table names
`{{ target.project }}.{{ target.dataset }}.table_name`

-- ✅ Safe casting (prevents errors)
SAFE_CAST(column AS INT64)
SAFE_CAST(column AS NUMERIC)
SAFE_CAST(column AS FLOAT64)

-- ✅ Safe parsing (prevents errors)
SAFE.PARSE_DATE('%Y-%m-%d', date_string)
SAFE.PARSE_TIMESTAMP('%Y-%m-%d %H:%M:%S', timestamp_string)
SAFE.PARSE_DATETIME('%Y-%m-%d %H:%M:%S', datetime_string)

-- ✅ Null handling
COALESCE(column, default_value)
IFNULL(column, default_value)
NULLIF(column1, column2)

-- ✅ String functions
CONCAT(str1, str2, str3)
UPPER(column)
LOWER(column)
TRIM(column)
SUBSTR(column, start_pos, length)
REGEXP_REPLACE(column, pattern, replacement)

-- ✅ Date functions
CURRENT_DATE()
CURRENT_TIMESTAMP()
DATE_ADD(date_column, INTERVAL 1 DAY)
DATE_SUB(date_column, INTERVAL 7 DAY)
DATE_DIFF(date1, date2, DAY)
EXTRACT(YEAR FROM date_column)
FORMAT_DATE('%Y-%m-%d', date_column)

-- ✅ Window functions
ROW_NUMBER() OVER(PARTITION BY col1 ORDER BY col2)
RANK() OVER(PARTITION BY col1 ORDER BY col2)
LAG(column, 1) OVER(PARTITION BY col1 ORDER BY col2)
LEAD(column, 1) OVER(PARTITION BY col1 ORDER BY col2)
```

**❌ DO NOT Use Deprecated Syntax:**

```sql
-- ❌ Old BigQuery table syntax
[project:dataset.table]  -- Use backticks instead

-- ❌ Unsafe casting (can fail)
CAST(column AS INT)  -- Use SAFE_CAST

-- ❌ Ambiguous date parsing
PARSE_DATE(date_string)  -- Specify format

-- ❌ Non-portable functions
CONCAT_WS(sep, col1, col2)  -- Not standard BigQuery
```

## CTE (Common Table Expression) Standards

**✅ Proper CTE Structure:**

```sql
-- Each CTE should:
-- 1. Have descriptive name matching purpose
-- 2. Include comment explaining Talend source
-- 3. Be indented consistently (4 spaces)
-- 4. Have clear logical separation

WITH source_customers AS (
    -- Talend: tDBInput_1 (DCS_CUST_ACCT_DIM)
    SELECT
        customer_id,
        first_name,
        last_name,
        email
    FROM {{ source('raw', 'dcs_cust_acct_dim') }}
),

cleansed_customers AS (
    -- Talend: tMap_1 (Customer cleansing transformations)
    SELECT
        customer_id,
        UPPER(TRIM(first_name)) AS first_name,
        UPPER(TRIM(last_name)) AS last_name,
        LOWER(TRIM(email)) AS email
    FROM source_customers
    WHERE customer_id IS NOT NULL
),

segment_lookup AS (
    -- Talend: tDBInput_2 (CUST_SEGMENT_LKP lookup table)
    SELECT
        customer_id,
        segment_code,
        segment_name
    FROM {{ source('raw', 'cust_segment_lkp') }}
),

enriched AS (
    -- Talend: tMap_2 (Join customer with segment)
    SELECT
        c.customer_id,
        c.first_name,
        c.last_name,
        c.email,
        COALESCE(s.segment_code, 'UNKNOWN') AS segment_code,
        COALESCE(s.segment_name, 'Unclassified') AS segment_name
    FROM cleansed_customers c
    LEFT JOIN segment_lookup s
        ON c.customer_id = s.customer_id
)

-- Final select (always end with this pattern)
SELECT * FROM enriched
```

**CTE Naming Conventions:**
- `source_[entity]` - For source() or staging refs
- `[entity]_lookup` - For dimension lookups
- `cleansed` / `validated` / `filtered` - For quality steps
- `enriched` / `joined` / `aggregated` - For transformations
- `final` - For final output CTE (if needed)