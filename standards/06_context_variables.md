# Context Variable Mapping

**Version:** 3.0.0

---

## Talend Context → DBT Variable Translation

| Talend Context | DBT Equivalent | Usage | Validation |
|---------------|----------------|-------|------------|
| `context.schema` | `{{ target.dataset }}` | Schema/dataset reference | Must be valid BigQuery dataset |
| `context.from_date` | `{{ var('from_date') }}` | Date parameters | Format: YYYY-MM-DD |
| `context.to_date` | `{{ var('to_date') }}` | Date parameters | Format: YYYY-MM-DD |
| `context.batch_size` | `{{ var('batch_size', 1000) }}` | Processing limits | Must be integer |
| `context.env` | `{{ target.name }}` | Environment (dev/prod) | dev\|test\|prod |
| `context.project_id` | `{{ target.project }}` | GCP project ID | Valid GCP project |
| `globalMap.get("var")` | `{{ var('var') }}` or CTE column | Runtime variables | Depends on scope |

## Context Variable Classification

### Schema/Database Variables

```sql
-- Talend: "+context.schema+".table_name
-- DBT:
FROM {{ target.dataset }}.table_name
-- OR for source:
FROM {{ source('raw', 'table_name') }}  -- Preferred
```

### Date Filter Variables

```sql
-- Talend: WHERE date >= context.from_date
-- DBT:
WHERE date >= '{{ var("from_date") }}'
-- With default:
WHERE date >= '{{ var("from_date", run_started_at.strftime("%Y-%m-%d")) }}'
```

### Environment Variables

```sql
-- Talend: IF context.env == "PROD" THEN ... ELSE ...
-- DBT:
{% if target.name == 'prod' %}
    -- Production logic
{% else %}
    -- Non-production logic
{% endif %}
```

### Batch/Iteration Variables

```sql
-- Talend: ((Integer)globalMap.get("tAggregateRow_1_NB_LINE"))
-- DBT: Use CTE with COUNT() instead
WITH aggregated AS (
    SELECT
        ...,
        COUNT(*) OVER() as total_rows
    FROM source
)
```