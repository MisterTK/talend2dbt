# Materialization Strategy

**Version:** 3.0.0

---

## Materialization by Layer

| Layer | Default Strategy | When to Override | Configuration |
|-------|-----------------|------------------|---------------|
| Staging | `view` | If source >1M rows → `table` | Ephemeral for speed |
| Intermediate | `table` | If >10M rows → `incremental` | Persistent for reuse |
| Marts (Facts) | `incremental` | Small dims (<100K) → `table` | Partitioned/clustered |
| Marts (Dims) | `table` | Large dims (>1M) → `incremental` | SCD Type 2 if needed |

## Configuration Examples

**Staging (view):**
```sql
{{
    config(
        materialized='view'
    )
}}
```

**Intermediate (table):**
```sql
{{
    config(
        materialized='table'
    )
}}
```

**Marts - Incremental Fact (partitioned):**
```sql
{{
    config(
        materialized='incremental',
        partition_by={
            'field': 'order_date',
            'data_type': 'date',
            'granularity': 'day'
        },
        cluster_by=['customer_id', 'product_id'],
        unique_key='order_id'
    )
}}

SELECT *
FROM {{ ref('int_sales__orders_enriched') }}
{% if is_incremental() %}
    WHERE order_date > (SELECT MAX(order_date) FROM {{ this }})
{% endif %}
```

**Marts - Dimension (table with clustering):**
```sql
{{
    config(
        materialized='table',
        cluster_by=['customer_id']
    )
}}
```