# Performance Optimization

**Version:** 3.0.0

---

## Partitioning Guidelines

**✅ Always partition these:**
- Fact tables (by date dimension: order_date, event_date, created_date)
- Large intermediate tables (>10M rows)
- Any table with time-based queries

**Example:**
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
```

## Clustering Guidelines

**✅ Cluster on:**
- Primary key columns
- Foreign key columns (join keys)
- Common filter columns (WHERE clauses)
- Max 4 columns per table

**Example:**
```sql
{{
    config(
        materialized='table',
        cluster_by=['customer_id', 'order_date', 'status']
    )
}}
```