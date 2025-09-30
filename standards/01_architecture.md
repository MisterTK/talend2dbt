# Architecture Standards: Medallion (Bronze/Silver/Gold)

**Version:** 3.0.0
**Target Platform:** BigQuery 2025

---

## Three-Layer Structure

```
┌─────────────────────────────────────────────────────────────┐
│ STAGING (Bronze) - Raw Data Ingestion                      │
│ Purpose: 1:1 mapping of source tables, minimal transforms  │
│ Naming: stg_[domain]__[source].sql                         │
│ Materialization: view (ephemeral, unless >1M rows)         │
│ SQL Completeness: 100% (no placeholders)                   │
└─────────────────────────────────────────────────────────────┘
              ↓ ref()
┌─────────────────────────────────────────────────────────────┐
│ INTERMEDIATE (Silver) - Business Logic                     │
│ Purpose: Transformations, joins, filters, enrichment       │
│ Naming: int_[domain]__[transformation].sql                 │
│ Materialization: table (incremental if >10M rows)          │
│ SQL Completeness: 98%+ (preserve all business logic)       │
└─────────────────────────────────────────────────────────────┘
              ↓ ref()
┌─────────────────────────────────────────────────────────────┐
│ MARTS (Gold) - Analytics-Ready                             │
│ Purpose: Dimensional models (facts, dimensions)            │
│ Naming: fct_[entity].sql OR dim_[entity].sql               │
│ Materialization: table (partitioned/clustered)             │
│ SQL Completeness: 100% (production-ready)                  │
└─────────────────────────────────────────────────────────────┘
```

## Layer Responsibilities

**STAGING (Bronze):**
- ✅ Load raw data from sources 1:1
- ✅ Basic type casting (SAFE_CAST)
- ✅ Renaming columns for consistency (snake_case)
- ❌ NO business logic
- ❌ NO joins or aggregations
- ❌ NO filtering (except null key removal)

**INTERMEDIATE (Silver):**
- ✅ tMap transformations (expressions, CASE statements)
- ✅ Joins (INNER, LEFT, lookups)
- ✅ Filters (WHERE clauses)
- ✅ Aggregations (GROUP BY)
- ✅ Data quality rules
- ❌ NO direct source() references (use staging models)

**MARTS (Gold):**
- ✅ Dimensional models (star schema)
- ✅ Facts (transactions, events, metrics)
- ✅ Dimensions (customers, products, dates)
- ✅ Surrogate keys, SCD Type 2 if needed
- ❌ NO raw data (only from intermediate)
- ❌ NO cross-mart dependencies