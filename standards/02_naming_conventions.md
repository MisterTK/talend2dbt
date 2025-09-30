# Naming Conventions (STRICT ENFORCEMENT)

**Version:** 3.0.0

---

## Format Pattern

```
[layer_prefix]_[domain]__[specific_entity].[extension]

Components:
  layer_prefix := stg | int | fct | dim
  domain := extracted from Talend job/folder name
  specific_entity := table or transformation description
  __ := double underscore (REQUIRED between domain and entity)
  _ := single underscore (within multi-word components)
  extension := .sql | .yml
```

## Naming Rules

**✅ CORRECT Examples:**
```
stg_customer360__account_dim.sql
int_customer360__customer_profile_enriched.sql
fct_order_line_items.sql
dim_customer.sql
stg_sales__orders.sql
int_sales__orders_items_joined.sql
```

**❌ WRONG Examples (NEVER USE):**
```
staging_customer_profile.sql          ❌ (use stg_, not staging_)
stg_customer_profile.sql              ❌ (missing domain: stg_[domain]__profile)
stg_customer360_account_dim.sql       ❌ (single _ instead of __)
STG_CUSTOMER360__ACCOUNT.SQL          ❌ (uppercase - use lowercase)
customer_profile_staging.sql          ❌ (wrong order)
fact_orders.sql                       ❌ (use fct_, not fact_)
```

## Domain Extraction Rules

**From Talend Job Names:**

```
Pattern: [PROJECT_CODE]_[Entity]_[Type]_[Job]
Example: C360_Customer_Profile_FACT → domain: customer360 or c360

Pattern: [PROJECT]_[Module]_[Entity]
Example: KPI_Sales_Daily_Load → domain: kpi_sales

Pattern: Master_[Domain]_[Action]
Example: Master_Finance_Loader → domain: finance

Default: If no pattern matches → domain: default (flag for review)
```

**From Talend Folder Hierarchy:**

```
workspace/
  └── PROJECT_NAME/           ← Use this as domain
      └── process/
          └── Jobs/
              └── job.item

Example:
  workspace/Customer360/process/Jobs/LoadProfile.item
  → domain: customer360
```

## Layer Detection from Talend Job Names

**Dimension Detection (→ dim_[entity].sql):**
- Job name contains: "DIM", "DIMENSION", "LKP", "LOOKUP", "_D_"
- Examples:
  - `Customer_Profile_DIM` → `dim_customer_profile.sql`
  - `Product_Lookup` → `dim_product.sql`

**Fact Detection (→ fct_[entity].sql):**
- Job name contains: "FACT", "TRN", "TRANSACTION", "_F_", "METRICS"
- Examples:
  - `Order_Line_Items_FACT` → `fct_order_line_items.sql`
  - `Sales_Transaction` → `fct_sales.sql`

**Staging Detection (→ stg_[domain]__[source].sql):**
- Job name contains: "LOAD", "EXTRACT", "SOURCE", "RAW"
- Job has only tDBInput + minimal tMap
- Examples:
  - `C360_Load_Accounts` → `stg_customer360__accounts.sql`

**Intermediate Detection (→ int_[domain]__[desc].sql):**
- Job name contains: "TRANSFORM", "PROCESS", "ENRICH", "CLEANSE"
- Job has tMap with complex expressions
- Examples:
  - `C360_Transform_Customer_Profile` → `int_customer360__customer_profile_transformed.sql`