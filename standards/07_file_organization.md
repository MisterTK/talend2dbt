# File Organization & Project Structure

**Version:** 3.0.0

---

## Directory Layout

```
dbt_project/
├── dbt_project.yml              # Project configuration
├── profiles.yml                 # Connection profiles (not in git)
├── models/
│   ├── staging/                 # Bronze layer
│   │   ├── _sources.yml         # ALL source table definitions
│   │   ├── [domain_1]/
│   │   │   ├── _schema.yml      # Staging model docs/tests
│   │   │   ├── stg_[domain_1]__[table_1].sql
│   │   │   ├── stg_[domain_1]__[table_2].sql
│   │   │   └── ...
│   │   └── [domain_2]/
│   │       └── ...
│   ├── intermediate/            # Silver layer
│   │   └── [domain_1]/
│   │       ├── _schema.yml      # Intermediate model docs/tests
│   │       ├── int_[domain_1]__[transform_1].sql
│   │       └── ...
│   └── marts/                   # Gold layer
│       └── [domain_1]/
│           ├── _schema.yml      # Mart model docs/tests
│           ├── fct_[entity].sql
│           ├── dim_[entity].sql
│           └── ...
├── macros/
│   ├── talend_compatibility.sql # Talend function macros
│   └── transformations.sql      # Reusable business logic
├── tests/
│   ├── generic/                 # Reusable test definitions
│   └── singular/                # Custom SQL tests
└── docs/
    └── migration_report.md      # Migration documentation
```

## Source Definitions (_sources.yml)

**✅ REQUIRED - Complete source definitions:**

```yaml
# models/staging/_sources.yml
version: 2

sources:
  - name: raw
    description: Raw source tables from Talend extraction
    database: "{{ target.project }}"
    schema: "{{ target.dataset }}"
    tables:
      - name: dcs_cust_acct_dim
        description: Customer account dimension from Talend DCS_CUST_ACCT_DIM
        columns:
          - name: customer_id
            description: Unique customer identifier
            tests:
              - not_null
              - unique
          - name: first_name
            description: Customer first name
          - name: last_name
            description: Customer last name
          - name: email
            description: Customer email address

      - name: cust_segment_lkp
        description: Customer segment lookup from Talend CUST_SEGMENT_LKP
        columns:
          - name: customer_id
            description: Customer identifier (FK)
          - name: segment_code
            description: Segment classification code
```

**Rules:**
- ✅ ALL tables referenced in Talend tDBInput MUST be defined in _sources.yml
- ✅ Use `{{ target.project }}` and `{{ target.dataset }}` for portability
- ✅ Include primary key tests (not_null + unique)
- ✅ Add descriptions from Talend component comments
- ❌ DO NOT hardcode project/dataset names

## Schema Documentation (_schema.yml)

**Staging Layer Example:**
```yaml
# models/staging/customer360/_schema.yml
version: 2

models:
  - name: stg_customer360__account_dim
    description: Staging model for customer account dimension
    columns:
      - name: customer_id
        description: Unique customer identifier
        tests:
          - not_null
          - unique
      - name: email
        description: Customer email address
        tests:
          - not_null
```

**Intermediate Layer Example:**
```yaml
# models/intermediate/customer360/_schema.yml
version: 2

models:
  - name: int_customer360__customer_profile_enriched
    description: Enriched customer profile with segment information
    columns:
      - name: customer_id
        description: Unique customer identifier
        tests:
          - not_null
          - unique
      - name: segment_code
        description: Customer segment classification
        tests:
          - not_null
          - accepted_values:
              values: ['HIGH_VALUE', 'MEDIUM_VALUE', 'LOW_VALUE', 'UNKNOWN']
```