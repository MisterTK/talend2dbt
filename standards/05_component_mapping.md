# Talend Component to SQL Mapping

**Version:** 3.0.0

---

## Component Translation Table

| Talend Component | SQL Pattern | Completeness |
|-----------------|-------------|--------------|
| tDBInput | `SELECT * FROM {{ source() }}` | 100% |
| tMap (expressions) | `CASE WHEN ... THEN ... END`, `CONCAT()`, `UPPER()` | 98% |
| tMap (joins) | `LEFT/INNER JOIN ON condition` | 100% |
| tFilterRow | `WHERE condition` | 100% |
| tAggregateRow | `GROUP BY ... HAVING ...` | 100% |
| tJoin | `JOIN ... ON ...` | 100% |
| tSortRow | `ORDER BY` | 100% |
| tUniqRow | `DISTINCT` or `ROW_NUMBER() OVER()` | 100% |
| tDBOutput (INSERT) | Materialized table | 100% |
| tDBOutput (UPDATE) | Incremental with merge | 95% |
| tNormalize | `UNNEST()` or `CROSS JOIN` | 95% |
| tDenormalize | `STRING_AGG()` or pivot | 95% |
| tRunJob | `ref('child_model')` | 100% |

## Talend Expression to BigQuery SQL

### String Functions

```python
# Talend Java Expression → BigQuery SQL
row.field.substring(0,10)           → SUBSTR(field, 1, 10)
row.field.length()                  → LENGTH(field)
row.field.trim()                    → TRIM(field)
row.field.toUpperCase()             → UPPER(field)
row.field.toLowerCase()             → LOWER(field)
row.field.replace("old", "new")     → REPLACE(field, 'old', 'new')
StringHandling.UPCASE(field)        → UPPER(field)
field1 + field2                     → CONCAT(field1, field2)
field1 + "_" + field2               → CONCAT(field1, '_', field2)
```

### Date Functions

```python
# Talend Java Expression → BigQuery SQL
TalendDate.getCurrentDate()                          → CURRENT_DATE()
TalendDate.parseDate("yyyy-MM-dd", str)              → PARSE_DATE('%Y-%m-%d', str)
TalendDate.formatDate("yyyy-MM-dd", date)            → FORMAT_DATE('%Y-%m-%d', date)
TalendDate.addDate(date, 1, "dd")                    → DATE_ADD(date, INTERVAL 1 DAY)
TalendDate.diffDate(date1, date2, "dd")              → DATE_DIFF(date2, date1, DAY)
TalendDate.getPartOfDate("YEAR", date)               → EXTRACT(YEAR FROM date)
new java.util.Date()                                 → CURRENT_TIMESTAMP()
```

### Numeric Functions

```python
# Talend Java Expression → BigQuery SQL
Math.round(value)                   → ROUND(value)
Math.floor(value)                   → FLOOR(value)
Math.ceil(value)                    → CEIL(value)
Math.abs(value)                     → ABS(value)
Math.pow(base, exp)                 → POWER(base, exp)
Numeric.sequence("s1", 1, 1)        → ROW_NUMBER() OVER()
```

### Null/Conditional Logic

```python
# Talend Java Expression → BigQuery SQL
row.field == null ? 0 : row.field                    → COALESCE(field, 0)
field1 == null && field2 == null                     → field1 IS NULL AND field2 IS NULL
Relational.ISNULL(field)                             → field IS NULL
!Relational.ISNULL(field)                            → field IS NOT NULL
field.equals("value")                                → field = 'value'

# Nested ternary (complex)
a ? (b ? x : y) : z                                  → CASE WHEN a THEN
                                                           CASE WHEN b THEN x ELSE y END
                                                         ELSE z END
```