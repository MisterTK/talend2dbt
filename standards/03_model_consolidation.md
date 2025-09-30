# Model Consolidation Strategy

**Version:** 3.0.0

---

## Consolidation Decision Tree

```
START: Analyze Talend Job
       ↓
┌──────────────────────────────────────────┐
│ Job has tRunJob calls?                   │
│ (has_child_jobs = true)                  │
└──────┬───────────────────────┬───────────┘
   YES │                       │ NO
       ↓                       ↓
┌──────────────────┐   ┌─────────────────────────────┐
│ DO NOT           │   │ Job in transaction group?   │
│ CONSOLIDATE      │   │ (transaction_patterns data) │
│                  │   └──────┬──────────────┬───────┘
│ Role:            │      YES │              │ NO
│ orchestrator     │          ↓              ↓
│                  │   ┌──────────────┐  ┌──────────────────┐
│ Action:          │   │ PRESERVE as  │  │ Job has error    │
│ Document in      │   │ TRANSACTION  │  │ patterns?        │
│ comments only    │   │ MODEL        │  │ (tDie/tWarn)     │
│ No SQL generated │   │              │  └──────┬──────┬────┘
└──────────────────┘   │ Keep atomic  │     YES │      │ NO
                       │ operation    │         ↓      ↓
                       └──────────────┘   ┌──────────┐ ┌────────────┐
                                          │ SEPARATE │ │ SAFE TO    │
                                          │ AUDIT    │ │ CONSOLIDATE│
                                          │ MODEL    │ │            │
                                          └──────────┘ └────────────┘
```

## Consolidation Rules

**✅ CONSOLIDATE These Patterns:**

1. **Sequential Single-Path Transformations**
   ```
   Talend: JobA (tDBInput) → JobB (tMap) → JobC (tFilter) → JobD (tOutput)
   Condition: No branching, no tRunJob, same data grain
   Result: 1 DBT model with CTEs
   ```

2. **Temporary Tables (Single Use)**
   ```
   Talend: Job writes to temp table, only one job reads it
   Result: Convert temp table to CTE in consuming model
   ```

3. **Multiple Aggregations (Same Grain)**
   ```
   Talend: tAggregateRow_1 → tAggregateRow_2 → tAggregateRow_3
   Condition: All aggregate same dimensions
   Result: Single GROUP BY with multiple aggregations
   ```

4. **Small Lookup Tables (<10K rows, static)**
   ```
   Talend: Child job loads lookup dimension
   Condition: No updates, small data, inline-able
   Result: CTE in consuming fact model
   ```

5. **Simple Child Jobs (<5 components, no dependencies)**
   ```
   Talend: Child job with tDBInput → tMap → tDBOutput
   Condition: Called by single parent, no reuse
   Result: Merge into parent as CTE
   ```

**❌ DO NOT CONSOLIDATE These Patterns:**

1. **Orchestrator Jobs (tRunJob present)**
   ```
   Talend: Master job with tRunJob_1, tRunJob_2, tRunJob_3
   Reason: Orchestration boundary (each child is separate model)
   Action: Document orchestration in comments, no SQL
   ```

2. **Transaction Boundaries**
   ```
   Talend: tDBConnection → operations → tDBCommit/tDBRollback
   Reason: Atomic operation group (preserve ACID semantics)
   Action: One model per transaction group
   ```

3. **Error/Reject Flows**
   ```
   Talend: Main flow → tFilterRow → REJECT → tLogRow → audit_table
   Reason: Separate audit concern
   Action: Create [model_name]_rejects.sql for audit
   ```

4. **Reusable Transformations (>3 dependencies)**
   ```
   Talend: Dimension loaded by multiple facts
   Reason: Shared dependency (DRY principle)
   Action: Keep as separate model, ref() from consumers
   ```

5. **Different Materialization Strategies**
   ```
   Talend: JobA (view), JobB (incremental table), JobC (full refresh)
   Reason: Different performance/update patterns
   Action: Separate models with appropriate config
   ```

6. **Different Update Cadences**
   ```
   Talend: Dimension (daily refresh), Fact (hourly incremental)
   Reason: Different scheduling requirements
   Action: Separate models
   ```

7. **Cross-Domain Jobs**
   ```
   Talend: Job combines customer360 + sales + finance data
   Reason: Domain boundary preservation
   Action: Keep domains separated for maintainability
   ```

## Consolidation Validation

**Calculate Consolidation Score:**
```python
consolidation_score = (dbt_models_generated / talend_jobs_parsed)

TARGET RANGE: 0.15 ≤ score ≤ 0.30 (70-85% reduction)

if score > 0.30:
    # Under-consolidating (<70% reduction)
    WARNING: "Look for more merge opportunities"

elif score < 0.15:
    # Over-consolidating (>85% reduction)
    ERROR: "May have violated boundaries - review orchestrators, transactions"

else:
    # Optimal range
    SUCCESS: "Consolidation within target"
```

**Pre-Consolidation Checklist:**
```
Before merging Job A + Job B into single model:
  ☐ Same data grain/granularity
  ☐ Same refresh schedule (hourly, daily, etc.)
  ☐ No transaction boundary between them
  ☐ No error handling separation needed
  ☐ Combined SQL <500 lines
  ☐ No loss of testability
  ☐ Preserves all ref() dependencies
  ☐ No circular dependency created
```