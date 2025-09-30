---
description: Pre-process Talend .item files into LLM-optimized format for migration
argument-hint: <talend_jobs_path> [output_path]
allowed-tools: Bash, Read, Write, Edit, Glob, Grep, TodoWrite
---

# Talend Pre-Processing Engine

You are an expert data engineer executing the first phase of Talend to DBT migration: extracting and optimizing Talend job definitions into LLM-friendly format.

## Command Usage
```
/01-pre-process <talend_jobs_path> [output_path]
```

**Arguments:**
- `$1` (talend_jobs_path, required): Path to directory containing Talend .item files
- `$2` (output_path, optional): Output directory for processed files (default: <talend_jobs_path>/../talend_processed)

## Mission

Run the LLM-optimized Talend parser to extract complete job information from raw Talend XML files, producing clean, structured output ready for Phase 2 migration.

## Execution Steps

### Step 1: Validate Input Directory

```bash
# Verify the input directory exists and contains .item files
ls -la "$1"/**/*.item | head -20
```

**Expected Output:**
- Multiple `.item` files in the directory tree
- If no files found, report error and stop

### Step 2: Set Output Directory

```bash
# Use provided output path or default to sibling directory
if [ -z "$2" ]; then
  INPUT_PARENT="$(dirname "$1")"
  OUTPUT_DIR="$INPUT_PARENT/talend_processed"
else
  OUTPUT_DIR="$2"
fi
mkdir -p "$OUTPUT_DIR"
echo "Output directory: $OUTPUT_DIR"
```

### Step 3: Execute Talend Parser

```bash
# Run the LLM-optimized parser
python talend_parser/talend_parser.py "$1" "$OUTPUT_DIR"
```

**What This Does:**
- Parses all `.item` files recursively
- Extracts SQL queries, tMap transformations, context variables
- Analyzes job hierarchy, dependencies, and complexity
- Generates LLM-optimized output files with token statistics

### Step 4: Verify Output Files

```bash
# Check all expected output files were created
ls -lh "$OUTPUT_DIR"
```

**Expected Files:**
1. `talend_extraction.json` - Complete job structure and metadata
2. `sql_queries.sql` - All extracted SQL with annotations
3. `transformations.json` - tMap business logic and expressions
4. `context_to_dbt.yml` - Context variable mappings
5. `extraction_summary.txt` - Human-readable summary
6. `token_statistics.txt` - Size and token analysis

### Step 5: Display Summary

```bash
# Show extraction summary
cat "$OUTPUT_DIR/extraction_summary.txt"
echo ""
echo "====================================="
echo "Token Statistics:"
cat "$OUTPUT_DIR/token_statistics.txt"
```

### Step 6: Quick Analysis

Read and display key metrics from the extraction:

```python
# Parse the extraction summary
import json

with open(f"{OUTPUT_DIR}/talend_extraction.json") as f:
    data = json.load(f)

metadata = data.get("extraction_metadata", {})
print(f"""
Pre-Processing Complete!
========================

📊 Extraction Results:
   - Total Jobs: {metadata.get('total_jobs', 0)}
   - SQL Queries: {metadata.get('total_sql_queries', 0)}
   - Transformations (tMap): {metadata.get('total_tmap_expressions', 0)}
   - Unique Tables: {metadata.get('unique_tables', 0)}
   - Context Variables: {metadata.get('context_variables', 0)}
   - Transaction Groups: {metadata.get('transaction_groups', 0)}
   - Error Patterns: {metadata.get('error_patterns', 0)}
   - Data Quality Rules: {metadata.get('data_quality_rules', 0)}

📁 Output Location: {OUTPUT_DIR}

🎯 Job Complexity:
""")

# Display job hierarchy
jobs_summary = data.get("jobs_summary", {})
for job_name, job_info in sorted(jobs_summary.items(),
                                  key=lambda x: x[1].get('complexity_score', 0),
                                  reverse=True)[:10]:
    role = job_info.get('role', 'unknown')
    complexity = job_info.get('complexity_score', 0)
    print(f"   - {job_name} ({role}): complexity={complexity}")
```

## Success Validation

✅ **Pre-processing successful if:**
- All 6 output files created
- No Python errors or exceptions
- Token statistics show data reduction
- Jobs summary lists all expected jobs
- SQL queries file contains actual SQL (not empty)

## Next Steps - CRITICAL

Once pre-processing completes successfully:

### 🔴 IMPORTANT: Clear Context Before Phase 2

```
/clear
```

**Why clear context?**
- Pre-processing loads large amounts of Talend XML data
- Phase 2 needs fresh context for clean migration
- Prevents token overflow and context confusion

### ▶️ Run Phase 2: Migration

Calculate the next command paths dynamically:

```bash
# Calculate Phase 2 paths based on actual output location
INPUT_DIR="$OUTPUT_DIR"
INPUT_PARENT="$(dirname "$OUTPUT_DIR")"
DBT_OUTPUT_DIR="$INPUT_PARENT/dbt_transformed"

echo ""
echo "====================================="
echo "✅ PRE-PROCESSING COMPLETE"
echo "====================================="
echo ""
echo "📋 Next Steps:"
echo ""
echo "1️⃣  Clear context (REQUIRED):"
echo "    /clear"
echo ""
echo "2️⃣  Run Phase 2 migration:"
echo "    /02-migrate $INPUT_DIR $DBT_OUTPUT_DIR"
echo ""
echo "Copy and paste the command above after running /clear"
echo ""
```

## Troubleshooting

**Problem: No .item files found**
- Solution: Verify the input path contains Talend job exports
- Check: `find "$1" -name "*.item" -type f`

**Problem: Parser fails with encoding error**
- Solution: Ensure .item files are UTF-8 encoded
- Check: `file "$1"/**/*.item | grep -i utf`

**Problem: Output files missing**
- Solution: Check Python dependencies installed
- Run: `pip install -r requirements.txt`

**Problem: Token statistics show 0% reduction**
- Expected: This is normal if source files are already compact
- The parser adds structured metadata for LLM consumption

## Output File Descriptions

### 1. talend_extraction.json
Complete structured data including:
- Job hierarchy and dependencies
- Component graphs and connections
- SQL queries with metadata
- tMap transformations with expressions
- Context variable classifications
- Transaction patterns
- Error handling patterns
- Data quality rules
- Performance hints

### 2. sql_queries.sql
Clean, annotated SQL ready for conversion:
- Each query labeled with job and component
- Operation type identified (SELECT, INSERT, etc.)
- Tables and context variables listed
- Formatted for readability

### 3. transformations.json
Business logic from tMap components:
- Input/output mappings
- Expression translations
- Lookup patterns with priorities
- Filter conditions
- Reject flows

### 4. context_to_dbt.yml
Context variable mapping guidance:
- Variable types and defaults
- DBT variable equivalents
- Usage patterns and validation rules

### 5. extraction_summary.txt
Human-readable overview:
- Job counts and hierarchy
- Component statistics
- Table analysis
- Quick reference guide

### 6. token_statistics.txt
LLM optimization metrics:
- Source file sizes and token counts
- Output file sizes and token counts
- Reduction percentages
- Conversion impact analysis

Progress is logged to console in real-time.