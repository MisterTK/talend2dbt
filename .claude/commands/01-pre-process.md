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

### Step 1: Activate Virtual Environment

```bash
# Activate the virtual environment created by bootstrap
if [ ! -d "venv" ]; then
    echo "❌ ERROR: Virtual environment not found"
    echo "Please run /00-bootstrap first"
    exit 1
fi

source venv/bin/activate

if [ -z "$VIRTUAL_ENV" ]; then
    echo "❌ ERROR: Failed to activate virtual environment"
    exit 1
fi

echo "✅ Virtual environment activated: $VIRTUAL_ENV"
echo ""
```

### Step 2: Validate Input Directory

```bash
# Verify the input directory exists
if [ ! -d "$1" ]; then
  echo "❌ ERROR: Input directory does not exist: $1"
  exit 1
fi

# Check for .item files
ITEM_COUNT=$(find "$1" -name "*.item" -type f 2>/dev/null | wc -l | tr -d ' ')

if [ "$ITEM_COUNT" -eq 0 ]; then
  echo "❌ ERROR: No .item files found in: $1"
  echo "Please provide a directory containing Talend .item files"
  exit 1
fi

echo "✅ Found $ITEM_COUNT Talend .item files"
echo ""

# Display first 20 files
echo "Sample files:"
ls -la "$1"/**/*.item 2>/dev/null | head -20
echo ""
```

### Step 3: Set Output Directory

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

### Step 4: Execute Talend Parser

```bash
# Run the LLM-optimized parser using venv Python
echo "Running Talend parser..."
echo "  Input: $1"
echo "  Output: $OUTPUT_DIR"
echo ""

# Always use venv/bin/activate to ensure correct environment
source venv/bin/activate && python -m talend_parser.talend_parser "$1" "$OUTPUT_DIR"

if [ $? -ne 0 ]; then
  echo ""
  echo "❌ ERROR: Parser failed. See error messages above."
  echo ""
  echo "Common fixes:"
  echo "  - Check .item files are valid XML (not corrupted)"
  echo "  - Verify virtual environment exists (run /00-bootstrap)"
  echo "  - Try: pip install -e . --force-reinstall"
  exit 1
fi

echo ""
echo "✅ Parser execution completed"
echo ""
```

**What This Does:**
- Parses all `.item` files recursively
- Extracts SQL queries, tMap transformations, context variables
- Analyzes job hierarchy, dependencies, and complexity
- Generates LLM-optimized output files with token statistics

### Step 5: Verify Output Files

```bash
# Validate all required files exist and are non-empty
REQUIRED_FILES=(
  "talend_extraction.json"
  "sql_queries.sql"
  "transformations.json"
  "context_to_dbt.yml"
  "extraction_summary.txt"
  "token_statistics.txt"
)

echo "Validating output files..."
ALL_VALID=true

for file in "${REQUIRED_FILES[@]}"; do
  if [ ! -f "$OUTPUT_DIR/$file" ]; then
    echo "❌ ERROR: Missing required file: $file"
    ALL_VALID=false
  elif [ ! -s "$OUTPUT_DIR/$file" ]; then
    echo "⚠️  WARNING: Empty file: $file"
  else
    SIZE=$(ls -lh "$OUTPUT_DIR/$file" | awk '{print $5}')
    echo "✅ $file ($SIZE)"
  fi
done

echo ""

if [ "$ALL_VALID" = false ]; then
  echo "❌ ERROR: Some required files are missing"
  echo "Parser may have failed. Check error messages above."
  exit 1
fi

echo "✅ All required files present"
echo ""

# Show directory listing
echo "Complete output:"
ls -lh "$OUTPUT_DIR"
echo ""
```

**Expected Files:**
1. `talend_extraction.json` - Complete job structure and metadata
2. `sql_queries.sql` - All extracted SQL with annotations
3. `transformations.json` - tMap business logic and expressions
4. `context_to_dbt.yml` - Context variable mappings
5. `extraction_summary.txt` - Human-readable summary
6. `token_statistics.txt` - Size and token analysis

### Step 6: Display Summary

```bash
# Show extraction summary
cat "$OUTPUT_DIR/extraction_summary.txt"
echo ""
echo "====================================="
echo "Token Statistics:"
cat "$OUTPUT_DIR/token_statistics.txt"
```

### Step 7: Quick Analysis

Read and display key metrics from the extraction:

```bash
# Use Python to parse and display extraction metrics
python << 'PYTHON_EOF'
import json
import sys

try:
    # Load extraction data
    with open("$OUTPUT_DIR/talend_extraction.json") as f:
        data = json.load(f)

    # Get metadata
    metadata = data.get("extraction_metadata", {})

    # Display summary
    print()
    print("=" * 60)
    print("PRE-PROCESSING COMPLETE")
    print("=" * 60)
    print()
    print("Extraction Results:")
    print(f"  - Total Jobs: {metadata.get('total_jobs', 0)}")
    print(f"  - SQL Queries: {metadata.get('total_sql_queries', 0)}")
    print(f"  - Transformations (tMap): {metadata.get('total_tmap_expressions', 0)}")
    print(f"  - Unique Tables: {metadata.get('unique_tables', 0)}")
    print(f"  - Context Variables: {metadata.get('context_variables', 0)}")
    print(f"  - Transaction Groups: {metadata.get('transaction_groups', 0)}")
    print(f"  - Error Patterns: {metadata.get('error_patterns', 0)}")
    print(f"  - Data Quality Rules: {metadata.get('data_quality_rules', 0)}")
    print()
    print(f"Output Location: $OUTPUT_DIR")
    print()

    # Display job hierarchy
    jobs_summary = data.get("jobs_summary", {})
    if jobs_summary:
        print("Job Hierarchy:")
        print()

        # Sort by component count (proxy for complexity)
        sorted_jobs = sorted(
            jobs_summary.items(),
            key=lambda x: x[1].get('components', 0),
            reverse=True
        )

        for job_name, job_info in sorted_jobs:
            role = job_info.get('role', 'unknown')
            comp_count = job_info.get('components', 0)
            sql_count = job_info.get('sql_queries', 0)
            tmap_count = job_info.get('transformations', 0)

            print(f"  {job_name}")
            print(f"    Role: {role} | Components: {comp_count} | SQL: {sql_count} | tMap: {tmap_count}")
            print()

    print("=" * 60)
    print()

except FileNotFoundError as e:
    print(f"ERROR: Could not find extraction file: {e}", file=sys.stderr)
    sys.exit(1)
except json.JSONDecodeError as e:
    print(f"ERROR: Invalid JSON in extraction file: {e}", file=sys.stderr)
    sys.exit(1)
except Exception as e:
    print(f"ERROR: Failed to parse extraction data: {e}", file=sys.stderr)
    sys.exit(1)

PYTHON_EOF

if [ $? -ne 0 ]; then
  echo ""
  echo "⚠️  WARNING: Could not parse extraction metrics"
  echo "Check $OUTPUT_DIR/extraction_summary.txt for details"
  echo ""
fi
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