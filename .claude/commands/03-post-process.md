______________________________________________________________________

## description: Format, lint, and validate DBT migration output with automated quality checks argument-hint: \<dbt_project_path> allowed-tools: Bash, Read, Write, Edit, TodoWrite

# DBT Migration Quality Validation

You are a code quality expert who will validate and fix formatting, linting, and compilation issues
in a migrated DBT project. This command runs after `/02-migrate` to ensure production-ready code
quality.

## Command Usage

```
/03-post-process <dbt_project_path>
```

**Arguments:**

- `$1` (dbt_project_path, required): Path to the generated DBT project directory

## Mission Statement

Ensure the migrated DBT project meets production quality standards:

- Format and lint all SQL files with sqlfluff
- Validate all YAML files with yamllint
- Parse and compile the DBT project successfully
- Format markdown documentation
- Generate comprehensive quality report
- Fix all auto-fixable issues

## Phase 1: Environment Verification

**Verify Tools Available (installed by bootstrap):**

```bash
echo "==================================="
echo "VERIFYING TOOL VERSIONS"
echo "==================================="
echo ""

cd "$1"

# Capture tool versions for report
SQLFLUFF_VERSION=$(sqlfluff --version 2>&1 | head -1)
YAMLLINT_VERSION=$(yamllint --version 2>&1)
DBT_VERSION=$(dbt --version 2>&1 | grep "Core:" | awk '{print $2}')
MDFORMAT_VERSION=$(mdformat --version 2>&1)

echo "✅ sqlfluff: $SQLFLUFF_VERSION"
echo "✅ yamllint: $YAMLLINT_VERSION"
echo "✅ dbt: $DBT_VERSION"
echo "✅ mdformat: $MDFORMAT_VERSION"
echo ""

# Save versions for Python helper
cat > /tmp/tool_versions.json <<EOF
{
  "sqlfluff": "$SQLFLUFF_VERSION",
  "yamllint": "$YAMLLINT_VERSION",
  "dbt": "$DBT_VERSION",
  "mdformat": "$MDFORMAT_VERSION"
}
EOF
```

## Phase 2: SQL Linting & Formatting

### Step 1: Create SQLFluff Configuration

Create `.sqlfluff` configuration file:

```ini
[sqlfluff]
templater = dbt
dialect = bigquery
sql_file_exts = .sql
exclude_rules = L034, L036, L044

[sqlfluff:templater:dbt]
project_dir = .
profiles_dir = .

[sqlfluff:indentation]
indent_unit = space
tab_space_size = 4

max_line_length = 120

[sqlfluff:rules:L010]
capitalisation_policy = upper

[sqlfluff:rules:L014]
capitalisation_policy = upper

[sqlfluff:rules:L030]
capitalisation_policy = upper

[sqlfluff:rules:L040]
capitalisation_policy = upper
```

### Step 2: Lint SQL Files

```bash
echo "==================================="
echo "SQL LINTING & FORMATTING"
echo "==================================="
echo ""

export GCP_PROJECT_ID="migration-validation-project"

# Initial lint check
echo "Running initial SQL lint check..."
INITIAL_ISSUES=$(sqlfluff lint models/ --dialect bigquery --nocolor 2>&1 | grep -c "^L:" || echo "0")
echo "Found $INITIAL_ISSUES linting issues"
echo ""

# Auto-fix issues
echo "Auto-fixing SQL issues..."
sqlfluff fix models/ --dialect bigquery --force --nocolor 2>&1 | tail -5
echo ""

# Final lint check
echo "Running final SQL lint check..."
REMAINING_ISSUES=$(sqlfluff lint models/ --dialect bigquery --nocolor 2>&1 | grep -c "^L:" || echo "0")
ISSUES_FIXED=$((INITIAL_ISSUES - REMAINING_ISSUES))

echo "✅ Fixed $ISSUES_FIXED issues"
echo "⚠️  Remaining issues: $REMAINING_ISSUES"
echo ""

# Count SQL files
SQL_FILES=$(find models -name "*.sql" | wc -l | tr -d ' ')

# Save results for Python helper
cat > /tmp/sql_results.json <<EOF
{
  "files_linted": $SQL_FILES,
  "issues_fixed": $ISSUES_FIXED,
  "remaining_issues": $REMAINING_ISSUES,
  "passed": $([ $REMAINING_ISSUES -eq 0 ] && echo "true" || echo "false")
}
EOF
```

## Phase 3: YAML Validation

### Step 1: Create YAML Lint Configuration

Create `.yamllint` configuration file:

```yaml
---
extends: default

rules:
  line-length:
    max: 120
    level: warning
  indentation:
    spaces: 2
    indent-sequences: true
  comments:
    min-spaces-from-content: 1
  document-start: disable
  truthy:
    allowed-values: ['true', 'false', 'yes', 'no']
```

### Step 2: Validate YAML Files

```bash
echo "==================================="
echo "YAML VALIDATION"
echo "==================================="
echo ""

# Count YAML errors before fixes
echo "Checking YAML files..."
YAML_ERRORS_BEFORE=$(yamllint models/ dbt_project.yml profiles.yml 2>&1 | grep -c "error" || echo "0")
echo "Found $YAML_ERRORS_BEFORE YAML errors"
echo ""

# Count YAML files
YAML_FILES=$(find models -name "*.yml" | wc -l | tr -d ' ')

# Save initial state
YAML_ERRORS_FIXED=0
```

### Step 3: Fix Deprecated DBT Test Syntax (Python Helper)

```bash
echo "Fixing deprecated DBT 1.10+ test syntax..."
python -m dbt_generator.generator fix-test-syntax --project "$1" > /tmp/test_syntax_fixes.json

# Extract results
TEST_FIXES=$(cat /tmp/test_syntax_fixes.json | python3 -c "import sys, json; data=json.load(sys.stdin); print(data['fixes_applied'])")
echo "✅ Fixed $TEST_FIXES deprecated test syntax issues"
echo ""

# Re-check YAML after fixes
YAML_ERRORS_AFTER=$(yamllint models/ dbt_project.yml profiles.yml 2>&1 | grep -c "error" || echo "0")
YAML_ERRORS_FIXED=$((YAML_ERRORS_BEFORE - YAML_ERRORS_AFTER))

# Save results for Python helper
cat > /tmp/yaml_results.json <<EOF
{
  "files_validated": $YAML_FILES,
  "errors_fixed": $YAML_ERRORS_FIXED,
  "test_syntax_fixes": $TEST_FIXES,
  "errors_remaining": $YAML_ERRORS_AFTER,
  "passed": $([ $YAML_ERRORS_AFTER -eq 0 ] && echo "true" || echo "false"),
  "accepted_values_fixed": $TEST_FIXES,
  "relationships_fixed": 0
}
EOF
```

## Phase 4: DBT Project Validation

### Step 1: Parse DBT Project

```bash
echo "==================================="
echo "DBT PROJECT VALIDATION"
echo "==================================="
echo ""

export GCP_PROJECT_ID="migration-validation-project"

echo "Parsing DBT project..."
DBT_PARSE_OUTPUT=$(dbt parse --no-partial-parse 2>&1)

# Check if parse succeeded
if echo "$DBT_PARSE_OUTPUT" | grep -q "Completed successfully"; then
    DBT_PASSED="true"
    echo "✅ DBT parse successful"
else
    DBT_PASSED="false"
    echo "❌ DBT parse failed"
fi
echo ""

# Extract resource counts
MODELS_COUNT=$(echo "$DBT_PARSE_OUTPUT" | grep -oP "Found \K\d+(?= models)" || echo "0")
TESTS_COUNT=$(echo "$DBT_PARSE_OUTPUT" | grep -oP "Found \K\d+(?= tests)" || echo "0")
SOURCES_COUNT=$(echo "$DBT_PARSE_OUTPUT" | grep -oP "Found \K\d+(?= sources)" || echo "0")
MACROS_COUNT=$(echo "$DBT_PARSE_OUTPUT" | grep -oP "Found \K\d+(?= macros)" || echo "0")
WARNINGS_COUNT=$(echo "$DBT_PARSE_OUTPUT" | grep -c "WARNING" || echo "0")

echo "Resources found:"
echo "  - Models: $MODELS_COUNT"
echo "  - Tests: $TESTS_COUNT"
echo "  - Sources: $SOURCES_COUNT"
echo "  - Macros: $MACROS_COUNT"
echo "  - Warnings: $WARNINGS_COUNT"
echo ""

# Save results for Python helper
cat > /tmp/dbt_results.json <<EOF
{
  "passed": $DBT_PASSED,
  "models": $MODELS_COUNT,
  "tests": $TESTS_COUNT,
  "sources": $SOURCES_COUNT,
  "macros": $MACROS_COUNT,
  "warnings": $WARNINGS_COUNT
}
EOF
```

## Phase 5: Markdown Formatting

```bash
echo "==================================="
echo "MARKDOWN FORMATTING"
echo "==================================="
echo ""

# Format all markdown files
echo "Formatting markdown files..."
if ls *.md docs/*.md 2>/dev/null | head -1 > /dev/null; then
    mdformat *.md docs/*.md 2>&1 | tail -5 || true
    echo "✅ Markdown files formatted"
else
    echo "⚠️  No markdown files found to format"
fi
echo ""
```

## Phase 6: Quality Report Generation (Python Helper)

```bash
echo "==================================="
echo "GENERATING QUALITY REPORT"
echo "==================================="
echo ""

# Generate comprehensive quality report using Python helper
python -m dbt_generator.generator quality-report \
    --project "$1" \
    --tool-versions "$(cat /tmp/tool_versions.json)" \
    --sql-results "$(cat /tmp/sql_results.json)" \
    --yaml-results "$(cat /tmp/yaml_results.json)" \
    --dbt-results "$(cat /tmp/dbt_results.json)" > /tmp/quality_report_result.json

REPORT_PATH=$(cat /tmp/quality_report_result.json | python3 -c "import sys, json; print(json.load(sys.stdin)['report_path'])")

echo "✅ Quality report generated: $REPORT_PATH"
echo ""
```

## Phase 7: Final Summary

```bash
echo "==================================="
echo "✅ POST-PROCESSING COMPLETE"
echo "==================================="
echo ""

# Get file counts from Python helper
python -m dbt_generator.generator file-count --project "$1" > /tmp/file_counts.json

SQL_MODELS=$(cat /tmp/file_counts.json | python3 -c "import sys, json; print(json.load(sys.stdin)['sql_models'])")
YAML_SCHEMAS=$(cat /tmp/file_counts.json | python3 -c "import sys, json; print(json.load(sys.stdin)['yaml_schemas'])")
TESTS=$(cat /tmp/file_counts.json | python3 -c "import sys, json; print(json.load(sys.stdin)['tests'])")
MARKDOWN=$(cat /tmp/file_counts.json | python3 -c "import sys, json; print(json.load(sys.stdin)['markdown_docs'])")
TOTAL=$(cat /tmp/file_counts.json | python3 -c "import sys, json; print(json.load(sys.stdin)['total_files'])")

echo "📊 Files Processed:"
echo "   - SQL Models: $SQL_MODELS"
echo "   - YAML Schemas: $YAML_SCHEMAS"
echo "   - Tests: $TESTS"
echo "   - Documentation: $MARKDOWN"
echo "   - Total: $TOTAL"
echo ""

echo "✅ SQL Linting: $ISSUES_FIXED issues fixed, $REMAINING_ISSUES remaining"
echo "✅ YAML Validation: $TEST_FIXES test syntax fixes applied"
echo "✅ DBT Parse: $MODELS_COUNT models, $TESTS_COUNT tests, $SOURCES_COUNT sources"
echo "✅ Quality Report: $REPORT_PATH"
echo ""

if [ "$REMAINING_ISSUES" -eq 0 ] && [ "$DBT_PASSED" = "true" ]; then
    echo "🎉 Project is production-ready!"
else
    echo "⚠️  Manual review required - see $REPORT_PATH for details"
fi
echo ""
```

## Success Criteria

✅ Post-processing complete when:

- All SQL files linted with sqlfluff (auto-fixes applied)
- All YAML files validated with yamllint
- DBT project parses without errors
- Deprecated test syntax updated to DBT 1.10+
- All markdown files formatted consistently
- Comprehensive quality report generated (`QUALITY_REPORT.md`)
- Project status clear (production-ready or needs review)

## Output Deliverables

1. **Configuration Files** (created in project root):

   - `.sqlfluff` - SQL linting rules for BigQuery
   - `.yamllint` - YAML validation rules

1. **Quality Report**:

   - `QUALITY_REPORT.md` - Comprehensive validation results with metrics

1. **Fixed Files**:

   - All SQL models formatted and linted
   - All YAML schemas with DBT 1.10+ test syntax
   - All markdown documentation formatted

1. **Validation Data** (temporary files in `/tmp`):

   - `tool_versions.json` - Tool versions used
   - `sql_results.json` - SQL linting metrics
   - `yaml_results.json` - YAML validation metrics
   - `dbt_results.json` - DBT parse results
   - `file_counts.json` - Project file counts

## Phase Execution Order

The command ALWAYS executes in this order:

1. **Phase 1** - Verify tools and capture versions
1. **Phase 2** - Create configs, lint and fix SQL files
1. **Phase 3** - Validate YAML, fix deprecated test syntax (Python helper)
1. **Phase 4** - Parse and validate DBT project
1. **Phase 5** - Format markdown documentation
1. **Phase 6** - Generate quality report (Python helper)
1. **Phase 7** - Display final summary with file counts (Python helper)

**Python Helpers Used:**

- `fix-test-syntax` - Updates DBT 1.10+ test syntax
- `file-count` - Counts project files by type
- `quality-report` - Generates comprehensive QUALITY_REPORT.md

All auto-fixable issues are resolved automatically. The quality report identifies any remaining
issues requiring manual review.
