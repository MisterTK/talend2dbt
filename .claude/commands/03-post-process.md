---
description: Format, lint, and validate DBT migration output with automated quality checks
argument-hint: <dbt_project_path>
allowed-tools: Bash, Read, Write, Edit, TodoWrite
---

# DBT Migration Quality Validation

You are a code quality expert who will validate and fix formatting, linting, and compilation issues in a migrated DBT project. This command runs after `/02-migrate` to ensure production-ready code quality.

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

## Phase 1: Environment Setup

**Install Required Tools:**

```bash
cd $1
pip install sqlfluff sqlfluff-templater-dbt yamllint mdformat mdformat-gfm --quiet
```

**Verify Tool Versions:**

```bash
sqlfluff --version
yamllint --version
dbt --version
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

**Initial Lint Check:**

```bash
export GCP_PROJECT_ID="migration-validation-project"
sqlfluff lint models/ --dialect bigquery --nocolor 2>&1 | grep -E "^(L:|==|All)"
```

**Auto-Fix Issues:**

```bash
export GCP_PROJECT_ID="migration-validation-project"
sqlfluff fix models/ --dialect bigquery --force --nocolor
```

**Expected Fixes:**

- Layout issues (indentation, line length, trailing newlines)
- Aliasing (explicit table aliases)
- Structure (ELSE NULL removal, join order optimization)

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

**Check All YAML Files:**

```bash
yamllint models/ dbt_project.yml profiles.yml 2>&1 | head -50
```

**Fix Common Issues:**

```bash
# Add missing newlines at end of files
for file in models/staging/_schema.yml models/staging/_sources.yml models/marts/_schema.yml models/intermediate/_schema.yml dbt_project.yml profiles.yml; do
  echo "" >> "$file"
done
```

### Step 3: Fix Deprecated DBT Test Syntax

**Update `accepted_values` tests:**

```python
import re

files = [
    'models/staging/_schema.yml',
    'models/intermediate/_schema.yml',
    'models/marts/_schema.yml'
]

for filepath in files:
    with open(filepath, 'r') as f:
        content = f.read()

    # Fix accepted_values syntax (DBT 1.10+)
    content = re.sub(
        r'accepted_values:\n(\s+)values:',
        r'accepted_values:\n\1arguments:\n\1  values:',
        content
    )

    # Fix relationships syntax (DBT 1.10+)
    content = re.sub(
        r'relationships:\n(\s+)to:',
        r'relationships:\n\1arguments:\n\1  to:',
        content
    )

    with open(filepath, 'w') as f:
        f.write(content)
```

## Phase 4: DBT Project Validation

### Step 1: Parse DBT Project

**Run DBT Parse:**

```bash
export GCP_PROJECT_ID="migration-validation-project"
dbt parse --no-partial-parse 2>&1 | grep -E "(Running with|Registered|Found|Completed|ERROR|WARNING)"
```

**Expected Output:**

- ✅ Successfully parsed
- ✅ All models found
- ✅ All tests found
- ✅ All sources found
- ✅ All macros found

### Step 2: Address Warnings

**Common Warnings to Fix:**

1. **Deprecated Test Syntax:**
   - Update `accepted_values` to use `arguments` property
   - Update `relationships` to use `arguments` property

2. **Unused Configurations:**
   - Remove or comment out unused seed/snapshot configs
   - These are informational only if no seeds/snapshots exist

## Phase 5: Markdown Formatting

### Step 1: Format Documentation

**Check Markdown Files:**

```bash
mdformat --check docs/*.md README.md *.md 2>&1 | tail -10
```

**Auto-Format:**

```bash
mdformat docs/*.md README.md *.md
```

**Formatting Applied:**

- Consistent heading styles
- Standardized table formatting
- Code block language tags
- Horizontal rule formatting

## Phase 6: Quality Report Generation

### Generate Comprehensive Quality Report

Create `QUALITY_REPORT.md` with the following sections:

1. **Summary**
   - Tool versions used
   - Total files processed
   - Issues found and fixed
   - Overall status

2. **SQL Quality Results**
   - Files linted
   - Issues auto-fixed
   - Remaining issues (if any)

3. **YAML Validation Results**
   - Files validated
   - Syntax errors fixed
   - DBT 1.10 compatibility

4. **DBT Compilation Results**
   - Parse status
   - Models/tests/sources found
   - Warnings addressed

5. **Documentation Quality**
   - Markdown files formatted
   - Total lines processed

6. **Production Readiness Checklist**
   - [ ] SQL files formatted and linted
   - [ ] YAML files validated
   - [ ] DBT project parses successfully
   - [ ] All tests defined correctly
   - [ ] Documentation formatted
   - [ ] Quality report generated

## Phase 7: Final Validation

### Run Complete Validation Suite

**Execute All Checks:**

```bash
# SQL Linting
echo "=== SQL LINTING ==="
sqlfluff lint models/ --dialect bigquery --nocolor 2>&1 | grep -E "^(==|All)"

# YAML Validation
echo "=== YAML VALIDATION ==="
yamllint models/ dbt_project.yml profiles.yml 2>&1 | wc -l

# DBT Parse
echo "=== DBT PARSE ==="
dbt parse --no-partial-parse 2>&1 | grep "Completed"

# File Count
echo "=== FILE SUMMARY ==="
echo "Total Files: $(find . -type f \( -name "*.sql" -o -name "*.yml" -o -name "*.md" \) | wc -l | tr -d ' ')"
echo "SQL Models: $(find models -name "*.sql" | wc -l | tr -d ' ')"
echo "YAML Schemas: $(find models -name "*.yml" | wc -l | tr -d ' ')"
echo "Tests: $(find tests -name "*.sql" | wc -l | tr -d ' ')"
echo "Documentation: $(find . -name "*.md" | wc -l | tr -d ' ')"
```

## Success Criteria

The validation is complete when:

- ✅ All SQL files pass sqlfluff linting
- ✅ All YAML files pass yamllint validation
- ✅ DBT project parses without errors
- ✅ All deprecated syntax updated to DBT 1.10
- ✅ All markdown files formatted consistently
- ✅ Quality report generated
- ✅ No manual intervention required

## Output Deliverables

1. **Configuration Files**
   - `.sqlfluff` - SQL linting rules
   - `.yamllint` - YAML validation rules

2. **Quality Report**
   - `QUALITY_REPORT.md` - Complete validation results

3. **Fixed Files**
   - All SQL models formatted and linted
   - All YAML schemas validated and fixed
   - All markdown documentation formatted

## Execution Guarantee

**The validation ALWAYS follows this order:**

1. **Setup** - Install tools and create configurations
2. **SQL** - Lint and format all SQL files
3. **YAML** - Validate and fix all YAML files
4. **DBT** - Parse and validate DBT project
5. **Docs** - Format all markdown documentation
6. **Report** - Generate comprehensive quality report
7. **Validate** - Run final validation suite

The validation process is fully automated with minimal manual intervention required. All auto-fixable issues are resolved automatically.