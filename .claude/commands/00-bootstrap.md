---
description: Setup Python virtual environment and install all required dependencies for Talend-to-DBT migration
allowed-tools: Bash, Read, Write
---

# Talend Migration Environment Bootstrap

You are a Python environment setup expert who will automatically create a clean virtual environment and install all dependencies required for the complete Talend-to-DBT migration pipeline.

## Command Usage

```
/00-bootstrap
```

**No arguments required** - Fully automated setup.

## Mission Statement

Set up a complete, isolated Python environment with all tools needed for:
- Phase 1: Talend XML parsing and optimization (`/01-pre-process`)
- Phase 2: DBT project generation and migration (`/02-migrate`)
- Phase 3: Code quality validation and linting (`/03-post-process`)

## Phase 1: Python Detection & Setup

### Step 1: Find or Install Python 3.11+

```bash
echo "==================================="
echo "TALEND MIGRATION BOOTSTRAP"
echo "==================================="
echo ""
echo "Detecting Python installation..."

# Function to check Python version
check_python_version() {
    local python_cmd=$1
    if command -v "$python_cmd" &> /dev/null; then
        local version=$($python_cmd --version 2>&1 | awk '{print $2}')
        local major=$(echo $version | cut -d. -f1)
        local minor=$(echo $version | cut -d. -f2)

        if [ "$major" -ge 3 ] && [ "$minor" -ge 11 ]; then
            echo "$python_cmd"
            return 0
        fi
    fi
    return 1
}

# Try to find suitable Python version
PYTHON_CMD=""
for cmd in python3.13 python3.12 python3.11 python3 python; do
    if PYTHON_CMD=$(check_python_version "$cmd"); then
        break
    fi
done

# If no suitable Python found, install locally
if [ -z "$PYTHON_CMD" ]; then
    echo "⚠️  No suitable Python 3.11+ found on system"
    echo "Installing Python 3.11 locally..."

    # Detect OS
    if [[ "$OSTYPE" == "darwin"* ]]; then
        # macOS
        if command -v brew &> /dev/null; then
            brew install python@3.11 --quiet
            PYTHON_CMD="python3.11"
        else
            echo "❌ ERROR: Homebrew not found. Please install from https://brew.sh"
            exit 1
        fi
    elif [[ "$OSTYPE" == "linux-gnu"* ]]; then
        # Linux
        if command -v apt-get &> /dev/null; then
            sudo apt-get update -qq
            sudo apt-get install -y python3.11 python3.11-venv python3.11-dev
            PYTHON_CMD="python3.11"
        elif command -v yum &> /dev/null; then
            sudo yum install -y python311 python311-devel
            PYTHON_CMD="python3.11"
        else
            echo "❌ ERROR: Unsupported package manager. Please install Python 3.11+ manually"
            exit 1
        fi
    else
        echo "❌ ERROR: Unsupported OS. Please install Python 3.11+ manually"
        exit 1
    fi
fi

PYTHON_VERSION=$($PYTHON_CMD --version 2>&1 | awk '{print $2}')
echo "✅ Using: $PYTHON_CMD (version $PYTHON_VERSION)"
echo ""
```

### Step 2: Clean Previous Environment

```bash
# Remove existing venv if present
if [ -d "venv" ]; then
    echo "Removing existing virtual environment..."
    rm -rf venv
    echo "✅ Old environment removed"
fi
echo ""
```

## Phase 2: Virtual Environment Creation

### Step 3: Create Virtual Environment

```bash
echo "==================================="
echo "CREATING VIRTUAL ENVIRONMENT"
echo "==================================="
echo ""

# Create venv
$PYTHON_CMD -m venv venv

if [ $? -ne 0 ]; then
    echo "❌ ERROR: Failed to create virtual environment"
    echo "Try: $PYTHON_CMD -m pip install --upgrade virtualenv"
    exit 1
fi

echo "✅ Virtual environment created: ./venv"
echo ""

# Activate venv
source venv/bin/activate

# Verify activation
if [ -z "$VIRTUAL_ENV" ]; then
    echo "❌ ERROR: Failed to activate virtual environment"
    exit 1
fi

echo "✅ Virtual environment activated"
echo "   Location: $VIRTUAL_ENV"
echo ""
```

### Step 4: Upgrade Core Tools

```bash
echo "==================================="
echo "UPGRADING CORE TOOLS"
echo "==================================="
echo ""

# Upgrade pip, setuptools, wheel
python -m pip install --upgrade pip setuptools wheel --quiet

if [ $? -eq 0 ]; then
    PIP_VERSION=$(pip --version | awk '{print $2}')
    echo "✅ pip upgraded to version $PIP_VERSION"
else
    echo "❌ WARNING: Failed to upgrade pip"
fi
echo ""
```

## Phase 3: Core Dependencies Installation

### Step 5: Install All Required Dependencies

```bash
echo "==================================="
echo "INSTALLING CORE DEPENDENCIES"
echo "==================================="
echo ""

# Install all core dependencies in one go
pip install \
    pyyaml>=6.0 \
    tiktoken>=0.5.0 \
    dbt-core>=1.10.0 \
    dbt-bigquery>=1.10.0 \
    sqlfluff>=3.0.0 \
    sqlfluff-templater-dbt>=3.0.0 \
    yamllint>=1.35.0 \
    mdformat>=0.7.17 \
    mdformat-gfm>=0.3.5 \
    --quiet

if [ $? -ne 0 ]; then
    echo "❌ ERROR: Failed to install dependencies"
    echo "Retrying with verbose output..."
    pip install \
        pyyaml>=6.0 \
        tiktoken>=0.5.0 \
        dbt-core>=1.10.0 \
        dbt-bigquery>=1.10.0 \
        sqlfluff>=3.0.0 \
        sqlfluff-templater-dbt>=3.0.0 \
        yamllint>=1.35.0 \
        mdformat>=0.7.17 \
        mdformat-gfm>=0.3.5
    exit 1
fi

echo "✅ All dependencies installed successfully"
echo ""
```

## Phase 4: Verification & Summary

### Step 6: Verify Installation

```bash
echo "==================================="
echo "VERIFYING INSTALLATION"
echo "==================================="
echo ""

# Test critical imports
python -c "
import yaml
import tiktoken
try:
    from dbt.version import get_installed_version
    dbt_version = get_installed_version().to_version_string()
except:
    import dbt
    dbt_version = 'installed'
import sqlfluff.core
import yamllint
print('✅ All critical packages verified')
print(f'   - pyyaml: OK')
print(f'   - tiktoken: OK')
print(f'   - dbt-core: {dbt_version}')
print(f'   - sqlfluff: OK')
print(f'   - yamllint: OK')
" 2>&1

if [ $? -ne 0 ]; then
    echo "❌ ERROR: Package verification failed"
    echo ""
    echo "Installed packages:"
    pip list | grep -E 'pyyaml|tiktoken|dbt|sqlfluff|yamllint|mdformat'
    exit 1
fi
echo ""
```

### Step 7: Display Final Status

```bash
echo "==================================="
echo "✅ BOOTSTRAP COMPLETE"
echo "==================================="
echo ""
echo "📦 Installed Packages:"
TOTAL_PACKAGES=$(pip list --format=freeze | wc -l | tr -d ' ')
echo "   Total: $TOTAL_PACKAGES packages"
echo ""
echo "Core Dependencies:"
pip list | grep -E '^(PyYAML|tiktoken|dbt-core|dbt-bigquery|sqlfluff|yamllint|mdformat)' | sed 's/^/   /'
echo ""
echo "Environment:"
echo "   Python: $PYTHON_VERSION"
echo "   Location: $VIRTUAL_ENV"
echo ""
echo "==================================="
echo "⚠️  IMPORTANT NEXT STEPS"
echo "==================================="
echo ""
echo "1. ✅ Setup complete - only run once per project"
echo ""
echo "2. 🧹 Clear context (REQUIRED):"
echo "   /clear"
echo ""
echo "3. ▶️  Start migration pipeline:"
echo "   /01-pre-process <path/to/talend/jobs>"
echo ""
echo "==================================="
echo ""
```

## Troubleshooting

**Problem: Python installation fails**
- The script will attempt to install Python 3.11 using your system's package manager
- On macOS, Homebrew is required
- On Linux, apt-get or yum is required
- If installation fails, manually install Python 3.11+ and re-run

**Problem: Package installation fails**
- Script will automatically retry with verbose output
- Check for missing system dependencies (libpq, postgresql-dev)
- Ensure sufficient disk space
- Check internet connectivity

**Problem: Import verification fails**
- Check which specific package failed in the error output
- Manually verify with: `pip list | grep <package-name>`
- Try reinstalling failed package: `pip install --force-reinstall <package>`

## Success Criteria

✅ Bootstrap successful when:
- Python 3.11+ detected or installed
- Virtual environment created at ./venv
- All core dependencies installed
- Package imports verified
- Status summary displayed

## What Happens Next

After successful bootstrap:

1. **Run once** - No need to repeat unless you delete venv
2. **Clear context** - Free up memory before next phase: `/clear`
3. **Start migration** - Begin with: `/01-pre-process <path/to/talend/jobs>`

## Scope

This command ONLY:
- ✅ Detects/installs Python 3.11+
- ✅ Creates virtual environment
- ✅ Installs core dependencies
- ✅ Verifies installation

This command does NOT:
- ❌ Process Talend jobs
- ❌ Generate DBT projects
- ❌ Configure BigQuery credentials
- ❌ Run any migration logic