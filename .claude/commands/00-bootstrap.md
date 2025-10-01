---
description: Setup Python virtual environment and install talend2dbt package with all dependencies
allowed-tools: Bash, Read
---

# Talend Migration Environment Bootstrap

You are a Python environment setup expert who will automatically create a clean virtual environment and install the talend2dbt package with all its dependencies from `pyproject.toml`.

## Command Usage

```
/00-bootstrap
```

**No arguments required** - Fully automated setup.

## Mission Statement

Set up a complete, isolated Python environment with the talend2dbt package installed, providing:
- Phase 1: Talend XML parsing and optimization (`/01-pre-process`)
- Phase 2: DBT project generation and migration (`/02-migrate`)
- Phase 3: Code quality validation and linting (`/03-post-process`)
- CLI commands: `talend-parser` and `dbt-generator`

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

## Phase 3: Package Installation

### Step 5: Install talend2dbt Package

```bash
echo "==================================="
echo "INSTALLING TALEND2DBT PACKAGE"
echo "==================================="
echo ""
echo "Installing from pyproject.toml..."
echo ""

# Install package in editable mode with all dependencies
pip install -e . --quiet

if [ $? -ne 0 ]; then
    echo "❌ ERROR: Failed to install talend2dbt package"
    echo "Retrying with verbose output..."
    echo ""
    pip install -e .

    if [ $? -ne 0 ]; then
        echo ""
        echo "❌ CRITICAL: Package installation failed"
        echo ""
        echo "Troubleshooting:"
        echo "  1. Check pyproject.toml exists in current directory"
        echo "  2. Verify network connectivity for PyPI"
        echo "  3. Check for system dependencies (postgresql-dev, libpq-dev)"
        echo ""
        exit 1
    fi
fi

echo ""
echo "✅ talend2dbt package installed successfully"
echo ""
```

## Phase 4: Verification & Summary

### Step 6: Verify Installation

```bash
echo "==================================="
echo "VERIFYING INSTALLATION"
echo "==================================="
echo ""

# Test package installation
python -c "
import sys
try:
    # Test talend2dbt package imports
    from talend_parser.talend_parser import TalendParser
    from dbt_generator.generator import DBTGenerator
    print('✅ talend2dbt package verified')
    print('   - talend_parser: OK')
    print('   - dbt_generator: OK')

    # Test critical dependencies
    import yaml
    import tiktoken
    try:
        from dbt.version import get_installed_version
        dbt_version = get_installed_version().to_version_string()
    except:
        import dbt.version
        dbt_version = 'installed'
    import sqlfluff.core
    import yamllint
    print('   - pyyaml: OK')
    print('   - tiktoken: OK')
    print(f'   - dbt-core: {dbt_version}')
    print('   - sqlfluff: OK')
    print('   - yamllint: OK')
except ImportError as e:
    print(f'❌ ERROR: Import failed - {e}')
    sys.exit(1)
" 2>&1

if [ $? -ne 0 ]; then
    echo ""
    echo "❌ ERROR: Package verification failed"
    echo ""
    echo "Installed packages:"
    pip list | grep -E 'talend|pyyaml|tiktoken|dbt|sqlfluff|yamllint|mdformat'
    echo ""
    echo "Try reinstalling: pip install -e . --force-reinstall"
    exit 1
fi
echo ""

# Verify CLI commands are available
echo "Verifying CLI commands..."
if command -v talend-parser &> /dev/null && command -v dbt-generator &> /dev/null; then
    echo "✅ CLI commands registered:"
    echo "   - talend-parser"
    echo "   - dbt-generator"
else
    echo "⚠️  WARNING: CLI commands not found in PATH"
    echo "   You may need to restart your shell or run: hash -r"
fi
echo ""
```

### Step 7: Display Final Status

```bash
echo "==================================="
echo "✅ BOOTSTRAP COMPLETE"
echo "==================================="
echo ""
echo "📦 Package Information:"
TALEND2DBT_VERSION=$(python -c "import importlib.metadata; print(importlib.metadata.version('talend2dbt'))" 2>/dev/null || echo "1.0.0")
TOTAL_PACKAGES=$(pip list --format=freeze | wc -l | tr -d ' ')
echo "   talend2dbt: v$TALEND2DBT_VERSION"
echo "   Total packages: $TOTAL_PACKAGES"
echo ""
echo "Core Dependencies:"
pip list --format=columns | grep -E '^(PyYAML|tiktoken|dbt-core|dbt-bigquery|sqlfluff|yamllint|mdformat)' | sed 's/^/   /'
echo ""
echo "CLI Commands:"
echo "   talend-parser --help"
echo "   dbt-generator --help"
echo ""
echo "Environment:"
echo "   Python: $PYTHON_VERSION"
echo "   Virtual Env: $VIRTUAL_ENV"
echo "   Installed: editable mode (pip install -e .)"
echo ""
echo "==================================="
echo "⚠️  IMPORTANT NEXT STEPS"
echo "==================================="
echo ""
echo "1. ✅ Setup complete - only run once per project"
echo ""
echo "2. 🧹 Clear context (REQUIRED before migration):"
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
- macOS: Requires Homebrew (`brew install python@3.11`)
- Linux: Requires apt-get (`sudo apt-get install python3.11`) or yum
- Windows: Not supported - use WSL2 with Ubuntu
- Manual install: Download from python.org and re-run

**Problem: pip install -e . fails**
- **Missing pyproject.toml**: Ensure you're in the project root directory
- **System dependencies**: Install postgresql-dev/libpq-dev for dbt-bigquery
  - macOS: `brew install postgresql`
  - Ubuntu: `sudo apt-get install libpq-dev`
- **Network issues**: Check PyPI connectivity
- **Force reinstall**: `pip install -e . --force-reinstall`

**Problem: Import verification fails**
- Check error message for specific failed import
- Verify package installed: `pip list | grep talend2dbt`
- Check for conflicting versions: `pip check`
- Try clean reinstall:
  ```bash
  pip uninstall talend2dbt -y
  pip install -e .
  ```

**Problem: CLI commands not found**
- Run `hash -r` to refresh shell PATH cache
- Restart terminal session
- Verify venv activated: `echo $VIRTUAL_ENV`
- Check scripts installed: `ls venv/bin/ | grep talend`

## Success Criteria

✅ Bootstrap successful when:
- Python 3.11+ detected or installed
- Virtual environment created at `./venv`
- talend2dbt package installed in editable mode
- All package imports verified
- CLI commands registered (`talend-parser`, `dbt-generator`)
- Status summary displayed

## What Happens Next

After successful bootstrap:

1. **Run once** - No need to repeat unless you delete venv or update dependencies
2. **Clear context** - Free up memory before next phase: `/clear`
3. **Start migration** - Begin with: `/01-pre-process <path/to/talend/jobs>`

## Scope

This command ONLY:
- ✅ Detects/installs Python 3.11+
- ✅ Creates virtual environment (`./venv`)
- ✅ Installs talend2dbt package with all dependencies
- ✅ Registers CLI commands
- ✅ Verifies installation

This command does NOT:
- ❌ Process Talend jobs (use `/01-pre-process`)
- ❌ Generate DBT projects (use `/02-migrate`)
- ❌ Configure BigQuery credentials (manual setup required)
- ❌ Run any migration logic