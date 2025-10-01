#!/usr/bin/env python3
"""
DBT Generator - Minimal Python helpers for Talend-to-DBT migration

IMPORTANT: This module handles ONLY:
- File system operations (mkdir, copy, backup)
- Data loading (JSON, YAML, SQL parsing)
- Validation (file existence, structure checks)
- Metrics calculation (pure math, counts)

It does NOT:
- Interpret standards (LLM does this)
- Make consolidation decisions (LLM does this)
- Generate SQL (LLM does this)
- Classify jobs or detect layers (LLM does this)

Philosophy: Python for infrastructure, LLM for intelligence.
"""

import json
import logging
import shutil
from datetime import datetime
from pathlib import Path
from typing import Any

import yaml

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)


class DBTGenerator:
    """Minimal Python helper for DBT migration file operations"""

    def __init__(self, input_dir: Path, output_dir: Path, mode: str = "new"):
        """
        Initialize DBT Generator

        Args:
            input_dir: Path to pre-processed Talend output
            output_dir: Path for DBT project output
            mode: 'new' for new project, 'merge' for existing project
        """
        self.input_dir = Path(input_dir)
        self.output_dir = Path(output_dir)
        self.mode = mode

        logger.info(
            f"DBTGenerator initialized: mode={mode}, input={input_dir}, output={output_dir}"
        )

    # =========================================================================
    # FILE LOADING (No interpretation, just reading)
    # =========================================================================

    def load_talend_data(self) -> dict[str, Any]:
        """
        Load all pre-processed Talend files into memory

        Returns:
            Dict with all loaded data:
            {
                'extraction': {...},      # talend_extraction.json
                'sql_queries': "...",     # sql_queries.sql
                'transformations': {...}, # transformations.json
                'context_vars': {...},    # context_to_dbt.yml
                'summary': "..."          # extraction_summary.txt
            }
        """
        logger.info("Loading Talend data files...")

        data = {
            "extraction": self._load_json("talend_extraction.json"),
            "sql_queries": self._load_text("sql_queries.sql"),
            "transformations": self._load_json("transformations.json"),
            "context_vars": self._load_yaml("context_to_dbt.yml"),
            "summary": self._load_text("extraction_summary.txt"),
        }

        logger.info("All Talend data loaded successfully")
        return data

    def _load_json(self, filename: str) -> dict[str, Any]:
        """Load JSON file from input directory"""
        path = self.input_dir / filename
        logger.debug(f"Loading JSON: {path}")

        with open(path) as f:
            return json.load(f)

    def _load_yaml(self, filename: str) -> dict[str, Any]:
        """Load YAML file from input directory"""
        path = self.input_dir / filename
        logger.debug(f"Loading YAML: {path}")

        with open(path) as f:
            return yaml.safe_load(f)

    def _load_text(self, filename: str) -> str:
        """Load text file from input directory"""
        path = self.input_dir / filename
        logger.debug(f"Loading text: {path}")

        return path.read_text()

    # =========================================================================
    # PROJECT STRUCTURE (File system operations only)
    # =========================================================================

    def setup_project_structure(self, domains: list[str]) -> dict[str, Any]:
        """
        Create or enhance DBT project folder structure

        Args:
            domains: List of domain names (e.g., ['sales', 'finance', 'customer360'])

        Returns:
            Dict with setup results:
            {
                'mode': 'new' or 'merge',
                'folders_created': [...],
                'existing_project': {...} (if merge mode)
            }
        """
        logger.info(f"Setting up project structure: mode={self.mode}, domains={domains}")

        if self.mode == "new":
            return self._create_new_project(domains)
        else:
            return self._enhance_existing_project(domains)

    def _create_new_project(self, domains: list[str]) -> dict[str, Any]:
        """Create fresh DBT project structure"""
        logger.info("Creating new DBT project structure...")

        folders_created = []

        # Create main directories
        main_dirs = [
            "models/staging",
            "models/intermediate",
            "models/marts",
            "macros",
            "tests/generic",
            "tests/singular",
            "docs",
        ]

        for dir_path in main_dirs:
            full_path = self.output_dir / dir_path
            full_path.mkdir(parents=True, exist_ok=True)
            folders_created.append(str(full_path))
            logger.debug(f"Created: {full_path}")

        # Create domain-specific folders
        for layer in ["staging", "intermediate", "marts"]:
            for domain in domains:
                domain_path = self.output_dir / "models" / layer / domain
                domain_path.mkdir(parents=True, exist_ok=True)
                folders_created.append(str(domain_path))
                logger.debug(f"Created domain folder: {domain_path}")

        logger.info(f"New project structure created: {len(folders_created)} folders")

        return {
            "mode": "new",
            "folders_created": folders_created,
            "output_dir": str(self.output_dir),
            "domains": domains,
        }

    def _enhance_existing_project(self, domains: list[str]) -> dict[str, Any]:
        """Add domain folders to existing DBT project"""
        logger.info("Enhancing existing DBT project with new domains...")

        folders_created = []

        # Only create domain folders in existing layers
        for layer in ["staging", "intermediate", "marts"]:
            for domain in domains:
                domain_path = self.output_dir / "models" / layer / domain

                if domain_path.exists():
                    logger.warning(f"Domain folder already exists: {domain_path}")
                else:
                    domain_path.mkdir(parents=True, exist_ok=True)
                    folders_created.append(str(domain_path))
                    logger.debug(f"Created domain folder: {domain_path}")

        logger.info(f"Existing project enhanced: {len(folders_created)} new folders")

        return {
            "mode": "merge",
            "folders_created": folders_created,
            "output_dir": str(self.output_dir),
            "domains": domains,
        }

    # =========================================================================
    # VALIDATION (Structure checks, no interpretation)
    # =========================================================================

    def validate_inputs(self) -> dict[str, Any]:
        """
        Check that all required input files exist

        Returns:
            Dict with validation results:
            {
                'valid': bool,
                'files': {'filename': bool, ...},
                'missing': [...]
            }
        """
        logger.info("Validating input files...")

        required_files = [
            "talend_extraction.json",
            "sql_queries.sql",
            "transformations.json",
            "context_to_dbt.yml",
            "extraction_summary.txt",
        ]

        files_status = {}
        missing_files = []

        for filename in required_files:
            exists = (self.input_dir / filename).exists()
            files_status[filename] = exists

            if not exists:
                missing_files.append(filename)
                logger.error(f"Missing required file: {filename}")

        valid = len(missing_files) == 0

        result = {
            "valid": valid,
            "files": files_status,
            "missing": missing_files,
            "input_dir": str(self.input_dir),
        }

        if valid:
            logger.info("✅ All input files present")
        else:
            logger.error(f"❌ Missing {len(missing_files)} required files")

        return result

    def validate_existing_project(self) -> dict[str, Any]:
        """
        Validate existing DBT project structure (for merge mode)

        Returns:
            Dict with validation results:
            {
                'valid': bool,
                'project_name': str,
                'existing_domains': {'staging': [...], 'intermediate': [...], 'marts': [...]},
                'error': str (if invalid)
            }
        """
        logger.info("Validating existing DBT project...")

        dbt_project_yml = self.output_dir / "dbt_project.yml"

        if not dbt_project_yml.exists():
            logger.error("Not a valid DBT project: missing dbt_project.yml")
            return {
                "valid": False,
                "error": "Missing dbt_project.yml",
                "project_path": str(self.output_dir),
            }

        # Load project metadata (just parse, no interpretation)
        with open(dbt_project_yml) as f:
            project_config = yaml.safe_load(f)

        project_name = project_config.get("name", "unknown")

        # List existing domain folders
        existing_domains = self._list_existing_domains()

        logger.info(f"✅ Valid DBT project: {project_name}")
        logger.info(f"   Existing domains: {sum(len(v) for v in existing_domains.values())} total")

        return {
            "valid": True,
            "project_name": project_name,
            "existing_domains": existing_domains,
            "project_path": str(self.output_dir),
        }

    def _list_existing_domains(self) -> dict[str, list[str]]:
        """List existing domain folders in each layer (no interpretation)"""
        domains = {}

        for layer in ["staging", "intermediate", "marts"]:
            layer_path = self.output_dir / "models" / layer

            if layer_path.exists() and layer_path.is_dir():
                # List all subdirectories (domains)
                domains[layer] = [
                    d.name
                    for d in layer_path.iterdir()
                    if d.is_dir() and not d.name.startswith("_")
                ]
            else:
                domains[layer] = []

        return domains

    # =========================================================================
    # BACKUP & MERGE (File operations only)
    # =========================================================================

    def backup_sources_yml(self) -> str | None:
        """
        Backup existing _sources.yml file before merge

        Returns:
            Path to backup file, or None if no existing file
        """
        sources_file = self.output_dir / "models" / "staging" / "_sources.yml"

        if not sources_file.exists():
            logger.info("No existing _sources.yml to backup")
            return None

        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        backup_file = sources_file.with_suffix(f".yml.backup_{timestamp}")

        shutil.copy(sources_file, backup_file)
        logger.info(f"Backed up _sources.yml to: {backup_file}")

        return str(backup_file)

    def load_existing_sources_yml(self) -> dict[str, Any] | None:
        """
        Load existing _sources.yml for merge operations

        Returns:
            Parsed YAML dict, or None if file doesn't exist
        """
        sources_file = self.output_dir / "models" / "staging" / "_sources.yml"

        if not sources_file.exists():
            logger.info("No existing _sources.yml found")
            return None

        with open(sources_file) as f:
            sources = yaml.safe_load(f)
            return sources if sources else None

    def save_sources_yml(self, sources_data: dict) -> str:
        """
        Save _sources.yml file

        Args:
            sources_data: Dict containing sources YAML structure

        Returns:
            Path to saved file
        """
        sources_file = self.output_dir / "models" / "staging" / "_sources.yml"
        sources_file.parent.mkdir(parents=True, exist_ok=True)

        with open(sources_file, "w") as f:
            yaml.dump(sources_data, f, sort_keys=False, indent=2)

        logger.info(f"Saved _sources.yml to: {sources_file}")
        return str(sources_file)

    # =========================================================================
    # METRICS CALCULATION (Pure math, no interpretation)
    # =========================================================================

    def calculate_migration_metrics(
        self, talend_jobs: int, dbt_models: int, complete_models: int
    ) -> dict[str, Any]:
        """
        Calculate migration quality metrics (pure math)

        Args:
            talend_jobs: Total Talend jobs migrated
            dbt_models: Total DBT models generated
            complete_models: DBT models without placeholders

        Returns:
            Dict with calculated metrics:
            {
                'total_talend_jobs': int,
                'total_dbt_models': int,
                'complete_models': int,
                'sql_coverage_pct': float,
                'model_reduction_pct': float,
                'consolidation_ratio': float
            }
        """
        logger.info("Calculating migration metrics...")

        # SQL coverage = (complete models / total models) * 100
        sql_coverage = round((complete_models / dbt_models) * 100, 2) if dbt_models > 0 else 0

        # Model reduction = ((talend_jobs - dbt_models) / talend_jobs) * 100
        model_reduction = (
            round(((talend_jobs - dbt_models) / talend_jobs) * 100, 2) if talend_jobs > 0 else 0
        )

        # Consolidation ratio = dbt_models / talend_jobs
        consolidation_ratio = round(dbt_models / talend_jobs, 3) if talend_jobs > 0 else 0

        metrics = {
            "total_talend_jobs": talend_jobs,
            "total_dbt_models": dbt_models,
            "complete_models": complete_models,
            "models_with_placeholders": dbt_models - complete_models,
            "sql_coverage_pct": sql_coverage,
            "model_reduction_pct": model_reduction,
            "consolidation_ratio": consolidation_ratio,
        }

        logger.info(
            f"Metrics calculated: SQL coverage={sql_coverage}%, Reduction={model_reduction}%"
        )

        return metrics

    def count_models_and_placeholders(self) -> dict[str, Any]:
        """
        Count SQL models and identify those with placeholders

        Returns:
            Dict with counts:
            {
                'total_models': int,
                'models_with_placeholders': int,
                'complete_models': int,
                'models_by_layer': {'staging': int, 'intermediate': int, 'marts': int}
            }
        """
        logger.info("Counting models and placeholders...")

        total_models = 0
        models_with_placeholders = 0
        models_by_layer: dict[str, int] = {"staging": 0, "intermediate": 0, "marts": 0}

        placeholder_markers = ["TODO", "PLACEHOLDER", "FIXME"]

        for sql_file in self.output_dir.rglob("models/**/*.sql"):
            total_models += 1

            # Determine layer
            if "staging" in sql_file.parts:
                models_by_layer["staging"] += 1
            elif "intermediate" in sql_file.parts:
                models_by_layer["intermediate"] += 1
            elif "marts" in sql_file.parts:
                models_by_layer["marts"] += 1

            # Check for placeholders
            content = sql_file.read_text()
            if any(marker in content for marker in placeholder_markers):
                models_with_placeholders += 1
                logger.debug(f"Placeholder found in: {sql_file}")

        complete_models = total_models - models_with_placeholders

        result: dict[str, Any] = {
            "total_models": total_models,
            "models_with_placeholders": models_with_placeholders,
            "complete_models": complete_models,
            "models_by_layer": models_by_layer,
        }

        logger.info(
            f"Counted {total_models} models: {complete_models} complete, {models_with_placeholders} with placeholders"
        )

        return result

    def count_sources_in_yml(self) -> int:
        """
        Count number of sources defined in _sources.yml

        Returns:
            Number of source tables defined
        """
        sources_file = self.output_dir / "models" / "staging" / "_sources.yml"

        if not sources_file.exists():
            logger.warning("No _sources.yml found")
            return 0

        with open(sources_file) as f:
            sources_data = yaml.safe_load(f)

        total_sources = 0
        for source_group in sources_data.get("sources", []):
            total_sources += len(source_group.get("tables", []))

        logger.info(f"Counted {total_sources} sources in _sources.yml")
        return total_sources

    # =========================================================================
    # POST-PROCESSING HELPERS
    # =========================================================================

    def fix_deprecated_test_syntax(self) -> dict[str, int]:
        """
        Fix deprecated DBT 1.10+ test syntax in schema YAML files

        Updates:
        - accepted_values to use 'arguments' property
        - relationships to use 'arguments' property

        Returns:
            Dict with counts of files processed and fixes applied
        """
        import re

        logger.info("Fixing deprecated test syntax for DBT 1.10+...")

        schema_files = [
            self.output_dir / "models" / "staging" / "_schema.yml",
            self.output_dir / "models" / "intermediate" / "_schema.yml",
            self.output_dir / "models" / "marts" / "_schema.yml",
        ]

        files_processed = 0
        fixes_applied = 0

        for filepath in schema_files:
            if not filepath.exists():
                logger.debug(f"Schema file not found: {filepath}")
                continue

            with open(filepath) as f:
                original_content = f.read()

            content = original_content

            # Fix accepted_values syntax
            content, count1 = re.subn(
                r"accepted_values:\n(\s+)values:",
                r"accepted_values:\n\1arguments:\n\1  values:",
                content,
            )

            # Fix relationships syntax
            content, count2 = re.subn(
                r"relationships:\n(\s+)to:", r"relationships:\n\1arguments:\n\1  to:", content
            )

            if content != original_content:
                with open(filepath, "w") as f:
                    f.write(content)
                files_processed += 1
                fixes_applied += count1 + count2
                logger.info(f"Fixed {count1 + count2} deprecated tests in {filepath.name}")

        logger.info(f"Processed {files_processed} files, applied {fixes_applied} fixes")

        return {"files_processed": files_processed, "fixes_applied": fixes_applied}

    def count_project_files(self) -> dict[str, int]:
        """
        Count files in DBT project for quality report

        Returns:
            Dict with file counts by type
        """
        logger.info("Counting project files...")

        counts = {
            "sql_models": len(list(self.output_dir.rglob("models/**/*.sql"))),
            "yaml_schemas": len(list(self.output_dir.rglob("models/**/*.yml"))),
            "tests": len(list(self.output_dir.rglob("tests/**/*.sql"))),
            "markdown_docs": len(list(self.output_dir.rglob("**/*.md"))),
            "total_files": 0,
        }

        # Count all relevant files
        counts["total_files"] = (
            counts["sql_models"]
            + counts["yaml_schemas"]
            + counts["tests"]
            + counts["markdown_docs"]
        )

        logger.info(
            f"File counts: {counts['sql_models']} SQL, {counts['yaml_schemas']} YAML, "
            f"{counts['tests']} tests, {counts['markdown_docs']} docs"
        )

        return counts

    def generate_quality_report(
        self,
        tool_versions: dict[str, str],
        sql_results: dict[str, Any],
        yaml_results: dict[str, Any],
        dbt_results: dict[str, Any],
    ) -> str:
        """
        Generate comprehensive quality report

        Args:
            tool_versions: Dict of tool names and versions
            sql_results: SQL linting results
            yaml_results: YAML validation results
            dbt_results: DBT parse results

        Returns:
            Path to generated report
        """
        logger.info("Generating quality report...")

        file_counts = self.count_project_files()
        report_path = self.output_dir / "QUALITY_REPORT.md"

        report = f"""# DBT Migration Quality Report

Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}

---

## Summary

**Tool Versions:**
- sqlfluff: {tool_versions.get('sqlfluff', 'N/A')}
- yamllint: {tool_versions.get('yamllint', 'N/A')}
- dbt: {tool_versions.get('dbt', 'N/A')}
- mdformat: {tool_versions.get('mdformat', 'N/A')}

**Files Processed:**
- SQL Models: {file_counts['sql_models']}
- YAML Schemas: {file_counts['yaml_schemas']}
- Tests: {file_counts['tests']}
- Documentation: {file_counts['markdown_docs']}
- **Total: {file_counts['total_files']}**

**Overall Status:** {'✅ PASSED' if sql_results.get('passed', False) and yaml_results.get('passed', False) and dbt_results.get('passed', False) else '⚠️ NEEDS ATTENTION'}

---

## SQL Quality Results

**Files Linted:** {sql_results.get('files_linted', 0)}
**Issues Auto-Fixed:** {sql_results.get('issues_fixed', 0)}
**Remaining Issues:** {sql_results.get('remaining_issues', 0)}

{"**Status:** ✅ All SQL files pass linting" if sql_results.get('passed', False) else f"**Status:** ⚠️ {sql_results.get('remaining_issues', 0)} issues require manual review"}

**Common Fixes Applied:**
- Layout issues (indentation, line length)
- Aliasing (explicit table aliases)
- Structure (ELSE NULL removal, join order)

---

## YAML Validation Results

**Files Validated:** {yaml_results.get('files_validated', 0)}
**Syntax Errors Fixed:** {yaml_results.get('errors_fixed', 0)}
**DBT 1.10 Test Updates:** {yaml_results.get('test_syntax_fixes', 0)}

{"**Status:** ✅ All YAML files valid" if yaml_results.get('passed', False) else f"**Status:** ⚠️ {yaml_results.get('errors_remaining', 0)} validation errors"}

**DBT 1.10 Compatibility:**
- Updated `accepted_values` syntax: {yaml_results.get('accepted_values_fixed', 0)}
- Updated `relationships` syntax: {yaml_results.get('relationships_fixed', 0)}

---

## DBT Compilation Results

**Parse Status:** {'✅ Success' if dbt_results.get('passed', False) else '❌ Failed'}

**Resources Found:**
- Models: {dbt_results.get('models', 0)}
- Tests: {dbt_results.get('tests', 0)}
- Sources: {dbt_results.get('sources', 0)}
- Macros: {dbt_results.get('macros', 0)}

**Warnings:** {dbt_results.get('warnings', 0)}

---

## Documentation Quality

**Markdown Files Formatted:** {file_counts['markdown_docs']}

**Formatting Applied:**
- Consistent heading styles
- Standardized table formatting
- Code block language tags
- Horizontal rule formatting

---

## Production Readiness Checklist

- [{'x' if sql_results.get('passed', False) else ' '}] SQL files formatted and linted
- [{'x' if yaml_results.get('passed', False) else ' '}] YAML files validated
- [{'x' if dbt_results.get('passed', False) else ' '}] DBT project parses successfully
- [{'x' if yaml_results.get('test_syntax_fixes', 0) > 0 else ' '}] All tests defined correctly (DBT 1.10+)
- [x] Documentation formatted
- [x] Quality report generated

---

## Next Steps

{'**✅ Project is production-ready!**' if sql_results.get('passed', False) and yaml_results.get('passed', False) and dbt_results.get('passed', False) else '**⚠️ Manual review required:**'}

{'' if sql_results.get('passed', False) and yaml_results.get('passed', False) and dbt_results.get('passed', False) else f'''
1. Review remaining SQL linting issues: {sql_results.get('remaining_issues', 0)}
2. Fix YAML validation errors: {yaml_results.get('errors_remaining', 0)}
3. Address DBT parse warnings: {dbt_results.get('warnings', 0)}
'''}

**Deploy to production:**
```bash
# Configure BigQuery credentials
export GCP_PROJECT_ID="your-project-id"

# Test compilation
dbt parse --no-partial-parse

# Run models
dbt run

# Run tests
dbt test

# Build everything
dbt build
```
"""

        with open(report_path, "w") as f:
            f.write(report)

        logger.info(f"Quality report generated: {report_path}")

        return str(report_path)

    # =========================================================================
    # UTILITY METHODS
    # =========================================================================

    def save_json(self, data: dict, filename: str, output_path: Path | None = None) -> str:
        """
        Save data as JSON file

        Args:
            data: Data to save
            filename: Output filename
            output_path: Optional custom output path (defaults to output_dir)

        Returns:
            Path to saved file
        """
        if output_path is None:
            output_path = self.output_dir

        file_path = Path(output_path) / filename
        file_path.parent.mkdir(parents=True, exist_ok=True)

        with open(file_path, "w") as f:
            json.dump(data, f, indent=2)

        logger.info(f"Saved JSON to: {file_path}")
        return str(file_path)

    def get_project_info(self) -> dict[str, Any]:
        """
        Get basic project information

        Returns:
            Dict with project info:
            {
                'mode': str,
                'input_dir': str,
                'output_dir': str,
                'input_exists': bool,
                'output_exists': bool
            }
        """
        return {
            "mode": self.mode,
            "input_dir": str(self.input_dir),
            "output_dir": str(self.output_dir),
            "input_exists": self.input_dir.exists(),
            "output_exists": self.output_dir.exists(),
        }


# =============================================================================
# CLI INTERFACE
# =============================================================================


def main():
    """Command-line interface for DBT Generator"""
    import argparse

    parser = argparse.ArgumentParser(
        description="DBT Generator - Python helpers for Talend-to-DBT migration"
    )

    subparsers = parser.add_subparsers(dest="command", help="Command to execute")

    # Load command
    load_parser = subparsers.add_parser("load", help="Load Talend data files")
    load_parser.add_argument("--input", required=True, help="Input directory with Talend data")
    load_parser.add_argument("--output", required=True, help="Output JSON file path")

    # Setup command
    setup_parser = subparsers.add_parser("setup", help="Setup project structure")
    setup_parser.add_argument("--input", required=True, help="Input directory")
    setup_parser.add_argument("--output", required=True, help="Output directory")
    setup_parser.add_argument(
        "--mode", choices=["new", "merge"], default="new", help="Migration mode"
    )
    setup_parser.add_argument("--domains", nargs="+", required=True, help="Domain names")

    # Validate command
    validate_parser = subparsers.add_parser("validate", help="Validate inputs or existing project")
    validate_parser.add_argument("--input", help="Input directory to validate")
    validate_parser.add_argument(
        "--output", help="Output directory (for existing project validation)"
    )
    validate_parser.add_argument(
        "--mode", choices=["inputs", "project"], default="inputs", help="What to validate"
    )

    # Backup command
    backup_parser = subparsers.add_parser("backup-sources", help="Backup _sources.yml")
    backup_parser.add_argument("--output", required=True, help="DBT project directory")

    # Metrics command
    metrics_parser = subparsers.add_parser("metrics", help="Calculate migration metrics")
    metrics_parser.add_argument("--project", required=True, help="DBT project directory")
    metrics_parser.add_argument("--talend-jobs", type=int, help="Total Talend jobs")
    metrics_parser.add_argument("--output", help="Output JSON file for metrics")

    # Count sources command
    count_sources_parser = subparsers.add_parser(
        "count-sources", help="Count sources in _sources.yml"
    )
    count_sources_parser.add_argument("--project", required=True, help="DBT project directory")

    # Fix test syntax command
    fix_tests_parser = subparsers.add_parser(
        "fix-test-syntax", help="Fix deprecated DBT 1.10+ test syntax"
    )
    fix_tests_parser.add_argument("--project", required=True, help="DBT project directory")

    # Quality report command
    quality_parser = subparsers.add_parser("quality-report", help="Generate quality report")
    quality_parser.add_argument("--project", required=True, help="DBT project directory")
    quality_parser.add_argument(
        "--tool-versions", required=True, help="JSON string with tool versions"
    )
    quality_parser.add_argument(
        "--sql-results", required=True, help="JSON string with SQL linting results"
    )
    quality_parser.add_argument(
        "--yaml-results", required=True, help="JSON string with YAML validation results"
    )
    quality_parser.add_argument(
        "--dbt-results", required=True, help="JSON string with DBT parse results"
    )

    # File count command
    file_count_parser = subparsers.add_parser("file-count", help="Count project files")
    file_count_parser.add_argument("--project", required=True, help="DBT project directory")

    args = parser.parse_args()

    if not args.command:
        parser.print_help()
        return

    # Execute command
    if args.command == "load":
        gen = DBTGenerator(input_dir=Path(args.input), output_dir=Path("/tmp"))
        data = gen.load_talend_data()

        with open(args.output, "w") as f:
            json.dump(data, f, indent=2)

        print(json.dumps({"status": "success", "output": args.output}))

    elif args.command == "setup":
        gen = DBTGenerator(input_dir=Path(args.input), output_dir=Path(args.output), mode=args.mode)
        result = gen.setup_project_structure(domains=args.domains)
        print(json.dumps(result, indent=2))

    elif args.command == "validate":
        if args.mode == "inputs":
            gen = DBTGenerator(input_dir=Path(args.input), output_dir=Path("/tmp"))
            result = gen.validate_inputs()
        else:
            gen = DBTGenerator(input_dir=Path("/tmp"), output_dir=Path(args.output))
            result = gen.validate_existing_project()

        print(json.dumps(result, indent=2))

    elif args.command == "backup-sources":
        gen = DBTGenerator(input_dir=Path("/tmp"), output_dir=Path(args.output))
        backup_path = gen.backup_sources_yml()
        print(json.dumps({"backup_path": backup_path}))

    elif args.command == "metrics":
        gen = DBTGenerator(input_dir=Path("/tmp"), output_dir=Path(args.project))

        # Count models and placeholders
        counts = gen.count_models_and_placeholders()

        # Calculate migration metrics if Talend job count provided
        if args.talend_jobs:
            total_models = int(counts["total_models"])
            complete = int(counts["complete_models"])
            metrics = gen.calculate_migration_metrics(
                talend_jobs=args.talend_jobs,
                dbt_models=total_models,
                complete_models=complete,
            )
            result = {**counts, **metrics}
        else:
            result = counts

        if args.output:
            with open(args.output, "w") as f:
                json.dump(result, f, indent=2)
            result["saved_to"] = args.output

        print(json.dumps(result, indent=2))

    elif args.command == "count-sources":
        gen = DBTGenerator(input_dir=Path("/tmp"), output_dir=Path(args.project))
        count = gen.count_sources_in_yml()
        print(count)

    elif args.command == "fix-test-syntax":
        gen = DBTGenerator(input_dir=Path("/tmp"), output_dir=Path(args.project))
        result = gen.fix_deprecated_test_syntax()
        print(json.dumps(result, indent=2))

    elif args.command == "file-count":
        gen = DBTGenerator(input_dir=Path("/tmp"), output_dir=Path(args.project))
        counts = gen.count_project_files()
        print(json.dumps(counts, indent=2))

    elif args.command == "quality-report":
        gen = DBTGenerator(input_dir=Path("/tmp"), output_dir=Path(args.project))

        # Parse JSON arguments
        tool_versions = json.loads(args.tool_versions)
        sql_results = json.loads(args.sql_results)
        yaml_results = json.loads(args.yaml_results)
        dbt_results = json.loads(args.dbt_results)

        # Generate report
        report_path = gen.generate_quality_report(
            tool_versions=tool_versions,
            sql_results=sql_results,
            yaml_results=yaml_results,
            dbt_results=dbt_results,
        )

        print(json.dumps({"report_path": report_path}))


if __name__ == "__main__":
    main()
