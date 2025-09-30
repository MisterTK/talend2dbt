#!/usr/bin/env python3
"""
Talend Parser - LLM Optimized
Mission: Extract Talend .item files into clean, context-friendly format for LLM consumption
Focus: Mmize tokens, maximize context, enable accurate DBT/BigQuery migrationini
"""

import json
import logging
import re
import xml.etree.ElementTree as ET
from pathlib import Path
from typing import Dict, List, Any, DefaultDict, Set
from dataclasses import dataclass
from collections import defaultdict
import hashlib
from datetime import datetime
import yaml
import tiktoken

# Configure logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

# ============================================================
# CONFIGURATION
# ============================================================

# Components to EXCLUDE (pre/post processing, minimal connection management)
# Note: Error components (tDie, tWarn, tLogRow) are now extracted for patterns
EXCLUDED_COMPONENTS = {
    "tPrejob",
    "tPostjob",
    "tPreJob",
    "tPostJob",
    # Note: Connection components now extracted for transaction detection
    # "tDBConnection",  # Now extracted for transaction patterns
    # "tDBClose",  # Now extracted for transaction patterns
    # "tDBCommit",  # Now extracted for transaction patterns
    # "tDBRollback",  # Now extracted for transaction patterns
    # "tRedshiftConnection",  # Now extracted for transaction patterns
    # "tRedshiftClose",  # Now extracted for transaction patterns
    # "tRedshiftCommit",  # Now extracted for transaction patterns
    # "tRedshiftRollback",  # Now extracted for transaction patterns
    # "tLogRow",  # Now extracted for audit patterns
    # "tWarn",  # Now extracted for warning patterns
    # "tDie",  # Now extracted for error patterns
    "tStatCatcher",
    "tJava",
    "tSystem",
    "tSleep",
    "tContextLoad",
}

# ============================================================
# DATA CLASSES
# ============================================================


@dataclass
class ExtractedSQL:
    """Clean SQL extraction for LLM consumption"""

    job_name: str
    component_name: str
    component_type: str
    sql_operation: str
    raw_sql: str
    cleaned_sql: str
    tables: List[str]
    context_variables: List[str]

    def to_llm_format(self) -> Dict:
        """Format for minimal token consumption"""
        return {
            "component": f"{self.component_name} ({self.component_type})",
            "operation": self.sql_operation,
            "tables": self.tables,
            "context_vars": self.context_variables,
            "sql": self.cleaned_sql,
        }


@dataclass
class TMapExpression:
    """Clean tMap extraction for LLM consumption"""

    job_name: str
    component_name: str
    input_column: str
    output_column: str
    expression: str
    data_type: str


# ============================================================
# MAIN PARSER CLASS
# ============================================================


class TalendParserLLMOptimized:
    """Extract Talend jobs into LLM-optimized format"""

    def __init__(self) -> None:
        # Token encoder for statistics
        self.token_encoder = tiktoken.get_encoding("cl100k_base")

        # Enhanced data type mapping from Talend to BigQuery
        self.type_mappings = {
            # String types
            "id_String": "STRING",
            "id_Character": "STRING",
            "id_Text": "STRING",
            "String": "STRING",
            # Numeric types with precision handling
            "id_Integer": "INT64",
            "id_Long": "INT64",
            "id_Short": "INT64",
            "id_Byte": "INT64",
            "id_BigDecimal": "NUMERIC",
            "id_Double": "FLOAT64",
            "id_Float": "FLOAT64",
            "Integer": "INT64",
            "Long": "INT64",
            "Double": "FLOAT64",
            # Date/Time types
            "id_Date": "DATETIME",
            "id_Timestamp": "TIMESTAMP",
            "Date": "DATE",
            # Boolean
            "id_Boolean": "BOOL",
            "Boolean": "BOOL",
            # Binary
            "id_byte[]": "BYTES",
            "id_Object": "JSON",
        }

        # Safe casting expressions for type conversions
        self.cast_expressions = {
            "STRING_TO_INT": "SAFE_CAST({column} AS INT64)",
            "STRING_TO_FLOAT": "SAFE_CAST({column} AS FLOAT64)",
            "STRING_TO_DATE": "SAFE.PARSE_DATE('%Y-%m-%d', {column})",
            "STRING_TO_DATETIME": "SAFE.PARSE_DATETIME('%Y-%m-%d %H:%M:%S', {column})",
            "STRING_TO_TIMESTAMP": "SAFE.PARSE_TIMESTAMP('%Y-%m-%d %H:%M:%S', {column})",
            "INT_TO_STRING": "CAST({column} AS STRING)",
            "DATE_TO_STRING": "FORMAT_DATE('%Y-%m-%d', {column})",
            "NUMERIC_WITH_SCALE": "CAST({column} AS NUMERIC({precision}, {scale}))",
        }

        # Error flow tracking for reject connections
        self.error_flows: Dict[str, Any] = {}
        self.audit_templates = {
            "reject_row": {
                "columns": [
                    "reject_reason",
                    "reject_timestamp",
                    "source_component",
                    "error_code",
                ],
                "model_suffix": "_rejects",
            },
            "data_quality": {
                "columns": [
                    "check_name",
                    "check_result",
                    "check_timestamp",
                    "row_count",
                ],
                "model_suffix": "_dq_checks",
            },
        }

        self.jobs_summary: Dict[str, Dict[str, Any]] = {}
        self.all_sql: List[ExtractedSQL] = []
        self.all_tmap: List[TMapExpression] = []
        self.all_tables: Set[str] = set()
        self.context_mappings: Dict[str, Dict[str, str]] = {}
        self.job_hierarchy: Dict[str, str] = {}

        # Enhanced extraction storage
        self.transaction_patterns: Dict[str, Dict[str, Any]] = {}
        self.error_patterns: Dict[str, List[Dict[str, Any]]] = {}
        self.data_quality_rules: Dict[str, Dict[str, List[Dict[str, Any]]]] = {}
        self.performance_hints: Dict[str, Dict[str, Any]] = {}
        self.connection_metadata: Dict[str, Dict[str, Dict[str, Any]]] = {}

    def parse_folder(self, folder_path: str) -> Dict:
        """Parse all Talend jobs in folder"""
        folder = Path(folder_path)
        item_files = list(folder.rglob("*.item"))

        if not item_files:
            logger.warning(f"No .item files found in {folder_path}")
            return {}

        logger.info(f"Processing {len(item_files)} Talend jobs")

        # Calculate source file statistics
        self.source_stats = self._calculate_file_stats(item_files, "Source")

        for item_file in item_files:
            self._process_job(item_file)

        # Post-processing
        self._deduplicate_sql()
        self._build_hierarchy()
        self._create_context_mappings()

        return self._create_llm_output()

    def _calculate_file_stats(self, files: List[Path], label: str) -> Dict[str, Any]:
        """Calculate size and token statistics for files"""
        stats = {
            "label": label,
            "file_count": len(files),
            "files": [],
            "total_size_bytes": 0,
            "total_tokens": 0,
        }

        for file_path in files:
            try:
                # Read file content
                content = file_path.read_text(encoding="utf-8", errors="ignore")
                size_bytes = len(content.encode("utf-8"))

                # Calculate tokens
                tokens = len(self.token_encoder.encode(content))

                stats["files"].append(
                    {
                        "name": file_path.name,
                        "path": str(file_path),
                        "size_bytes": size_bytes,
                        "size_kb": round(size_bytes / 1024, 2),
                        "tokens": tokens,
                    }
                )

                stats["total_size_bytes"] += size_bytes
                stats["total_tokens"] += tokens

            except Exception as e:
                logger.warning(f"Error calculating stats for {file_path}: {e}")

        stats["total_size_kb"] = round(stats["total_size_bytes"] / 1024, 2)
        stats["total_size_mb"] = round(stats["total_size_bytes"] / (1024 * 1024), 2)

        return stats

    def _process_job(self, file_path: Path) -> None:
        """Process single job file"""
        job_name = file_path.stem
        logger.info(f"Processing: {job_name}")

        try:
            tree = ET.parse(file_path)
            root = tree.getroot()

            # Initialize job summary
            self.jobs_summary[job_name] = {
                "components": 0,
                "sql_count": 0,
                "tmap_count": 0,
                "tables": set(),
                "has_child_jobs": False,
            }

            # Extract components
            for node in root.findall(".//node"):
                self._process_component(node, job_name)

            # Extract transaction patterns for this job
            transaction_info = self._detect_transaction_patterns(root, job_name)
            if transaction_info and transaction_info.get("transaction_groups"):
                self.transaction_patterns[job_name] = transaction_info

            # Extract performance hints for this job
            perf_hints = self._extract_performance_hints(root, job_name)
            if perf_hints:
                self.performance_hints[job_name] = perf_hints

            # Extract context variables
            for context in root.findall(".//context"):
                for param in context.findall(".//contextParameter"):
                    var_name = param.get("name", "")
                    var_type = param.get("type", "")
                    var_value = param.get("value", "")

                    if var_name:
                        self.context_mappings[var_name] = {
                            "type": var_type,
                            "default": var_value,
                            "dbt_mapping": self._map_to_dbt(var_name, var_type),
                        }

        except Exception as e:
            logger.error(f"Error processing {job_name}: {e}")

    def _process_component(self, node: ET.Element, job_name: str) -> None:
        """Process individual component with dependency tracking"""
        comp_type = node.get("componentName", "")
        comp_name = node.get("uniqueName", "")

        # Build component dependency graph
        if "dependency_graph" not in self.jobs_summary[job_name]:
            self.jobs_summary[job_name]["dependency_graph"] = {
                "components": {},
                "connections": [],
                "execution_order": [],
                "model_boundaries": [],
            }

        # Track component metadata
        self.jobs_summary[job_name]["dependency_graph"]["components"][comp_name] = {
            "type": comp_type,
            "inputs": [],
            "outputs": [],
            "position": {"x": node.get("posX", 0), "y": node.get("posY", 0)},
        }

        # Process connections to build dependency graph
        for elem_param in node.findall('.//elementParameter[@field="TABLE"]'):
            if elem_param.get("name") == "CONNECTION":
                for item in elem_param.findall(".//elementValue"):
                    connection = item.get("value", "")
                    conn_type = item.get("elementRef", "CONNECTION_TYPE")
                    if connection and connection != comp_name:
                        self.jobs_summary[job_name]["dependency_graph"][
                            "connections"
                        ].append(
                            {"from": comp_name, "to": connection, "type": conn_type}
                        )
                        # Track inputs/outputs
                        if conn_type in ["FLOW_MAIN", "FLOW"]:
                            self.jobs_summary[job_name]["dependency_graph"][
                                "components"
                            ][comp_name]["outputs"].append(connection)

        # Handle special components that need extraction before exclusion
        if comp_type in ["tDie", "tWarn", "tLogRow"]:
            self._extract_error_handling(node, job_name, comp_name, comp_type)
            # Don't count these as regular components
            return

        if comp_type in [
            "tDBConnection",
            "tDBClose",
            "tDBCommit",
            "tDBRollback",
            "tRedshiftConnection",
            "tRedshiftClose",
            "tRedshiftCommit",
            "tRedshiftRollback",
        ]:
            # Extract connection metadata but don't count as components
            conn_meta = self._extract_connection_metadata(node)
            if conn_meta:
                if job_name not in self.connection_metadata:
                    self.connection_metadata[job_name] = {}
                self.connection_metadata[job_name][comp_name] = conn_meta
            return

        # Skip remaining excluded components
        if comp_type in EXCLUDED_COMPONENTS:
            return

        self.jobs_summary[job_name]["components"] += 1

        # Check for child jobs
        if comp_type == "tRunJob":
            self.jobs_summary[job_name]["has_child_jobs"] = True

        # Extract SQL
        self._extract_sql(node, job_name, comp_name, comp_type)

        # Extract tMap
        if comp_type == "tMap":
            self._extract_tmap_enhanced(node, job_name, comp_name)

        # Extract data quality rules
        if comp_type in ["tSchemaComplianceCheck", "tFilterRow", "tAggregateRow"]:
            self._extract_data_quality_rules(node, job_name, comp_name, comp_type)

    def _extract_sql(
        self, node: ET.Element, job_name: str, comp_name: str, comp_type: str
    ) -> None:
        """Extract SQL with enhanced pattern detection"""
        sql_params = ["QUERY", "DBTABLE", "TABLE", "DBQUERY", "SQL_QUERY", "QUERY_BAND"]

        for param_name in sql_params:
            for elem in node.findall(f".//elementParameter[@name='{param_name}']"):
                sql_raw = elem.get("value", "")
                if sql_raw and sql_raw != '""' and sql_raw != "null":
                    # Clean SQL
                    sql_cleaned = self._clean_sql(sql_raw)

                    # Extract tables with improved regex
                    tables = self._extract_tables(sql_cleaned)

                    # Extract context variables
                    context_vars = self._extract_context_vars(sql_cleaned)

                    # Detect operation
                    operation = self._detect_operation(sql_cleaned)

                    # Create extraction
                    extraction = ExtractedSQL(
                        job_name=job_name,
                        component_name=comp_name,
                        component_type=comp_type,
                        sql_operation=operation,
                        raw_sql=sql_raw,
                        cleaned_sql=sql_cleaned,
                        tables=tables,
                        context_variables=context_vars,
                    )

                    self.all_sql.append(extraction)
                    self.jobs_summary[job_name]["sql_count"] += 1
                    self.jobs_summary[job_name]["tables"].update(tables)
                    self.all_tables.update(tables)

    def _extract_tmap_enhanced(
        self, node: ET.Element, job_name: str, comp_name: str
    ) -> None:
        """Enhanced tMap extraction with complete lookup patterns and priorities"""
        # Call original extraction for backward compatibility
        self._extract_tmap(node, job_name, comp_name)

        # Add enhanced lookup extraction
        tmap_structure = (
            self.jobs_summary[job_name].get("tmap_structures", {}).get(comp_name, {})
        )
        if tmap_structure:
            # Extract enhanced lookups with priorities
            lookups_enhanced = self._extract_enhanced_lookups(node)
            tmap_structure["lookups_enhanced"] = lookups_enhanced

            # Store back the enhanced structure
            self.jobs_summary[job_name]["tmap_structures"][comp_name] = tmap_structure

    def _extract_enhanced_lookups(self, node: ET.Element) -> List[Dict]:
        """Extract complete lookup patterns with priorities and fallback logic"""
        lookups_with_priority = []

        for node_data in node.findall(".//nodeData"):
            for idx, input_table in enumerate(node_data.findall(".//inputTables")):
                lookup_info = {
                    "table": input_table.get("name", ""),
                    "matchingMode": input_table.get("matchingMode", ""),  # ALL modes
                    "lookupMode": input_table.get("lookupMode", ""),
                    "innerJoin": input_table.get("innerJoin", "false") == "true",
                    "persistent": input_table.get("persistent", "false") == "true",
                    "priority": input_table.get(
                        "sequenceOrder", str(idx)
                    ),  # Order matters
                    "cacheSize": input_table.get("cacheSizeHint", ""),
                    "preloadLookup": input_table.get("preload", "false") == "true",
                    "globalMapKeysValues": {},
                    "expressionFilter": input_table.get("expressionFilter", ""),
                }

                # Extract join conditions with NULL handling
                for global_map in input_table.findall(".//globalMapKeysValues"):
                    key = global_map.get("key", "")
                    value = global_map.get("value", "")
                    null_option = global_map.get("nullOption", "")
                    lookup_info["globalMapKeysValues"][key] = {
                        "value": value,
                        "null_handling": null_option,
                    }

                # Extract fallback expressions
                fallback_expr = input_table.get("lookupFailureExpression", "")
                if fallback_expr:
                    lookup_info["fallback_expression"] = fallback_expr

                # Only add if it has lookup information
                if lookup_info["matchingMode"] or lookup_info["lookupMode"]:
                    lookups_with_priority.append(lookup_info)

        return lookups_with_priority

    def _extract_tmap(self, node: ET.Element, job_name: str, comp_name: str) -> None:
        """Extract tMap transformations with deep expression parsing"""

        # Initialize tMap structure with complete schemas
        tmap_structure: Dict[str, Any] = {
            "input_schemas": {},
            "output_schemas": {},
            "filters": [],
            "lookups": [],
            "variables": {},
            "reject_flows": [],
        }

        # Extract input and output schemas
        for metadata in node.findall(".//metadata[@connector='FLOW']"):
            table_name = metadata.get("name", "default")
            tmap_structure["input_schemas"][table_name] = []
            for column in metadata.findall(".//column"):
                tmap_structure["input_schemas"][table_name].append(
                    {
                        "name": column.get("name", ""),
                        "type": column.get("type", ""),
                        "nullable": column.get("nullable", "true") == "true",
                    }
                )

        for metadata in node.findall(".//metadata[@connector='OUTPUT']"):
            table_name = metadata.get("name", "default")
            tmap_structure["output_schemas"][table_name] = []
            for column in metadata.findall(".//column"):
                tmap_structure["output_schemas"][table_name].append(
                    {
                        "name": column.get("name", ""),
                        "type": column.get("type", ""),
                        "expression": column.get("expression", ""),
                    }
                )

        # Extract filters, lookups, and reject conditions
        for node_data in node.findall(".//nodeData"):
            # Extract filter expressions
            for filter_elem in node_data.findall(".//filterIncomingConnections"):
                filter_expr = filter_elem.get("value", "")
                if filter_expr and filter_expr != "true":
                    tmap_structure["filters"].append(
                        {"type": "input_filter", "expression": filter_expr}
                    )

            # Extract output filters
            for out_table in node_data.findall(".//outputTables"):
                filter_expr = out_table.get("expressionFilter", "")
                if filter_expr:
                    tmap_structure["filters"].append(
                        {
                            "type": "output_filter",
                            "table": out_table.get("name", ""),
                            "expression": filter_expr,
                        }
                    )

                # Check for reject flow
                if out_table.get("reject", "false") == "true":
                    tmap_structure["reject_flows"].append(out_table.get("name", ""))

            # Extract lookup conditions
            for input_table in node_data.findall(".//inputTables"):
                if input_table.get("matchingMode", "") == "UNIQUE_MATCH":
                    lookup_expr = input_table.get("lookupMode", "")
                    if lookup_expr:
                        tmap_structure["lookups"].append(
                            {
                                "table": input_table.get("name", ""),
                                "mode": lookup_expr,
                                "condition": input_table.get("expressionFilter", ""),
                            }
                        )

            # Extract variable definitions
            for var_table in node_data.findall(".//varTables"):
                for var in var_table.findall(".//mapperTableEntries"):
                    var_name = var.get("name", "")
                    var_expr = var.get("expression", "")
                    if var_name and var_expr:
                        tmap_structure["variables"][var_name] = {
                            "expression": var_expr,
                            "type": var.get("type", "id_String"),
                        }

        # Continue with existing extraction logic for expressions
        for metadata in node.findall(".//metadata"):
            for column in metadata.findall(".//column"):
                expression = column.get("expression", "")
                if expression and expression != column.get("name", ""):
                    tmap_expr = TMapExpression(
                        job_name=job_name,
                        component_name=comp_name,
                        input_column=column.get("originalDbColumnName", ""),
                        output_column=column.get("name", ""),
                        expression=expression,
                        data_type=column.get("type", ""),
                    )
                    self.all_tmap.append(tmap_expr)
                    self.jobs_summary[job_name]["tmap_count"] += 1

        # Parse node data for detailed mappings
        for node_data in node.findall(".//nodeData"):
            for var_table in node_data.findall(".//varTables"):
                for mapping in var_table.findall(".//mapperTableEntries"):
                    expression = mapping.get("expression", "")
                    name = mapping.get("name", "")
                    if expression and expression != name:
                        tmap_expr = TMapExpression(
                            job_name=job_name,
                            component_name=comp_name,
                            input_column=name,
                            output_column=name,
                            expression=expression,
                            data_type=mapping.get("type", ""),
                        )
                        self.all_tmap.append(tmap_expr)
                        self.jobs_summary[job_name]["tmap_count"] += 1

        # Store the enhanced tMap structure in job summary
        if "tmap_structures" not in self.jobs_summary[job_name]:
            self.jobs_summary[job_name]["tmap_structures"] = {}
        self.jobs_summary[job_name]["tmap_structures"][comp_name] = tmap_structure

    def _detect_transaction_patterns(self, root: ET.Element, job_name: str) -> Dict:
        """Detect transaction boundaries and atomic operation groups"""
        transaction_groups = []
        current_transaction = None
        components_by_name = {}

        # First pass: Build component index
        for node in root.findall(".//node"):
            comp_name = node.get("uniqueName", "")
            comp_type = node.get("componentName", "")
            components_by_name[comp_name] = {"type": comp_type, "node": node}

        # Second pass: Detect transaction boundaries
        for node in root.findall(".//node"):
            comp_type = node.get("componentName", "")
            comp_name = node.get("uniqueName", "")

            # Detect transaction start
            if comp_type in ["tDBConnection", "tRedshiftConnection"]:
                # Extract connection settings
                auto_commit = node.find(".//elementParameter[@name='AUTO_COMMIT']")
                use_batch = node.find(".//elementParameter[@name='USE_BATCH']")
                batch_size = node.find(".//elementParameter[@name='BATCH_SIZE']")

                current_transaction = {
                    "connection_component": comp_name,
                    "auto_commit": (
                        auto_commit.get("value", "true")
                        if auto_commit is not None
                        else "true"
                    ),
                    "use_batch": (
                        use_batch.get("value", "false")
                        if use_batch is not None
                        else "false"
                    ),
                    "batch_size": (
                        int(batch_size.get("value", "0"))
                        if batch_size is not None
                        else 0
                    ),
                    "components": [],
                    "tables_modified": set(),
                    "requires_atomicity": False,
                }

            # Track components within transaction
            elif current_transaction and comp_type in [
                "tDBInput",
                "tDBOutput",
                "tDBRow",
                "tRedshiftInput",
                "tRedshiftOutput",
                "tRedshiftRow",
            ]:
                # Get SQL operation
                sql_operation = self._detect_operation_from_component(node)
                tables = self._extract_tables_from_component(node)

                current_transaction["components"].append(
                    {
                        "name": comp_name,
                        "type": comp_type,
                        "operation": sql_operation,
                        "tables": tables,
                    }
                )

                # Mark as requiring atomicity if modifying data
                if sql_operation in ["INSERT", "UPDATE", "DELETE", "MERGE"]:
                    current_transaction["requires_atomicity"] = True
                    current_transaction["tables_modified"].update(tables)

            # Detect transaction end
            elif comp_type in [
                "tDBCommit",
                "tDBRollback",
                "tDBClose",
                "tRedshiftCommit",
                "tRedshiftRollback",
                "tRedshiftClose",
            ]:
                if current_transaction:
                    current_transaction["end_component"] = comp_name
                    current_transaction["transaction_type"] = (
                        "COMMIT"
                        if "Commit" in comp_type
                        else "ROLLBACK" if "Rollback" in comp_type else "CLOSE"
                    )
                    current_transaction["tables_modified"] = list(
                        current_transaction["tables_modified"]
                    )
                    transaction_groups.append(current_transaction)
                    current_transaction = None

        # Handle unclosed transaction
        if current_transaction:
            current_transaction["transaction_type"] = "IMPLICIT_COMMIT"
            current_transaction["tables_modified"] = list(
                current_transaction["tables_modified"]
            )
            transaction_groups.append(current_transaction)

        return {
            "transaction_groups": transaction_groups,
            "atomic_operations_required": len(
                [t for t in transaction_groups if t.get("requires_atomicity", False)]
            ),
            "tables_requiring_atomicity": (
                list(
                    set().union(
                        *[set(t.get("tables_modified", [])) for t in transaction_groups]
                    )
                )
                if transaction_groups
                else []
            ),
        }

    def _detect_operation_from_component(self, node: ET.Element) -> str:
        """Detect SQL operation type from component configuration"""
        comp_type = node.get("componentName", "")

        # Direct mapping for obvious components
        if "Input" in comp_type:
            return "SELECT"
        elif "Output" in comp_type:
            # Check for specific operation
            action = node.find(".//elementParameter[@name='DATA_ACTION']")
            if action is not None:
                action_value = action.get("value", "INSERT").upper()
                if "INSERT" in action_value:
                    return "INSERT"
                elif "UPDATE" in action_value:
                    return "UPDATE"
                elif "DELETE" in action_value:
                    return "DELETE"
                elif "UPSERT" in action_value or "MERGE" in action_value:
                    return "MERGE"
            return "INSERT"  # Default for output

        # Check for SQL in tDBRow
        if "Row" in comp_type:
            query = node.find(".//elementParameter[@name='QUERY']")
            if query is not None:
                sql = query.get("value", "")
                return self._detect_operation(sql)

        return "UNKNOWN"

    def _extract_tables_from_component(self, node: ET.Element) -> List[str]:
        """Extract table names from a component"""
        tables = []

        # Check for table parameter
        table_param = node.find(".//elementParameter[@name='TABLE']")
        if table_param is not None:
            table = table_param.get("value", "").strip('"')
            if table and table != "null":
                tables.append(table)

        # Check for DBTABLE parameter
        dbtable_param = node.find(".//elementParameter[@name='DBTABLE']")
        if dbtable_param is not None:
            table = dbtable_param.get("value", "").strip('"')
            if table and table != "null":
                tables.append(table)

        # Check for SQL query
        query_param = node.find(".//elementParameter[@name='QUERY']")
        if query_param is not None:
            sql = query_param.get("value", "")
            sql_tables = self._extract_tables(self._clean_sql(sql))
            tables.extend(sql_tables)

        return list(set(tables))  # Remove duplicates

    def _extract_error_handling(
        self, node: ET.Element, job_name: str, comp_name: str, comp_type: str
    ) -> None:
        """Extract error handling and recovery patterns"""
        if job_name not in self.error_patterns:
            self.error_patterns[job_name] = []

        if comp_type == "tDie":
            error_info = {
                "component": comp_name,
                "type": "fatal_error",
                "message": "",
                "code": "",
                "priority": 1,
                "triggers": [],
            }

            # Extract error message
            message_param = node.find(".//elementParameter[@name='MESSAGE']")
            if message_param is not None:
                error_info["message"] = message_param.get("value", "")

            # Extract error code
            code_param = node.find(".//elementParameter[@name='CODE']")
            if code_param is not None:
                error_info["code"] = code_param.get("value", "")

            # Extract priority
            priority_param = node.find(".//elementParameter[@name='PRIORITY']")
            if priority_param is not None:
                try:
                    error_info["priority"] = int(priority_param.get("value", "1"))
                except ValueError:
                    error_info["priority"] = 1

            # Find incoming connections for triggers
            for connection in node.findall(".//connection"):
                error_info["triggers"].append(
                    {
                        "source": connection.get("source", ""),
                        "label": connection.get("label", ""),
                        "connector": connection.get("connectorName", ""),
                    }
                )

            self.error_patterns[job_name].append(error_info)

        elif comp_type == "tWarn":
            warning_info = {
                "component": comp_name,
                "type": "warning",
                "message": "",
                "code": "",
                "continue_on_error": True,
            }

            # Extract warning message
            message_param = node.find(".//elementParameter[@name='MESSAGE']")
            if message_param is not None:
                warning_info["message"] = message_param.get("value", "")

            # Extract warning code
            code_param = node.find(".//elementParameter[@name='CODE']")
            if code_param is not None:
                warning_info["code"] = code_param.get("value", "")

            self.error_patterns[job_name].append(warning_info)

        elif comp_type == "tLogRow":
            # Extract logging patterns for audit trail
            log_info = {
                "component": comp_name,
                "type": "audit_log",
                "mode": "basic",
                "separator": "|",
                "print_header": False,
            }

            # Extract log mode
            mode_param = node.find(
                ".//elementParameter[@name='PRINT_CONTENT_WITH_LOG4J']"
            )
            if mode_param is not None:
                log_info["mode"] = (
                    "log4j" if mode_param.get("value", "false") == "true" else "basic"
                )

            # Extract separator
            separator_param = node.find(".//elementParameter[@name='FIELDSEPARATOR']")
            if separator_param is not None:
                log_info["separator"] = separator_param.get("value", "|")

            # Extract header printing
            header_param = node.find(".//elementParameter[@name='PRINT_HEADER']")
            if header_param is not None:
                log_info["print_header"] = header_param.get("value", "false") == "true"

            self.error_patterns[job_name].append(log_info)

    def _extract_connection_metadata(self, node: ET.Element) -> Dict:
        """Extract detailed connection configuration"""
        connection_meta = {
            "commit_mode": "auto",
            "batch_settings": {},
            "parallel_hints": {},
            "resource_limits": {},
        }

        # Extract commit settings
        commit_every = node.find(".//elementParameter[@name='COMMIT_EVERY']")
        if commit_every is not None:
            connection_meta["commit_mode"] = "batch"
            connection_meta["batch_settings"]["commit_interval"] = int(
                commit_every.get("value", "1000")
            )

        # Extract batch configuration
        use_batch = node.find(".//elementParameter[@name='USE_BATCH']")
        batch_size = node.find(".//elementParameter[@name='BATCH_SIZE']")
        if use_batch is not None and use_batch.get("value") == "true":
            connection_meta["batch_settings"]["enabled"] = True
            if batch_size is not None:
                connection_meta["batch_settings"]["size"] = int(
                    batch_size.get("value", "1000")
                )

        # Extract parallel execution hints
        parallel_exec = node.find(
            ".//elementParameter[@name='ENABLE_PARALLEL_EXECUTION']"
        )
        if parallel_exec is not None and parallel_exec.get("value") == "true":
            connection_meta["parallel_hints"]["enabled"] = True
            num_parallel = node.find(
                ".//elementParameter[@name='NUMBER_OF_PARALLEL_EXECUTORS']"
            )
            if num_parallel is not None:
                connection_meta["parallel_hints"]["threads"] = int(
                    num_parallel.get("value", "4")
                )

        # Extract resource limits
        for param in ["MAX_MEMORY", "TIMEOUT", "MAX_ROWS"]:
            elem = node.find(f".//elementParameter[@name='{param}']")
            if elem is not None:
                connection_meta["resource_limits"][param.lower()] = elem.get("value")

        # Extract auto-commit setting
        auto_commit = node.find(".//elementParameter[@name='AUTO_COMMIT']")
        if auto_commit is not None:
            connection_meta["auto_commit"] = auto_commit.get("value", "true") == "true"

        return connection_meta

    def _extract_data_quality_rules(
        self, node: ET.Element, job_name: str, comp_name: str, comp_type: str
    ) -> None:
        """Extract data quality and validation rules"""
        if job_name not in self.data_quality_rules:
            self.data_quality_rules[job_name] = {}

        dq_rules = []

        # For tSchemaComplianceCheck
        if comp_type == "tSchemaComplianceCheck":
            for rule in node.findall(".//elementParameter[@field='TABLE']"):
                if rule.get("name") == "SCHEMA_COLUMN":
                    for item in rule.findall(".//elementValue"):
                        dq_rules.append(
                            {
                                "type": "schema_compliance",
                                "column": (
                                    item.get("value", "")
                                    if "COLUMN" in item.get("elementRef", "")
                                    else ""
                                ),
                                "data_type": (
                                    item.get("value", "")
                                    if "TYPE" in item.get("elementRef", "")
                                    else ""
                                ),
                                "nullable": (
                                    item.get("value", "")
                                    if "NULL" in item.get("elementRef", "")
                                    else ""
                                ),
                            }
                        )

        # For tFilterRow (data validation)
        elif comp_type == "tFilterRow":
            conditions = node.find(
                ".//elementParameter[@field='TABLE'][@name='CONDITIONS']"
            )
            if conditions is not None:
                for condition in conditions.findall(".//elementValue"):
                    # Build condition from multiple elementValues
                    condition_info = {
                        "type": "row_filter",
                        "column": "",
                        "function": "",
                        "operator": "",
                        "value": "",
                    }
                    element_ref = condition.get("elementRef", "")
                    value = condition.get("value", "")

                    if "COLUMN" in element_ref:
                        condition_info["column"] = value
                    elif "FUNCTION" in element_ref:
                        condition_info["function"] = value
                    elif "OPERATOR" in element_ref:
                        condition_info["operator"] = value
                    elif "VALUE" in element_ref:
                        condition_info["value"] = value

                    if condition_info["column"]:  # Only add if we have a column
                        dq_rules.append(condition_info)

        # For tAggregateRow (aggregation validations)
        elif comp_type == "tAggregateRow":
            operations = node.find(
                ".//elementParameter[@field='TABLE'][@name='OPERATIONS']"
            )
            if operations is not None:
                for op in operations.findall(".//elementValue"):
                    agg_info = {
                        "type": "aggregation",
                        "output_column": "",
                        "function": "",
                        "input_column": "",
                        "ignore_null": "false",
                    }

                    element_ref = op.get("elementRef", "")
                    value = op.get("value", "")

                    if "OUTPUT" in element_ref:
                        agg_info["output_column"] = value
                    elif "FUNCTION" in element_ref:
                        agg_info["function"] = value
                    elif "INPUT" in element_ref:
                        agg_info["input_column"] = value
                    elif "NULL" in element_ref:
                        agg_info["ignore_null"] = value

                    if agg_info["function"]:  # Only add if we have a function
                        dq_rules.append(agg_info)

        # Store DQ rules if any were found
        if dq_rules:
            self.data_quality_rules[job_name][comp_name] = dq_rules

    def _extract_performance_hints(self, root: ET.Element, job_name: str) -> Dict:
        """Extract performance-related configurations"""
        perf_hints = {
            "memory_settings": {},
            "parallelism": {},
            "caching": {},
            "optimization_flags": [],
        }

        # Job-level performance settings
        for param in root.findall(".//elementParameter"):
            param_name = param.get("name", "")
            param_value = param.get("value", "")

            if param_name == "JOB_RUN_VM_ARGUMENTS":
                perf_hints["memory_settings"]["jvm_args"] = param_value
            elif param_name == "JOB_RUN_VM_ARGUMENTS_OPTION":
                perf_hints["memory_settings"]["custom_jvm"] = param_value == "true"
            elif param_name == "MULTI_THREAD_EXECUTION":
                perf_hints["parallelism"]["enabled"] = param_value == "true"
            elif param_name == "PARALLELIZE_UNIT_SIZE":
                try:
                    perf_hints["parallelism"]["unit_size"] = int(param_value)
                except ValueError:
                    perf_hints["parallelism"]["unit_size"] = 1
            elif param_name == "IMPLICIT_TCONTEXTLOAD":
                perf_hints["optimization_flags"].append("implicit_context_load")
            elif param_name == "UPDATE_COMPONENTS":
                perf_hints["optimization_flags"].append("auto_update_components")

        # Clean up empty sections
        if not perf_hints["memory_settings"]:
            del perf_hints["memory_settings"]
        if not perf_hints["parallelism"]:
            del perf_hints["parallelism"]
        if not perf_hints["caching"]:
            del perf_hints["caching"]
        if not perf_hints["optimization_flags"]:
            del perf_hints["optimization_flags"]

        return perf_hints if perf_hints else {}

    def _clean_sql(self, sql: str) -> str:
        """Clean SQL for readability"""
        sql = sql.strip('"').strip()
        sql = sql.replace('\\"', '"')
        sql = sql.replace("\\n", "\n")
        sql = sql.replace("\\t", "  ")

        # Handle Java concatenation
        sql = re.sub(r'"\s*\+\s*"', "", sql)

        # Clean up multiple spaces
        sql = re.sub(r"\s+", " ", sql)

        return sql.strip()

    def _extract_tables(self, sql: str) -> List[str]:
        """Extract table names with improved pattern matching"""
        tables: Set[str] = set()

        # First, extract tables from context-embedded patterns
        # Pattern: "+context.schema+".table_name or "${context.schema}.table_name"
        patterns = [
            # Handle "+context.schema+".table format
            r'"\+[^+]+\+"\s*\.\s*([a-zA-Z_][a-zA-Z0-9_]*)',
            # Handle ${context.schema}.table format
            r"\$\{[^}]+\}\s*\.\s*([a-zA-Z_][a-zA-Z0-9_]*)",
            # Handle schema.table format
            r"(?:FROM|JOIN|INTO|UPDATE|DELETE\s+FROM)\s+(?:\w+\.)?\s*([a-zA-Z_][a-zA-Z0-9_]*)",
            # Handle just table name
            r"(?:FROM|JOIN|INTO|UPDATE|DELETE)\s+([a-zA-Z_][a-zA-Z0-9_]*)",
        ]

        for pattern in patterns:
            matches = re.finditer(pattern, sql, re.IGNORECASE)
            for match in matches:
                table = match.group(1)
                # Filter out Talend component names
                if not re.match(r"^t[A-Z][a-zA-Z]+_\d+$", table):
                    # Filter out keywords that might match
                    if table.lower() not in [
                        "select",
                        "from",
                        "where",
                        "join",
                        "on",
                        "as",
                        "and",
                        "or",
                        "not",
                        "null",
                        "case",
                        "when",
                        "then",
                        "else",
                        "end",
                        "using",
                        "set",
                        "values",
                        "into",
                    ]:
                        tables.add(table.lower())

        # Special handling for explicit table names in specific contexts
        # Look for patterns like: FROM context_var.table_name
        context_table_pattern = r"(?:FROM|JOIN)\s+[^.]+\.([a-zA-Z_][a-zA-Z0-9_]*)"
        for match in re.finditer(context_table_pattern, sql, re.IGNORECASE):
            table = match.group(1)
            if not re.match(r"^t[A-Z]", table):
                tables.add(table.lower())

        return sorted(list(tables))

    def _extract_context_vars(self, sql: str) -> List[str]:
        """Extract context variables from SQL"""
        vars = set()

        # Pattern for ${context.var} or "+context.var+"
        patterns = [
            r"\$\{context\.([^}]+)\}",
            r'"\+context\.([^+]+)\+"',
            r"'\+context\.([^+]+)\+'",
        ]

        for pattern in patterns:
            matches = re.finditer(pattern, sql)
            for match in matches:
                vars.add(match.group(1))

        return sorted(list(vars))

    def _detect_operation(self, sql: str) -> str:
        """Detect SQL operation type with enhanced pattern recognition"""
        sql_upper = sql.upper().strip()

        # Check first keyword
        operations = [
            "SELECT",
            "INSERT",
            "UPDATE",
            "DELETE",
            "MERGE",
            "CREATE",
            "DROP",
            "ALTER",
            "TRUNCATE",
            "WITH",
            "BEGIN",
            "DECLARE",
            "EXEC",
            "EXECUTE",
        ]

        for op in operations:
            if sql_upper.startswith(op):
                # Special handling for WITH (CTE)
                if op == "WITH":
                    # Check what follows the CTE
                    if "INSERT" in sql_upper:
                        return "CTE_INSERT"
                    elif "UPDATE" in sql_upper:
                        return "CTE_UPDATE"
                    elif "DELETE" in sql_upper:
                        return "CTE_DELETE"
                    elif "MERGE" in sql_upper:
                        return "CTE_MERGE"
                    else:
                        return "CTE_SELECT"
                return op

        # Enhanced detection for special patterns
        # MERGE with USING clause
        if "MERGE" in sql_upper and "USING" in sql_upper:
            return "MERGE_USING"

        # Multi-statement blocks (BEGIN...END)
        if "BEGIN" in sql_upper and "END" in sql_upper:
            operations_found = []
            if "UPDATE" in sql_upper:
                operations_found.append("UPDATE")
            if "DELETE" in sql_upper:
                operations_found.append("DELETE")
            if "INSERT" in sql_upper:
                operations_found.append("INSERT")
            if "MERGE" in sql_upper:
                operations_found.append("MERGE")

            if len(operations_found) > 1:
                return "MULTI_STATEMENT"
            elif operations_found:
                return f"BLOCK_{operations_found[0]}"

        # Window functions detection
        if re.search(r"\bOVER\s*\(", sql_upper):
            if sql_upper.startswith("SELECT"):
                return "SELECT_WINDOW"
            else:
                return "WINDOW_FUNCTION"

        # COPY and UNLOAD (Redshift/Snowflake specific)
        if "UNLOAD" in sql_upper:
            return "UNLOAD"
        if "COPY" in sql_upper and ("FROM" in sql_upper or "TO" in sql_upper):
            return "COPY"

        # Dynamic SQL with context variables
        if re.search(r"context\.\w+", sql, re.IGNORECASE):
            return "DYNAMIC_SQL"

        return "UNKNOWN"

    def _map_to_dbt(self, var_name: str, var_type: str) -> str:
        """Map Talend context variable to DBT format with intelligent classification"""
        var_lower = var_name.lower()

        # Context variable classification patterns
        variable_patterns = {
            "schema": {
                "patterns": ["schema", "database", "dataset", "catalog"],
                "mapping": "target.dataset",
                "validation": "Must be a valid database/schema identifier",
            },
            "date_filter": {
                "patterns": [
                    "date",
                    "start_date",
                    "end_date",
                    "process_date",
                    "run_date",
                ],
                "mapping": f"var('{var_lower}')",
                "validation": "Must be a valid date format (YYYY-MM-DD)",
                "type_check": var_type == "Date",
            },
            "timestamp": {
                "patterns": ["timestamp", "created_at", "updated_at", "modified"],
                "mapping": f"var('{var_lower}')",
                "validation": "Must be a valid timestamp format",
            },
            "file_path": {
                "patterns": ["path", "file", "dir", "folder", "location"],
                "mapping": f"var('external_{var_lower}')",
                "validation": "Must be a valid file path",
            },
            "cloud_storage": {
                "patterns": ["s3", "bucket", "gcs", "azure", "blob"],
                "mapping": f"var('cloud_{var_lower}')",
                "validation": "Must be a valid cloud storage path",
            },
            "environment": {
                "patterns": ["env", "environment", "stage", "tier"],
                "mapping": "target.name",
                "validation": "Must match defined environments (dev, test, prod)",
            },
            "batch": {
                "patterns": ["batch", "job_id", "run_id", "execution"],
                "mapping": f"var('batch_{var_lower}')",
                "validation": "Must be a valid batch identifier",
            },
            "threshold": {
                "patterns": ["threshold", "limit", "max", "min", "count"],
                "mapping": f"var('{var_lower}')",
                "validation": "Must be a numeric value",
                "type_check": var_type in ["Integer", "Long", "Double"],
            },
        }

        # Track usage context for better classification
        if not hasattr(self, "context_usage"):
            self.context_usage: Dict[str, Dict[str, Any]] = {}

        # Classify variable based on patterns
        for category, config in variable_patterns.items():
            # Check type-specific match first
            if "type_check" in config and config["type_check"]:
                if var_name not in self.context_usage:
                    self.context_usage[var_name] = {
                        "category": category,
                        "validation": config["validation"],
                        "usage_count": 0,
                    }
                return config["mapping"]

            # Check pattern match
            for pattern in config["patterns"]:
                if pattern in var_lower:
                    if var_name not in self.context_usage:
                        self.context_usage[var_name] = {
                            "category": category,
                            "validation": config["validation"],
                            "usage_count": 0,
                        }
                    return config["mapping"]

        # Handle environment-specific mappings
        env_prefixes = ["dev_", "test_", "prod_", "qa_", "staging_"]
        for prefix in env_prefixes:
            if var_lower.startswith(prefix):
                clean_name = var_lower[len(prefix) :]
                return f"var('{{{{'dev' if target.name == 'dev' else 'prod'}}}}_{clean_name}')"

        # Default mapping with tracking
        if var_name not in self.context_usage:
            self.context_usage[var_name] = {
                "category": "generic",
                "validation": "No specific validation rules",
                "usage_count": 0,
            }

        return f"var('{var_lower}')"

    def _deduplicate_sql(self) -> None:
        """Deduplicate SQL by hash"""
        seen = set()
        unique_sql = []

        for sql_obj in self.all_sql:
            sql_hash = hashlib.md5(sql_obj.cleaned_sql.encode()).hexdigest()
            if sql_hash not in seen:
                seen.add(sql_hash)
                unique_sql.append(sql_obj)

        logger.info(f"Deduplicated SQL: {len(self.all_sql)} -> {len(unique_sql)}")
        self.all_sql = unique_sql

    def _build_hierarchy(self) -> None:
        """Determine job hierarchy"""
        for job_name in self.jobs_summary:
            if "grandmaster" in job_name.lower():
                self.job_hierarchy[job_name] = "orchestrator_top"
            elif "master" in job_name.lower():
                self.job_hierarchy[job_name] = "orchestrator_mid"
            elif "child" in job_name.lower():
                self.job_hierarchy[job_name] = "processor"
            elif self.jobs_summary[job_name]["has_child_jobs"]:
                self.job_hierarchy[job_name] = "orchestrator"
            else:
                self.job_hierarchy[job_name] = "processor"

    def _create_context_mappings(self) -> None:
        """Create clean context variable mappings"""
        for var_name, info in self.context_mappings.items():
            # Ensure clean DBT mapping
            if not info.get("dbt_mapping"):
                info["dbt_mapping"] = self._map_to_dbt(var_name, info.get("type", ""))

    def _create_llm_output(self) -> Dict:
        """Create enhanced LLM-optimized output structure with migration intelligence"""
        # Generate performance suggestions
        performance_suggestions = self._generate_performance_suggestions()

        # Create dependency graphs for jobs
        dependency_graphs = self._generate_dependency_graphs()

        # Generate validation queries
        validation_queries = self._generate_validation_queries()

        return {
            "extraction_metadata": {
                "timestamp": datetime.now().isoformat(),
                "total_jobs": len(self.jobs_summary),
                "total_sql_queries": len(self.all_sql),
                "total_tmap_expressions": len(self.all_tmap),
                "unique_tables": len(self.all_tables),
                "context_variables": len(self.context_mappings),
                "transaction_groups": sum(
                    len(tp.get("transaction_groups", []))
                    for tp in self.transaction_patterns.values()
                ),
                "error_patterns": sum(len(ep) for ep in self.error_patterns.values()),
                "data_quality_rules": sum(
                    len(dq)
                    for dq_job in self.data_quality_rules.values()
                    for dq in dq_job.values()
                ),
                "migration_version": "3.0",  # Updated version for enhanced extraction
                "source_platform": "Talend",
                "target_platform": "BigQuery",
                "framework_version": "talend2dbt-3.0.0",
            },
            "job_hierarchy": self.job_hierarchy,
            "jobs_summary": {
                job: {
                    "role": self.job_hierarchy.get(job, "unknown"),
                    "components": info["components"],
                    "sql_queries": info["sql_count"],
                    "transformations": info["tmap_count"],
                    "tables": sorted(list(info["tables"])),
                    "dependency_graph": info.get("dependency_graph", {}),
                    "tmap_structures": info.get(
                        "tmap_structures", {}
                    ),  # Include enhanced tMap
                    "complexity_score": self._calculate_complexity(info),
                    "has_transactions": job in self.transaction_patterns,
                    "has_error_handling": job in self.error_patterns,
                    "has_data_quality": job in self.data_quality_rules,
                }
                for job, info in self.jobs_summary.items()
            },
            "sql_queries": [sql.to_llm_format() for sql in self.all_sql],
            "transformations": [
                {
                    "job": tmap.job_name,
                    "component": tmap.component_name,
                    "mapping": f"{tmap.input_column or tmap.output_column} = {tmap.expression}",
                    "type": tmap.data_type,
                    "safe_cast": self._get_safe_cast(tmap.data_type),
                }
                for tmap in self.all_tmap
            ],
            "tables": {
                "all_tables": sorted(list(self.all_tables)),
                "table_operations": self._analyze_table_operations(),
            },
            "context_mappings": {
                var: {
                    "type": info["type"],
                    "dbt": info["dbt_mapping"],
                    "category": getattr(self, "context_usage", {})
                    .get(var, {})
                    .get("category", "generic"),
                    "validation": getattr(self, "context_usage", {})
                    .get(var, {})
                    .get("validation", ""),
                }
                for var, info in self.context_mappings.items()
            },
            "transaction_patterns": self.transaction_patterns,  # NEW: Transaction boundaries
            "error_patterns": self.error_patterns,  # NEW: Error handling patterns
            "data_quality_rules": self.data_quality_rules,  # NEW: DQ validation rules
            "performance_hints": self.performance_hints,  # NEW: Performance optimization hints
            "connection_metadata": self.connection_metadata,  # NEW: Connection configurations
            "performance_suggestions": performance_suggestions,
            "dependency_graphs": dependency_graphs,
            "validation_queries": validation_queries,
            "migration_recommendations": self._generate_migration_recommendations(),
        }

    def _calculate_complexity(self, job_info: Dict) -> int:
        """Calculate job complexity score"""
        score = 0
        score += job_info.get("components", 0) * 2
        score += job_info.get("sql_count", 0) * 3
        score += job_info.get("tmap_count", 0) * 5
        score += len(job_info.get("tables", [])) * 1
        return score

    def _get_safe_cast(self, data_type: str) -> str:
        """Get safe casting expression for data type"""
        if data_type in self.type_mappings:
            target_type = self.type_mappings[data_type]
            if target_type in ["INT64", "FLOAT64", "NUMERIC"]:
                return f"SAFE_CAST({{column}} AS {target_type})"
            elif target_type == "DATE":
                return "SAFE.PARSE_DATE('%Y-%m-%d', {column})"
            elif target_type in ["DATETIME", "TIMESTAMP"]:
                return "SAFE.PARSE_TIMESTAMP('%Y-%m-%d %H:%M:%S', {column})"
        return "{column}"

    def _generate_performance_suggestions(self) -> List[str]:
        """Generate performance optimization suggestions"""
        suggestions = []

        # Check for large SQL queries
        large_queries = [sql for sql in self.all_sql if len(sql.cleaned_sql) > 1000]
        if large_queries:
            suggestions.append(
                f"Consider breaking down {len(large_queries)} complex SQL queries into smaller CTEs"
            )

        # Check for multiple operations on same table
        table_ops = defaultdict(int)
        for sql in self.all_sql:
            for table in sql.tables:
                table_ops[table] += 1

        multi_ops_tables = [t for t, count in table_ops.items() if count > 3]
        if multi_ops_tables:
            tables_msg = (
                f"Tables {', '.join(multi_ops_tables[:3])} are accessed multiple times"
            )
            suggestions.append(f"{tables_msg} - consider staging models")

        # Check for missing partitioning hints
        if any(
            "date" in t.lower() or "timestamp" in t.lower() for t in self.all_tables
        ):
            suggestions.append(
                "Consider adding partitioning on date/timestamp columns for better performance"
            )

        # Check for complex transformations
        complex_tmaps = [tm for tm in self.all_tmap if len(tm.expression) > 100]
        if complex_tmaps:
            complex_msg = f"Found {len(complex_tmaps)} complex expressions"
            suggestions.append(
                f"{complex_msg} - consider using DBT macros for reusability"
            )

        return suggestions

    def _generate_dependency_graphs(self) -> Dict[str, Dict]:
        """Generate job dependency graphs"""
        graphs = {}
        for job_name, job_info in self.jobs_summary.items():
            if "dependency_graph" in job_info and job_info["dependency_graph"]:
                graphs[job_name] = job_info["dependency_graph"]
        return graphs

    def _generate_validation_queries(self) -> List[Dict]:
        """Generate validation queries for migration testing"""
        queries = []

        # Row count validations
        for table in list(self.all_tables)[:10]:  # Top 10 tables
            queries.append(
                {
                    "type": "row_count",
                    "table": table,
                    "query": f"SELECT COUNT(*) as row_count FROM {table}",
                    "description": f"Validate row count for {table}",
                }
            )

        # Data type validations
        for sql in self.all_sql[:5]:  # Sample queries
            if sql.sql_operation == "SELECT":
                queries.append(
                    {
                        "type": "schema_validation",
                        "component": sql.component_name,
                        "query": f"SELECT * FROM ({sql.cleaned_sql}) LIMIT 0",
                        "description": f"Validate schema for {sql.component_name}",
                    }
                )

        # Null checks for critical columns
        if self.all_tmap:
            sample_columns = list(
                set(tm.output_column for tm in self.all_tmap if tm.output_column)
            )[:5]
            for col in sample_columns:
                queries.append(
                    {
                        "type": "null_check",
                        "column": col,
                        "query": f"SELECT COUNT(*) FROM target_table WHERE {col} IS NULL",
                        "description": f"Check for nulls in {col}",
                    }
                )

        return queries

    def _generate_migration_recommendations(self) -> Dict[str, List[str]]:
        """Generate migration recommendations"""
        recommendations = {
            "pre_migration": [],
            "during_migration": [],
            "post_migration": [],
        }

        # Pre-migration recommendations
        if len(self.jobs_summary) > 10:
            recommendations["pre_migration"].append(
                "Consider phased migration approach for large job count"
            )
        if self.context_mappings:
            recommendations["pre_migration"].append(
                f"Document {len(self.context_mappings)} context variables and their mappings"
            )

        # During migration recommendations
        if any(j.get("has_child_jobs") for j in self.jobs_summary.values()):
            recommendations["during_migration"].append(
                "Maintain job orchestration hierarchy in DBT"
            )
        if self.error_flows:
            recommendations["during_migration"].append(
                "Implement error handling patterns using DBT tests"
            )

        # Post-migration recommendations
        recommendations["post_migration"].append(
            "Run all validation queries to ensure data integrity"
        )
        recommendations["post_migration"].append(
            "Set up monitoring for DBT model performance"
        )
        if len(self.all_tables) > 20:
            recommendations["post_migration"].append(
                "Create documentation for all migrated models"
            )

        return recommendations

    def _analyze_table_operations(self) -> Dict[str, Dict[str, Any]]:
        """Analyze operations per table"""
        table_ops: DefaultDict[str, Dict[str, Any]] = defaultdict(
            lambda: {"operations": set(), "components": set(), "jobs": set()}
        )

        for sql in self.all_sql:
            for table in sql.tables:
                table_ops[table]["operations"].add(sql.sql_operation)
                table_ops[table]["components"].add(sql.component_name)
                table_ops[table]["jobs"].add(sql.job_name)

        return {
            table: {
                "operations": sorted(list(info["operations"])),
                "used_in_jobs": sorted(list(info["jobs"])),
                "component_count": len(info["components"]),
            }
            for table, info in table_ops.items()
        }

    def write_outputs(self, output_dir: str) -> None:
        """Write LLM-optimized outputs"""
        output_path = Path(output_dir)
        output_path.mkdir(parents=True, exist_ok=True)

        # Get LLM output
        llm_output = self._create_llm_output()

        # 1. Write main JSON output (structured for LLM)
        with open(output_path / "talend_extraction.json", "w") as f:
            json.dump(llm_output, f, indent=2, default=str)

        # Track output files for statistics
        output_files = [output_path / "talend_extraction.json"]

        # 2. Write SQL file (clean, ready for LLM)
        with open(output_path / "sql_queries.sql", "w") as f:
            f.write("-- TALEND SQL EXTRACTION FOR LLM PROCESSING\n")
            f.write(f"-- Total Queries: {len(self.all_sql)}\n")
            f.write(f"-- Unique Tables: {len(self.all_tables)}\n")
            f.write("-- " + "=" * 50 + "\n\n")

            for i, sql in enumerate(self.all_sql, 1):
                f.write(f"-- Query {i}: {sql.job_name}.{sql.component_name}\n")
                f.write(f"-- Operation: {sql.sql_operation}\n")
                f.write(f"-- Tables: {', '.join(sql.tables)}\n")
                if sql.context_variables:
                    f.write(
                        f"-- Context Variables: {', '.join(sql.context_variables)}\n"
                    )
                f.write(f"\n{sql.cleaned_sql};\n\n")
                f.write("-- " + "-" * 50 + "\n\n")

        output_files.append(output_path / "sql_queries.sql")

        # 3. Write transformations file (tMap expressions)
        with open(output_path / "transformations.json", "w") as f:
            json.dump(
                {
                    "total_transformations": len(self.all_tmap),
                    "by_job": self._group_tmap_by_job(),
                },
                f,
                indent=2,
            )

        output_files.append(output_path / "transformations.json")

        # 4. Write context mappings YAML (for DBT reference)
        with open(output_path / "context_to_dbt.yml", "w") as f:
            yaml.dump(
                {"context_variable_mappings": llm_output["context_mappings"]},
                f,
                default_flow_style=False,
                sort_keys=False,
            )

        output_files.append(output_path / "context_to_dbt.yml")

        # 5. Write summary report
        with open(output_path / "extraction_summary.txt", "w") as f:
            f.write("TALEND EXTRACTION SUMMARY FOR LLM\n")
            f.write("=" * 50 + "\n\n")
            f.write(f"Jobs Processed: {len(self.jobs_summary)}\n")
            f.write(f"SQL Queries Extracted: {len(self.all_sql)}\n")
            f.write(f"Transformations (tMap): {len(self.all_tmap)}\n")
            f.write(f"Unique Tables: {len(self.all_tables)}\n")
            f.write(f"Context Variables: {len(self.context_mappings)}\n\n")

            f.write("JOB HIERARCHY:\n")
            f.write("-" * 30 + "\n")
            for job, role in sorted(self.job_hierarchy.items(), key=lambda x: x[1]):
                summary = self.jobs_summary[job]
                f.write(f"{job}: {role}\n")
                f.write(f"  Components: {summary['components']}, ")
                f.write(f"SQL: {summary['sql_count']}, ")
                f.write(f"tMap: {summary['tmap_count']}\n")

            f.write("\nTABLE ANALYSIS:\n")
            f.write("-" * 30 + "\n")
            table_ops = self._analyze_table_operations()
            for table in sorted(self.all_tables):
                ops = table_ops[table]
                f.write(f"{table}:\n")
                f.write(f"  Operations: {', '.join(ops['operations'])}\n")
                f.write(f"  Used in {len(ops['used_in_jobs'])} jobs\n")

        output_files.append(output_path / "extraction_summary.txt")

        # Calculate destination file statistics
        self.destination_stats = self._calculate_file_stats(output_files, "Destination")

        # Write token statistics report
        self._write_token_statistics(output_path)

        logger.info(f"LLM-optimized extraction complete. Output in: {output_dir}")

    def _write_token_statistics(self, output_path: Path) -> None:
        """Write comprehensive token and size statistics report"""
        stats_file = output_path / "token_statistics.txt"

        with open(stats_file, "w") as f:
            f.write("=" * 70 + "\n")
            f.write("FILE SIZE AND TOKEN STATISTICS\n")
            f.write("=" * 70 + "\n\n")

            # Source statistics
            f.write(f"SOURCE FILES ({self.source_stats['label']}):\n")
            f.write("-" * 70 + "\n")
            f.write(f"Total Files: {self.source_stats['file_count']}\n")
            f.write(
                f"Total Size: {self.source_stats['total_size_mb']:.2f} MB "
                f"({self.source_stats['total_size_kb']:.2f} KB)\n"
            )
            f.write(f"Total Tokens: {self.source_stats['total_tokens']:,}\n\n")

            # Top 10 largest source files
            source_files_sorted = sorted(
                self.source_stats["files"], key=lambda x: x["tokens"], reverse=True
            )
            f.write("Top 10 Largest Source Files:\n")
            for i, file_info in enumerate(source_files_sorted[:10], 1):
                f.write(f"  {i}. {file_info['name']}\n")
                f.write(
                    f"     Size: {file_info['size_kb']:.2f} KB | "
                    f"Tokens: {file_info['tokens']:,}\n"
                )
            f.write("\n")

            # Destination statistics
            f.write(f"DESTINATION FILES ({self.destination_stats['label']}):\n")
            f.write("-" * 70 + "\n")
            f.write(f"Total Files: {self.destination_stats['file_count']}\n")
            f.write(
                f"Total Size: {self.destination_stats['total_size_mb']:.2f} MB "
                f"({self.destination_stats['total_size_kb']:.2f} KB)\n"
            )
            f.write(f"Total Tokens: {self.destination_stats['total_tokens']:,}\n\n")

            # Destination file breakdown
            f.write("Destination Files:\n")
            for i, file_info in enumerate(self.destination_stats["files"], 1):
                f.write(f"  {i}. {file_info['name']}\n")
                f.write(
                    f"     Size: {file_info['size_kb']:.2f} KB | "
                    f"Tokens: {file_info['tokens']:,}\n"
                )
            f.write("\n")

            # Comparison and conversion value
            f.write("CONVERSION IMPACT:\n")
            f.write("-" * 70 + "\n")

            size_reduction = (
                self.source_stats["total_size_bytes"]
                - self.destination_stats["total_size_bytes"]
            )
            size_reduction_pct = (
                size_reduction / self.source_stats["total_size_bytes"]
            ) * 100

            token_reduction = (
                self.source_stats["total_tokens"]
                - self.destination_stats["total_tokens"]
            )
            token_reduction_pct = (
                token_reduction / self.source_stats["total_tokens"]
            ) * 100

            f.write(
                f"Size Reduction: {abs(size_reduction / (1024*1024)):.2f} MB "
                f"({size_reduction_pct:+.1f}%)\n"
            )
            f.write(
                f"Token Reduction: {abs(token_reduction):,} tokens "
                f"({token_reduction_pct:+.1f}%)\n\n"
            )

            if token_reduction > 0:
                f.write(
                    "✓ CONVERSION VALUE: Successfully condensed Talend XML into "
                    "LLM-optimized format\n"
                )
                f.write(f"  - {token_reduction:,} fewer tokens for LLM consumption\n")
                f.write(f"  - {size_reduction_pct:.1f}% reduction in data size\n")
            else:
                f.write("✓ CONVERSION VALUE: Enriched with structured metadata\n")
                f.write(
                    f"  - Added context and analysis for better LLM understanding\n"
                )

            f.write("\n" + "=" * 70 + "\n")

        logger.info(f"Token statistics written to: {stats_file}")

    def _group_tmap_by_job(self) -> Dict:
        """Group tMap expressions by job for clarity"""
        by_job = defaultdict(list)
        for tmap in self.all_tmap:
            by_job[tmap.job_name].append(
                {
                    "component": tmap.component_name,
                    "expression": f"{tmap.output_column} = {tmap.expression}",
                    "type": tmap.data_type,
                }
            )
        return dict(by_job)


# ============================================================
# MAIN EXECUTION
# ============================================================


def main() -> None:
    import sys

    if len(sys.argv) != 3:
        print(
            "Usage: python talend_parser_llm_optimized.py <input_folder> <output_dir>"
        )
        sys.exit(1)

    input_folder = sys.argv[1]
    output_dir = sys.argv[2]

    # Create parser
    parser = TalendParserLLMOptimized()

    # Parse jobs
    logger.info(f"Parsing Talend jobs from: {input_folder}")
    parser.parse_folder(input_folder)

    # Write outputs
    parser.write_outputs(output_dir)

    # Summary
    print("\n" + "=" * 60)
    print("LLM-OPTIMIZED EXTRACTION COMPLETE")
    print("=" * 60)
    print(f"Jobs Processed: {len(parser.jobs_summary)}")
    print(f"SQL Queries: {len(parser.all_sql)}")
    print(f"Transformations: {len(parser.all_tmap)}")
    print(f"Tables Found: {len(parser.all_tables)}")
    print(f"Context Variables: {len(parser.context_mappings)}")

    # Token statistics summary
    if hasattr(parser, "source_stats") and hasattr(parser, "destination_stats"):
        print("\nToken Statistics:")
        print(
            f"  Source: {parser.source_stats['total_tokens']:,} tokens "
            f"({parser.source_stats['total_size_mb']:.2f} MB)"
        )
        print(
            f"  Destination: {parser.destination_stats['total_tokens']:,} tokens "
            f"({parser.destination_stats['total_size_mb']:.2f} MB)"
        )
        token_reduction = (
            parser.source_stats["total_tokens"]
            - parser.destination_stats["total_tokens"]
        )
        token_reduction_pct = (
            token_reduction / parser.source_stats["total_tokens"]
        ) * 100
        print(
            f"  Reduction: {abs(token_reduction):,} tokens ({token_reduction_pct:+.1f}%)"
        )

    print("\nOutput Files:")
    print(f"  - {output_dir}/talend_extraction.json (main LLM input)")
    print(f"  - {output_dir}/sql_queries.sql (all SQL)")
    print(f"  - {output_dir}/transformations.json (tMap)")
    print(f"  - {output_dir}/context_to_dbt.yml (variable mappings)")
    print(f"  - {output_dir}/extraction_summary.txt (summary)")
    print(f"  - {output_dir}/token_statistics.txt (size & token analysis)")
    print("=" * 60)


if __name__ == "__main__":
    main()
