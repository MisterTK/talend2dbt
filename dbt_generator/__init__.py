"""
DBT Generator - Minimal Python helpers for Talend-to-DBT migration
Handles file operations, data loading, validation, and metrics calculation.
LLM interprets standards and generates SQL - Python handles the infrastructure.
"""

from .generator import DBTGenerator

__version__ = "1.0.0"
__all__ = ["DBTGenerator"]
