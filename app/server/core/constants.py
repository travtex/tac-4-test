"""
Configuration constants for data processing.

This module defines constants used throughout the application for processing
and flattening nested data structures, particularly for JSONL file uploads.
"""

# Delimiter used to flatten nested dictionary keys
# Example: {"user": {"address": {"city": "NYC"}}} becomes {"user__address__city": "NYC"}
NESTED_FIELD_DELIMITER = "__"

# Delimiter used to index list items when flattening arrays
# Example: {"tags": ["python", "data"]} becomes {"tags_0": "python", "tags_1": "data"}
LIST_INDEX_DELIMITER = "_"
