import json
import pandas as pd
import sqlite3
import io
import re
import logging
from typing import Dict, Any, List, Set
from .sql_security import (
    execute_query_safely,
    validate_identifier,
    SQLSecurityError
)
from .constants import NESTED_FIELD_DELIMITER, LIST_INDEX_DELIMITER

logger = logging.getLogger(__name__)

def sanitize_table_name(table_name: str) -> str:
    """
    Sanitize table name for SQLite by removing/replacing bad characters
    and validating against SQL injection
    """
    # Remove file extension if present
    if '.' in table_name:
        table_name = table_name.rsplit('.', 1)[0]
    
    # Replace bad characters with underscores
    sanitized = re.sub(r'[^a-zA-Z0-9_]', '_', table_name)
    
    # Ensure it starts with a letter or underscore
    if sanitized and not sanitized[0].isalpha() and sanitized[0] != '_':
        sanitized = '_' + sanitized
    
    # Ensure it's not empty
    if not sanitized:
        sanitized = 'table'
    
    # Validate the sanitized name
    try:
        validate_identifier(sanitized, "table")
    except SQLSecurityError:
        # If validation fails, use a safe default
        sanitized = f"table_{hash(table_name) % 100000}"
    
    return sanitized

def convert_csv_to_sqlite(csv_content: bytes, table_name: str) -> Dict[str, Any]:
    """
    Convert CSV file content to SQLite table
    """
    try:
        # Sanitize table name
        table_name = sanitize_table_name(table_name)
        
        # Read CSV into pandas DataFrame
        df = pd.read_csv(io.BytesIO(csv_content))
        
        # Clean column names
        df.columns = [col.lower().replace(' ', '_').replace('-', '_') for col in df.columns]
        
        # Connect to SQLite database
        conn = sqlite3.connect("db/database.db")
        
        # Write DataFrame to SQLite
        df.to_sql(table_name, conn, if_exists='replace', index=False)
        
        # Get schema information using safe query execution
        cursor_info = execute_query_safely(
            conn,
            "PRAGMA table_info({table})",
            identifier_params={'table': table_name}
        )
        columns_info = cursor_info.fetchall()
        
        schema = {}
        for col in columns_info:
            schema[col[1]] = col[2]  # column_name: data_type
        
        # Get sample data using safe query execution
        cursor_sample = execute_query_safely(
            conn,
            "SELECT * FROM {table} LIMIT 5",
            identifier_params={'table': table_name}
        )
        sample_rows = cursor_sample.fetchall()
        column_names = [col[1] for col in columns_info]
        sample_data = [dict(zip(column_names, row)) for row in sample_rows]
        
        # Get row count using safe query execution
        cursor_count = execute_query_safely(
            conn,
            "SELECT COUNT(*) FROM {table}",
            identifier_params={'table': table_name}
        )
        row_count = cursor_count.fetchone()[0]
        
        conn.close()
        
        return {
            'table_name': table_name,
            'schema': schema,
            'row_count': row_count,
            'sample_data': sample_data
        }
        
    except Exception as e:
        raise Exception(f"Error converting CSV to SQLite: {str(e)}")

def convert_json_to_sqlite(json_content: bytes, table_name: str) -> Dict[str, Any]:
    """
    Convert JSON file content to SQLite table
    """
    try:
        # Sanitize table name
        table_name = sanitize_table_name(table_name)
        
        # Parse JSON
        data = json.loads(json_content.decode('utf-8'))
        
        # Ensure it's a list of objects
        if not isinstance(data, list):
            raise ValueError("JSON must be an array of objects")
        
        if not data:
            raise ValueError("JSON array is empty")
        
        # Convert to pandas DataFrame
        df = pd.DataFrame(data)
        
        # Clean column names
        df.columns = [col.lower().replace(' ', '_').replace('-', '_') for col in df.columns]
        
        # Connect to SQLite database
        conn = sqlite3.connect("db/database.db")
        
        # Write DataFrame to SQLite
        df.to_sql(table_name, conn, if_exists='replace', index=False)
        
        # Get schema information using safe query execution
        cursor_info = execute_query_safely(
            conn,
            "PRAGMA table_info({table})",
            identifier_params={'table': table_name}
        )
        columns_info = cursor_info.fetchall()
        
        schema = {}
        for col in columns_info:
            schema[col[1]] = col[2]  # column_name: data_type
        
        # Get sample data using safe query execution
        cursor_sample = execute_query_safely(
            conn,
            "SELECT * FROM {table} LIMIT 5",
            identifier_params={'table': table_name}
        )
        sample_rows = cursor_sample.fetchall()
        column_names = [col[1] for col in columns_info]
        sample_data = [dict(zip(column_names, row)) for row in sample_rows]
        
        # Get row count using safe query execution
        cursor_count = execute_query_safely(
            conn,
            "SELECT COUNT(*) FROM {table}",
            identifier_params={'table': table_name}
        )
        row_count = cursor_count.fetchone()[0]
        
        conn.close()
        
        return {
            'table_name': table_name,
            'schema': schema,
            'row_count': row_count,
            'sample_data': sample_data
        }
        
    except Exception as e:
        raise Exception(f"Error converting JSON to SQLite: {str(e)}")

def flatten_nested_dict(data: Dict[str, Any], parent_key: str = '', sep: str = NESTED_FIELD_DELIMITER) -> Dict[str, Any]:
    """
    Recursively flatten a nested dictionary into a single-level dictionary.

    Nested objects are flattened using the separator (default '__'):
        {"user": {"address": {"city": "NYC"}}} -> {"user__address__city": "NYC"}

    Lists are flattened with indexed keys using LIST_INDEX_DELIMITER:
        {"tags": ["python", "data"]} -> {"tags_0": "python", "tags_1": "data"}

    Args:
        data: Dictionary to flatten
        parent_key: Parent key prefix for recursive calls
        sep: Separator to use between nested keys

    Returns:
        Flattened dictionary with single-level keys

    Examples:
        >>> flatten_nested_dict({"user": {"name": "John", "age": 30}})
        {"user__name": "John", "user__age": 30}

        >>> flatten_nested_dict({"tags": ["a", "b", "c"]})
        {"tags_0": "a", "tags_1": "b", "tags_2": "c"}
    """
    items = []

    for k, v in data.items():
        new_key = f"{parent_key}{sep}{k}" if parent_key else k

        if isinstance(v, dict):
            # Recursively flatten nested dictionaries
            items.extend(flatten_nested_dict(v, new_key, sep=sep).items())
        elif isinstance(v, list):
            # Flatten lists with indexed keys
            for i, item in enumerate(v):
                list_key = f"{new_key}{LIST_INDEX_DELIMITER}{i}"
                if isinstance(item, dict):
                    # If list contains dicts, flatten them
                    items.extend(flatten_nested_dict(item, list_key, sep=sep).items())
                else:
                    # For primitive values, convert to string
                    items.append((list_key, item))
        elif v is None:
            # Preserve None values
            items.append((new_key, None))
        else:
            # Keep primitive values as-is
            items.append((new_key, v))

    return dict(items)

def collect_all_jsonl_fields(jsonl_content: bytes) -> Set[str]:
    """
    Read through entire JSONL file to discover all possible field names.

    Since JSONL records can have varying schemas, this function scans all
    records to collect the complete set of fields that will become table columns.

    Args:
        jsonl_content: Raw JSONL file content as bytes

    Returns:
        Set of all unique field names found across all records

    Raises:
        ValueError: If the file contains no valid JSON records
    """
    all_fields = set()
    lines_processed = 0
    valid_records = 0

    try:
        content = jsonl_content.decode('utf-8')
        lines = content.strip().split('\n')

        for line_num, line in enumerate(lines, 1):
            line = line.strip()
            if not line:
                continue

            lines_processed += 1
            try:
                record = json.loads(line)
                if isinstance(record, dict):
                    flattened = flatten_nested_dict(record)
                    all_fields.update(flattened.keys())
                    valid_records += 1
                else:
                    logger.warning(f"Line {line_num}: Expected JSON object, got {type(record).__name__}")
            except json.JSONDecodeError as e:
                logger.warning(f"Line {line_num}: Malformed JSON - {str(e)}")
                continue

        if valid_records == 0:
            raise ValueError("No valid JSON records found in JSONL file")

        logger.info(f"Processed {lines_processed} lines, found {valid_records} valid records with {len(all_fields)} unique fields")
        return all_fields

    except UnicodeDecodeError as e:
        raise ValueError(f"Unable to decode file content: {str(e)}")

def convert_jsonl_to_sqlite(jsonl_content: bytes, table_name: str) -> Dict[str, Any]:
    """
    Convert JSONL file content to SQLite table.

    JSONL (JSON Lines) format consists of one JSON object per line. This function:
    1. Scans all records to discover all possible fields (since schemas can vary)
    2. Flattens nested objects and arrays using configured delimiters
    3. Creates a pandas DataFrame with consistent schema
    4. Writes to SQLite with proper sanitization and security

    Args:
        jsonl_content: Raw JSONL file content as bytes
        table_name: Desired table name (will be sanitized)

    Returns:
        Dictionary containing:
            - table_name: Sanitized table name
            - schema: Column names and types
            - row_count: Number of records
            - sample_data: First 5 records

    Raises:
        Exception: If conversion fails (wraps underlying errors)

    Examples:
        Input JSONL:
            {"name": "Alice", "address": {"city": "NYC"}, "tags": ["a", "b"]}
            {"name": "Bob", "address": {"city": "LA"}}

        Output table columns:
            name, address__city, tags_0, tags_1
    """
    try:
        # Sanitize table name
        table_name = sanitize_table_name(table_name)

        # Collect all possible fields across all records
        all_fields = collect_all_jsonl_fields(jsonl_content)

        # Parse JSONL line-by-line and build records list
        records = []
        content = jsonl_content.decode('utf-8')
        lines = content.strip().split('\n')

        for line_num, line in enumerate(lines, 1):
            line = line.strip()
            if not line:
                continue

            try:
                record = json.loads(line)
                if isinstance(record, dict):
                    flattened = flatten_nested_dict(record)

                    # Ensure all fields are present (fill missing with None)
                    complete_record = {field: flattened.get(field, None) for field in all_fields}
                    records.append(complete_record)
            except json.JSONDecodeError:
                # Skip malformed lines (already logged in collect_all_jsonl_fields)
                continue

        if not records:
            raise ValueError("No valid records to process")

        # Create pandas DataFrame
        df = pd.DataFrame(records)

        # Clean column names (same pattern as CSV converter)
        df.columns = [col.lower().replace(' ', '_').replace('-', '_') for col in df.columns]

        # Connect to SQLite database
        conn = sqlite3.connect("db/database.db")

        # Write DataFrame to SQLite
        df.to_sql(table_name, conn, if_exists='replace', index=False)

        # Get schema information using safe query execution
        cursor_info = execute_query_safely(
            conn,
            "PRAGMA table_info({table})",
            identifier_params={'table': table_name}
        )
        columns_info = cursor_info.fetchall()

        schema = {}
        for col in columns_info:
            schema[col[1]] = col[2]  # column_name: data_type

        # Get sample data using safe query execution
        cursor_sample = execute_query_safely(
            conn,
            "SELECT * FROM {table} LIMIT 5",
            identifier_params={'table': table_name}
        )
        sample_rows = cursor_sample.fetchall()
        column_names = [col[1] for col in columns_info]
        sample_data = [dict(zip(column_names, row)) for row in sample_rows]

        # Get row count using safe query execution
        cursor_count = execute_query_safely(
            conn,
            "SELECT COUNT(*) FROM {table}",
            identifier_params={'table': table_name}
        )
        row_count = cursor_count.fetchone()[0]

        conn.close()

        return {
            'table_name': table_name,
            'schema': schema,
            'row_count': row_count,
            'sample_data': sample_data
        }

    except Exception as e:
        raise Exception(f"Error converting JSONL to SQLite: {str(e)}")