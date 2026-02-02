"""
Unit tests for file processor module, focusing on JSONL functionality.
"""

import pytest
import os
import sqlite3
from core.file_processor import (
    flatten_nested_dict,
    collect_all_jsonl_fields,
    convert_jsonl_to_sqlite,
    sanitize_table_name
)
from core.constants import NESTED_FIELD_DELIMITER, LIST_INDEX_DELIMITER


class TestFlattenNestedDict:
    """Tests for flatten_nested_dict function"""

    def test_simple_dict(self):
        """Test flattening a simple flat dictionary"""
        data = {"name": "John", "age": 30}
        result = flatten_nested_dict(data)
        assert result == {"name": "John", "age": 30}

    def test_nested_dict_two_levels(self):
        """Test flattening a dictionary with 2 levels of nesting"""
        data = {"user": {"name": "John", "age": 30}}
        result = flatten_nested_dict(data)
        assert result == {"user__name": "John", "user__age": 30}

    def test_nested_dict_three_levels(self):
        """Test flattening a dictionary with 3 levels of nesting"""
        data = {"user": {"address": {"city": "NYC", "zip": "10001"}}}
        result = flatten_nested_dict(data)
        assert result == {"user__address__city": "NYC", "user__address__zip": "10001"}

    def test_nested_dict_four_levels(self):
        """Test flattening a dictionary with 4+ levels of nesting"""
        data = {"a": {"b": {"c": {"d": "deep"}}}}
        result = flatten_nested_dict(data)
        assert result == {"a__b__c__d": "deep"}

    def test_list_of_primitives(self):
        """Test flattening lists with primitive values"""
        data = {"tags": ["python", "data", "ml"]}
        result = flatten_nested_dict(data)
        assert result == {"tags_0": "python", "tags_1": "data", "tags_2": "ml"}

    def test_list_of_dicts(self):
        """Test flattening lists containing dictionaries"""
        data = {"items": [{"id": 1, "name": "item1"}, {"id": 2, "name": "item2"}]}
        result = flatten_nested_dict(data)
        assert result == {
            "items_0__id": 1,
            "items_0__name": "item1",
            "items_1__id": 2,
            "items_1__name": "item2"
        }

    def test_empty_list(self):
        """Test flattening empty lists"""
        data = {"tags": []}
        result = flatten_nested_dict(data)
        assert result == {}

    def test_none_values(self):
        """Test handling of None values"""
        data = {"name": "John", "age": None}
        result = flatten_nested_dict(data)
        assert result == {"name": "John", "age": None}

    def test_mixed_types(self):
        """Test flattening with mixed data types"""
        data = {
            "name": "John",
            "age": 30,
            "active": True,
            "score": 95.5,
            "metadata": None
        }
        result = flatten_nested_dict(data)
        assert result == {
            "name": "John",
            "age": 30,
            "active": True,
            "score": 95.5,
            "metadata": None
        }

    def test_complex_nested_structure(self):
        """Test flattening complex nested structure with mixed types"""
        data = {
            "user": {
                "name": "John",
                "address": {"city": "NYC", "zip": "10001"},
                "tags": ["premium", "verified"]
            },
            "metadata": [{"key": "browser", "value": "chrome"}]
        }
        result = flatten_nested_dict(data)
        assert result == {
            "user__name": "John",
            "user__address__city": "NYC",
            "user__address__zip": "10001",
            "user__tags_0": "premium",
            "user__tags_1": "verified",
            "metadata_0__key": "browser",
            "metadata_0__value": "chrome"
        }


class TestCollectAllJsonlFields:
    """Tests for collect_all_jsonl_fields function"""

    def test_identical_schemas(self):
        """Test collecting fields when all records have identical schemas"""
        jsonl_content = b'{"name": "Alice", "age": 30}\n{"name": "Bob", "age": 25}'
        fields = collect_all_jsonl_fields(jsonl_content)
        assert fields == {"name", "age"}

    def test_varying_schemas(self):
        """Test collecting fields when records have different schemas"""
        jsonl_content = b'{"name": "Alice", "age": 30}\n{"name": "Bob", "city": "NYC"}\n{"age": 25, "country": "USA"}'
        fields = collect_all_jsonl_fields(jsonl_content)
        assert fields == {"name", "age", "city", "country"}

    def test_nested_objects(self):
        """Test collecting fields from nested objects"""
        jsonl_content = b'{"user": {"name": "Alice", "age": 30}}\n{"user": {"name": "Bob", "city": "NYC"}}'
        fields = collect_all_jsonl_fields(jsonl_content)
        assert fields == {"user__name", "user__age", "user__city"}

    def test_arrays(self):
        """Test collecting fields from arrays"""
        jsonl_content = b'{"tags": ["a", "b"]}\n{"tags": ["c", "d", "e"]}'
        fields = collect_all_jsonl_fields(jsonl_content)
        # Should have tags_0, tags_1 from first record and tags_0, tags_1, tags_2 from second
        assert "tags_0" in fields
        assert "tags_1" in fields
        assert "tags_2" in fields

    def test_malformed_json_line(self):
        """Test handling malformed JSON lines gracefully"""
        jsonl_content = b'{"name": "Alice"}\n{invalid json}\n{"name": "Bob"}'
        fields = collect_all_jsonl_fields(jsonl_content)
        # Should collect fields from valid lines only
        assert "name" in fields

    def test_empty_file(self):
        """Test handling empty JSONL file"""
        jsonl_content = b''
        with pytest.raises(ValueError, match="No valid JSON records"):
            collect_all_jsonl_fields(jsonl_content)

    def test_single_record(self):
        """Test collecting fields from single record"""
        jsonl_content = b'{"name": "Alice", "age": 30, "city": "NYC"}'
        fields = collect_all_jsonl_fields(jsonl_content)
        assert fields == {"name", "age", "city"}


class TestConvertJsonlToSqlite:
    """Tests for convert_jsonl_to_sqlite function"""

    def test_valid_jsonl_file(self):
        """Test converting valid JSONL file to SQLite"""
        jsonl_content = b'{"name": "Alice", "age": 30}\n{"name": "Bob", "age": 25}'
        result = convert_jsonl_to_sqlite(jsonl_content, "test_users")

        assert result["table_name"] == "test_users"
        assert result["row_count"] == 2
        assert "name" in result["schema"]
        assert "age" in result["schema"]
        assert len(result["sample_data"]) == 2

        # Cleanup
        conn = sqlite3.connect("db/database.db")
        conn.execute("DROP TABLE IF EXISTS test_users")
        conn.close()

    def test_nested_objects(self):
        """Test converting JSONL with nested objects"""
        jsonl_content = b'{"user": {"name": "Alice", "age": 30}}'
        result = convert_jsonl_to_sqlite(jsonl_content, "test_nested")

        assert result["table_name"] == "test_nested"
        assert result["row_count"] == 1
        # Check that nested fields are flattened
        assert any("user" in key and "name" in key for key in result["schema"].keys())

        # Cleanup
        conn = sqlite3.connect("db/database.db")
        conn.execute("DROP TABLE IF EXISTS test_nested")
        conn.close()

    def test_varying_schemas(self):
        """Test converting JSONL with varying schemas across records"""
        jsonl_content = b'{"name": "Alice", "age": 30}\n{"name": "Bob", "city": "NYC"}'
        result = convert_jsonl_to_sqlite(jsonl_content, "test_varying")

        assert result["table_name"] == "test_varying"
        assert result["row_count"] == 2
        # Should have all fields from both records
        assert "name" in result["schema"]
        assert "age" in result["schema"]
        assert "city" in result["schema"]

        # Cleanup
        conn = sqlite3.connect("db/database.db")
        conn.execute("DROP TABLE IF EXISTS test_varying")
        conn.close()

    def test_malformed_jsonl(self):
        """Test handling malformed JSONL"""
        jsonl_content = b'{"name": "Alice"}\n{invalid}\n{"name": "Bob"}'
        result = convert_jsonl_to_sqlite(jsonl_content, "test_malformed")

        # Should process valid lines only
        assert result["row_count"] == 2

        # Cleanup
        conn = sqlite3.connect("db/database.db")
        conn.execute("DROP TABLE IF EXISTS test_malformed")
        conn.close()

    def test_empty_jsonl(self):
        """Test handling empty JSONL file"""
        jsonl_content = b''
        with pytest.raises(Exception, match="Error converting JSONL"):
            convert_jsonl_to_sqlite(jsonl_content, "test_empty")

    def test_table_name_sanitization(self):
        """Test that table names are properly sanitized"""
        jsonl_content = b'{"name": "Alice"}'
        result = convert_jsonl_to_sqlite(jsonl_content, "test-table with spaces.jsonl")

        # Should sanitize to valid table name
        assert result["table_name"] == "test_table_with_spaces"

        # Cleanup
        conn = sqlite3.connect("db/database.db")
        conn.execute("DROP TABLE IF EXISTS test_table_with_spaces")
        conn.close()

    def test_column_name_sanitization(self):
        """Test that column names are properly sanitized"""
        jsonl_content = b'{"User Name": "Alice", "User-Age": 30}'
        result = convert_jsonl_to_sqlite(jsonl_content, "test_columns")

        # Column names should be lowercased and have spaces/hyphens replaced
        schema_keys = list(result["schema"].keys())
        assert "user_name" in schema_keys
        assert "user_age" in schema_keys

        # Cleanup
        conn = sqlite3.connect("db/database.db")
        conn.execute("DROP TABLE IF EXISTS test_columns")
        conn.close()

    def test_sql_injection_prevention(self):
        """Test that SQL injection attempts in field names are sanitized"""
        # Field names with SQL keywords/injection attempts
        jsonl_content = b'{"name": "Alice", "DROP TABLE": "should be safe"}'
        result = convert_jsonl_to_sqlite(jsonl_content, "test_injection")

        # Should successfully create table without executing malicious SQL
        assert result["table_name"] == "test_injection"
        assert result["row_count"] == 1

        # Cleanup
        conn = sqlite3.connect("db/database.db")
        conn.execute("DROP TABLE IF EXISTS test_injection")
        conn.close()

    def test_users_jsonl_file(self):
        """Test converting the users.jsonl test file"""
        test_file_path = os.path.join(os.path.dirname(__file__), "test_data", "users.jsonl")
        with open(test_file_path, "rb") as f:
            jsonl_content = f.read()

        result = convert_jsonl_to_sqlite(jsonl_content, "test_users_full")

        assert result["table_name"] == "test_users_full"
        assert result["row_count"] == 10
        # Check that nested fields are flattened
        assert any("address" in key for key in result["schema"].keys())
        assert any("tags" in key for key in result["schema"].keys())

        # Cleanup
        conn = sqlite3.connect("db/database.db")
        conn.execute("DROP TABLE IF EXISTS test_users_full")
        conn.close()

    def test_events_jsonl_file(self):
        """Test converting the events.jsonl test file"""
        test_file_path = os.path.join(os.path.dirname(__file__), "test_data", "events.jsonl")
        with open(test_file_path, "rb") as f:
            jsonl_content = f.read()

        result = convert_jsonl_to_sqlite(jsonl_content, "test_events_full")

        assert result["table_name"] == "test_events_full"
        assert result["row_count"] == 15
        # Check that deeply nested fields are flattened
        assert any("user" in key and "preferences" in key for key in result["schema"].keys())
        assert any("metadata" in key for key in result["schema"].keys())

        # Cleanup
        conn = sqlite3.connect("db/database.db")
        conn.execute("DROP TABLE IF EXISTS test_events_full")
        conn.close()


class TestSanitizeTableName:
    """Tests for sanitize_table_name function"""

    def test_remove_extension(self):
        """Test that file extensions are removed"""
        assert sanitize_table_name("users.jsonl") == "users"
        assert sanitize_table_name("data.csv") == "data"

    def test_replace_special_characters(self):
        """Test that special characters are replaced with underscores"""
        assert sanitize_table_name("test-table") == "test_table"
        assert sanitize_table_name("test table") == "test_table"
        assert sanitize_table_name("test@table") == "test_table"

    def test_starts_with_letter(self):
        """Test that table names starting with numbers get prefixed"""
        result = sanitize_table_name("123table")
        assert result.startswith("_")

    def test_empty_name(self):
        """Test that empty names get a default value or valid identifier"""
        result1 = sanitize_table_name("")
        assert result1.startswith("table")  # May be "table" or "table_<hash>"
        result2 = sanitize_table_name("...")
        # "..." becomes "___" after sanitization, which starts with underscore (valid)
        assert result2.startswith("_") or result2.startswith("table")
