# Feature: JSONL File Upload Support

## Feature Description
Add support for uploading JSONL (JSON Lines) files to the Natural Language SQL Interface application. JSONL files contain one JSON object per line, making them ideal for large datasets. The feature will parse JSONL files, automatically detect all possible fields by scanning the entire file, flatten nested objects and arrays using a configurable delimiter (`__`), and create a SQLite table just like existing CSV and JSON uploads. This enables users to work with complex, nested data structures and query them using natural language.

## User Story
As a data analyst
I want to upload JSONL files with nested data structures
So that I can query complex datasets using natural language without manual data preprocessing

## Problem Statement
The current application only supports CSV and JSON array uploads. Many modern data sources export in JSONL format (streaming logs, API exports, data pipelines), which consists of one JSON object per line rather than a single array. Additionally, nested JSON structures are not supported, limiting users who work with hierarchical or complex data. Users currently must manually convert JSONL to CSV/JSON arrays and flatten nested structures before uploading.

## Solution Statement
Implement a JSONL parser that reads files line-by-line, collects all possible fields across all records (since JSONL records can have varying schemas), and flattens nested objects and arrays into a single-level structure using a double-underscore delimiter (`__`). List items will be indexed using `_0`, `_1`, etc. pattern. The delimiter will be stored as a configurable constant for future flexibility. The solution will integrate seamlessly with the existing file upload flow, update the UI to indicate JSONL support, and include test JSONL files for validation.

## Relevant Files
Use these files to implement the feature:

- `app/server/server.py` (lines 72-109) - Main upload endpoint that validates file extensions and routes to conversion functions. Will need to add `.jsonl` to accepted extensions and route to new JSONL converter.

- `app/server/core/file_processor.py` (lines 1-174) - Contains `convert_csv_to_sqlite()` and `convert_json_to_sqlite()` functions. Will add new `convert_jsonl_to_sqlite()` function following the same pattern for schema extraction, table creation, and sample data generation.

- `app/client/index.html` (line 81) - File input accept attribute currently restricts to `.csv,.json`. Will add `.jsonl` to accepted file types.

- `app/client/src/main.ts` (line 80) - Drop zone description text mentions "csv or json files". Will update to include JSONL.

- `app/server/core/sql_security.py` - Security module for safe SQL operations. Will be used by the new JSONL converter to ensure secure table creation and queries.

### New Files

- `app/server/core/constants.py` - New file to store configuration constants including the field delimiter (`__`) and list index delimiter (`_`) for flattening nested structures. This allows easy modification of flattening behavior in the future.

- `app/server/tests/test_data/users.jsonl` - Sample JSONL test file with user records including nested objects (e.g., address, preferences) and arrays (e.g., tags, purchases) for comprehensive testing.

- `app/server/tests/test_data/events.jsonl` - Sample JSONL test file with event log records featuring deeply nested structures and varying schemas across records to test field collection and flattening logic.

- `app/server/tests/test_file_processor.py` - Unit tests for the new JSONL conversion functionality, including tests for field collection, nested object flattening, array indexing, empty files, malformed JSONL, and schema validation.

## Implementation Plan

### Phase 1: Foundation
Create the configuration constants file and implement the core JSONL parsing and flattening logic. This foundational work establishes the data transformation approach that will be used throughout the feature. The flattening algorithm must correctly handle arbitrary nesting levels, mixed data types, and varying schemas across JSONL records.

### Phase 2: Core Implementation
Implement the `convert_jsonl_to_sqlite()` function in the file processor module. This function will read JSONL line-by-line, collect all possible fields across all records, flatten each record using the established flattening logic, create a pandas DataFrame, and write it to SQLite following the same secure patterns as CSV and JSON converters. Include proper error handling for malformed JSONL and edge cases.

### Phase 3: Integration
Integrate JSONL support into the upload endpoint, update the frontend UI to communicate JSONL support to users, create comprehensive test files, and validate end-to-end functionality. Ensure security measures are applied consistently with existing file upload functionality.

## Step by Step Tasks
IMPORTANT: Execute every step in order, top to bottom.

### Create constants configuration file
- Create `app/server/core/constants.py` with field delimiter constant `NESTED_FIELD_DELIMITER = "__"`
- Add list index delimiter constant `LIST_INDEX_DELIMITER = "_"`
- Add docstring explaining the purpose of these constants for flattening nested JSON structures
- Include examples in comments showing how nested structures are flattened (e.g., `user.address.city` → `user__address__city`, `items[0]` → `items_0`)

### Implement JSONL flattening utility function
- Add `flatten_nested_dict()` helper function to `app/server/core/file_processor.py`
- Function should recursively flatten nested dictionaries using `NESTED_FIELD_DELIMITER`
- Handle lists by creating indexed keys using `LIST_INDEX_DELIMITER` pattern (e.g., `tags_0`, `tags_1`)
- Convert complex values (dicts, lists) to strings if they cannot be further flattened
- Handle None values and empty structures appropriately
- Add type hints and comprehensive docstring with examples

### Implement JSONL field collection function
- Add `collect_all_jsonl_fields()` helper function to `app/server/core/file_processor.py`
- Function reads through entire JSONL file to discover all possible fields across all records
- Parse each line as JSON, flatten it, and collect all unique field names
- Return a set of all field names found across all records
- Handle malformed lines gracefully (log warning and skip)
- Include error handling for invalid JSON and encoding issues

### Implement convert_jsonl_to_sqlite function
- Add `convert_jsonl_to_sqlite()` function to `app/server/core/file_processor.py` following same signature as CSV/JSON converters
- Accept `jsonl_content: bytes` and `table_name: str` parameters
- Sanitize table name using existing `sanitize_table_name()` function
- Call `collect_all_jsonl_fields()` to get complete field list
- Parse JSONL line-by-line, flatten each record, and build list of dictionaries
- Ensure all records have all fields (fill missing fields with None)
- Create pandas DataFrame from flattened records with consistent schema
- Clean column names using same pattern as CSV converter
- Write to SQLite using secure `execute_query_safely()` pattern
- Extract schema, sample data, and row count following existing converter patterns
- Return dictionary with `table_name`, `schema`, `row_count`, and `sample_data`
- Include comprehensive error handling with descriptive messages

### Update server upload endpoint
- Modify `app/server/server.py` line 77 to accept `.jsonl` extension: `if not file.filename.endswith(('.csv', '.json', '.jsonl')):`
- Update error message to include JSONL: `"Only .csv, .json, and .jsonl files are supported"`
- Add conditional routing for JSONL files around line 87-90
- Import `convert_jsonl_to_sqlite` from `core.file_processor`
- Route `.jsonl` files to `convert_jsonl_to_sqlite()` function
- Ensure logging captures JSONL file uploads with appropriate context

### Update frontend file input acceptance
- Modify `app/client/index.html` line 81 to accept JSONL files: `accept=".csv,.json,.jsonl"`
- Update drop zone text on line 80 in `app/client/src/main.ts` to: `"Drag and drop .csv, .json, or .jsonl files here"`
- Ensure file upload handler correctly processes JSONL file extensions

### Create test JSONL files
- Create `app/server/tests/test_data/` directory if it doesn't exist
- Create `app/server/tests/test_data/users.jsonl` with 10 user records
- Include nested objects: `{"name": "John", "address": {"city": "NYC", "zip": "10001"}, "tags": ["premium", "verified"]}`
- Include varying schemas across records (some with optional fields)
- Create `app/server/tests/test_data/events.jsonl` with 15 event records
- Include deeper nesting and arrays of objects: `{"event": "click", "user": {"id": 1, "prefs": {"theme": "dark"}}, "metadata": [{"key": "browser", "value": "chrome"}]}`
- Ensure files are valid JSONL format (one JSON object per line, no trailing commas)

### Create unit tests for JSONL functionality
- Create `app/server/tests/test_file_processor.py` if it doesn't exist
- Import pytest, the new constants, and conversion functions
- Test `flatten_nested_dict()` with various nested structures
- Test `flatten_nested_dict()` with arrays and verify indexing pattern
- Test `collect_all_jsonl_fields()` with varying schemas across records
- Test `convert_jsonl_to_sqlite()` with valid JSONL file
- Test `convert_jsonl_to_sqlite()` with malformed JSONL (should handle gracefully)
- Test `convert_jsonl_to_sqlite()` with empty file
- Test that flattened column names are properly sanitized and secure
- Verify schema, row count, and sample data are returned correctly

### Run validation commands
- Execute `cd app/server && uv run pytest` to ensure all tests pass with zero regressions
- Execute `./scripts/start.sh` to start both backend and frontend servers
- Manually test uploading `users.jsonl` via the UI and verify table creation
- Manually test uploading `events.jsonl` and verify complex nested structures are flattened correctly
- Test querying flattened fields using natural language (e.g., "show users where address__city is NYC")
- Verify sample data display shows flattened column names correctly
- Verify no security vulnerabilities with malicious field names in JSONL

## Testing Strategy

### Unit Tests
- Test `flatten_nested_dict()` with nested dictionaries (2, 3, 4+ levels deep)
- Test `flatten_nested_dict()` with arrays of primitives and arrays of objects
- Test `flatten_nested_dict()` with mixed types (strings, numbers, booleans, nulls)
- Test `collect_all_jsonl_fields()` returns complete field set from heterogeneous records
- Test `collect_all_jsonl_fields()` handles malformed lines without crashing
- Test `convert_jsonl_to_sqlite()` creates correct schema with all discovered fields
- Test `convert_jsonl_to_sqlite()` sanitizes table and column names properly
- Test `convert_jsonl_to_sqlite()` handles empty arrays and null values correctly
- Test field name collision scenarios (e.g., both `user_name` field and `user.name` nested field)

### Integration Tests
- Test end-to-end upload of JSONL file through API endpoint
- Test file upload with `.jsonl` extension is accepted and processed
- Test uploaded JSONL data is queryable via natural language interface
- Test schema endpoint returns correct information for JSONL-generated tables
- Test table deletion works for JSONL-generated tables
- Test JSONL upload with same filename replaces existing table
- Test security: JSONL with SQL injection attempts in field names is sanitized

### Edge Cases
- Empty JSONL file (0 records)
- JSONL file with single record
- JSONL file with 10,000+ records (performance test)
- JSONL with all records having identical schema vs all having different schemas
- JSONL with deeply nested objects (5+ levels)
- JSONL with very long array (100+ items)
- JSONL with unicode characters in field names and values
- JSONL with invalid JSON on some lines (partial file processing)
- JSONL with field names containing special characters
- JSONL with reserved SQL keywords as field names (should be sanitized)
- JSONL records where field type varies (string in one record, number in another)

## Acceptance Criteria
- Users can upload JSONL files via drag-and-drop or file browser with `.jsonl` extension
- UI clearly communicates that JSONL files are supported alongside CSV and JSON
- JSONL parser reads entire file to discover all possible fields across all records
- Nested objects are flattened using `__` delimiter (e.g., `address.city` becomes `address__city`)
- Array items are indexed using `_N` pattern (e.g., `tags[0]` becomes `tags_0`)
- Field delimiter `__` and list index delimiter `_` are stored as constants in `constants.py`
- JSONL upload creates a new SQLite table with all discovered fields as columns
- Records with missing fields have NULL values for those columns
- Uploaded JSONL data is immediately queryable via natural language interface
- Schema endpoint correctly displays flattened field names and types
- Sample data shows first 5 records with flattened structure
- Malformed JSONL lines are logged as warnings but don't crash the upload
- Field names are sanitized to prevent SQL injection
- Test JSONL files (`users.jsonl`, `events.jsonl`) exist in `tests/test_data/` directory
- All unit tests pass with 100% success rate
- No regressions in existing CSV and JSON upload functionality
- End-to-end manual testing confirms JSONL uploads work in production-like environment

## Validation Commands
Execute every command to validate the feature works correctly with zero regressions.

- `cd app/server && uv run pytest` - Run server tests to validate the feature works with zero regressions
- `cd app/server && uv run pytest tests/test_file_processor.py -v` - Run JSONL-specific tests with verbose output
- `./scripts/start.sh` - Start both backend and frontend for manual testing
- Manual test: Upload `app/server/tests/test_data/users.jsonl` via UI and verify table creation
- Manual test: Upload `app/server/tests/test_data/events.jsonl` via UI and verify nested fields are flattened
- Manual test: Query flattened fields using natural language (e.g., "show me all users where address__city is NYC")
- Manual test: Verify schema display shows correct flattened column names and types
- Manual test: Verify sample data displays correctly with flattened structure
- Manual test: Delete JSONL-generated table and confirm it's removed

## Notes
- JSONL format is line-delimited JSON where each line is a complete, valid JSON object
- Unlike standard JSON arrays, JSONL records can have heterogeneous schemas (different fields per record)
- The feature must scan the entire file first to discover all possible fields before creating the table schema
- Flattening deeply nested structures may result in very wide tables with many columns
- Consider memory usage for large JSONL files - pandas DataFrame creation may be memory-intensive for files with millions of records
- The `__` delimiter is chosen to be unlikely to conflict with existing field names, but collisions are theoretically possible
- Future enhancement: Allow users to configure the delimiter via UI settings
- Future enhancement: Support streaming JSONL processing for very large files (>1GB)
- Future enhancement: Add option to skip certain nested levels or filter fields during upload
- The flattening approach preserves all data but loses the hierarchical structure - queries must use flattened field names
- Standard library json module is sufficient for parsing - no need for external libraries like jsonlines
- Pandas is already a dependency for CSV processing, so it's appropriate to use for JSONL as well
