# ETL SQL Transformer - SQL Server to PostgreSQL Converter

## Overview

This project transforms SQL Server queries to PostgreSQL queries, with dynamic schema, table, and column name mapping based on DDL (Data Definition Language) files. It processes Excel files containing QC (Quality Control) rules and generates PostgreSQL-compatible SQL queries and INSERT statements.

## Key Features

- **SQL Server to PostgreSQL Transformation**: Converts SQL Server syntax to PostgreSQL syntax
- **Dynamic Schema Mapping**: Maps schema names using configurable dictionaries
- **DDL-Based Table/Column Mapping**: Automatically extracts table and column names from DDL files
- **Case-Insensitive to CamelCase Translation**: Converts lowercase table/column names to CamelCase as defined in DDL
- **Excel Processing**: Reads QC rules from Excel files and generates SQL INSERT statements
- **PostgreSQL Query Support**: Handles both SQL Server and PostgreSQL input queries

## Project Structure

```
ETL_rod787/
├── ETL_rod787/
│   ├── Program.cs                    # Main entry point
│   ├── Services/
│   │   ├── QCTransformator14.cs      # Base transformer class with DDL parsing
│   │   ├── QCTransformatorMSSQL.cs   # SQL Server to PostgreSQL transformer
│   │   ├── ExcelReader.cs            # Excel file reader
│   │   └── PostgresConn.cs          # PostgreSQL connection handler
│   └── ddl.sql                       # DDL file containing table definitions
└── README.md                         # This file
```

## Core Translation Rules

### 1. Schema Name Translation
- Maps source schema names to target schema names using `SchemaMap` dictionary
- Example: `{ "dataset_96269": "rod787" }`
- Configured in `Program.cs`

### 2. Table Name Translation
- **Rule**: `lowercase_table_name → CamelCaseTableName` (as defined in DDL)
- Tables are automatically extracted from DDL `CREATE TABLE` statements
- All table names are wrapped in double quotes for PostgreSQL: `"TableName"`

### 3. Column Name Translation
- **Rule**: `lowercase_column_name → camelCaseColumnName` (as defined in DDL)
- Columns are automatically extracted from DDL `CREATE TABLE` statements
- All column names are wrapped in double quotes for PostgreSQL: `"columnName"`
- Special case: `record_id` → `_id` (always translated)

### 4. SQL Syntax Transformations

#### SQL Server → PostgreSQL Conversions:
- `[identifier]` → `"identifier"` (brackets to double quotes)
- `dbo.schema` → `dataset_schema` (schema replacement)
- `ISNUMERIC(expr)` → `(expr ~ 'numeric-regex')` (for non-numeric columns)
- `GETDATE()` → `CURRENT_TIMESTAMP`
- `ISNULL(a, b)` → `COALESCE(a, b)`
- `TOP n` → `LIMIT n`
- `isdate(expr)` → PostgreSQL date validation CASE statement
- `REGEXP_MATCHES()` → `REGEXP_LIKE()`

#### Column Type-Aware Transformations:
- **Numeric columns**: Removes `ISNUMERIC` checks (redundant for numeric types)
- **Boolean columns**: Converts `'0'`/`'1'` to `false`/`true`
- **Numeric/Boolean columns**: Removes empty string checks (`= ''`, `<> ''`, `!= ''`)
- **Invalid CASTs**: Removes invalid type casts (e.g., `CAST(boolean AS NUMERIC)`)

## DDL Parsing

The DDL parser (`ParseDdl` method in `QCTransformator14.cs`) extracts:

1. **Table Names**: From `CREATE TABLE schema."TableName"` statements
2. **Column Names**: From column definitions within `CREATE TABLE` statements
3. **Column Types**: Normalized column types for type-aware transformations
4. **Foreign Keys**: From `ALTER TABLE ... FOREIGN KEY` statements

### DDL Format Expected:
```sql
CREATE TABLE rod787."MonitoringResult" (
    "season" text,
    "bathingWaterIdentifier" text,
    "sampleDate" date,
    ...
);
```

## Configuration

### Program.cs Configuration

```csharp
// Read DDL from file
string ddl = File.ReadAllText("ddl.sql");

// Define schema name mapping
var schemaMap = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase)
{
    { "dataset_96269", "rod787" }
};

// Initialize transformer
var transformer = new QCTransformatorMSSQL("path/to/excel.xlsx", ddl, schemaMap);
```

## Usage

### 1. Process Excel QC Rules

```csharp
var transformed = transformer.ProcessExpressionsFromExcel();
```

This processes Excel rows and transforms SQL expressions.

### 2. Generate INSERT Statements

```csharp
transformer.GenerateInsertStatements("output.txt");
```

Generates SQL INSERT statements for the `qc_rules_internal` table based on Excel data.

## Excel File Format

The Excel file should contain QC rules with the following columns:
- **Column A**: Table name (e.g., `MonitoringResult`)
- **Column B**: Column name (e.g., `season`)
- **Column C**: Rule code (e.g., `SV18`)
- **Column E**: Description (e.g., "Checks if the field is missing or empty")
- **Column F**: Expression (SQL query or pattern)
- **Column H**: Severity (e.g., `BLOCKER`)
- **Column I**: Additional context (for pattern extraction)

## Output Format

### Transformed SQL Queries
- All identifiers are properly quoted: `"TableName"`, `"columnName"`
- Schema names are translated: `rod787."MonitoringResult"`
- Column names are in CamelCase: `"season"`, `"bathingWaterIdentifier"`

### INSERT Statements
```sql
INSERT INTO rod787.qc_rules_internal 
(code, table_name, column_name, rule_level, operator_code, rule_param, severity, enabled) 
VALUES 
('SV18', 'MonitoringResult', 'season', 'COLUMN', 'SQL', '{"sql":"SELECT ..."}'::jsonb, 'BLOCKER', true);
```

## Rule Types

The transformer automatically detects rule types based on Excel descriptions:

1. **SQL**: Valid SQL queries → `operator_code = 'SQL'`, `rule_param = {"sql": "..."}`
2. **NOT_NULL_NOT_EMPTY**: Missing/empty checks → `operator_code = 'NOT_NULL_NOT_EMPTY'`, `rule_param = NULL`
3. **IS_DATE**: Date validation → `operator_code = 'IS_DATE'`, `rule_param = {"pattern": "[0-9]{4}-[0-9]{2}-[0-9]{2}"}`
4. **IS_DECIMAL**: Decimal validation → `operator_code = 'IS_DECIMAL'`, `rule_param = NULL`
5. **IS_INTEGER**: Integer validation → `operator_code = 'IS_INTEGER'`, `rule_param = NULL`
6. **FOREIGN_KEY**: Foreign key validation → `operator_code = 'FOREIGN_KEY'`, `rule_param = {"table": "...", "column": "...", "schema": "..."}`
7. **UNIQUE**: Unique constraint → `operator_code = 'UNIQUE'`, `rule_level = 'ROW'`, `rule_param = {"columns": [...]}`
8. **PATTERN**: Pattern matching → `operator_code = 'PATTERN'`, `rule_param = {"pattern": "..."}`

## Important Notes

### String Literal Protection
- **CRITICAL**: Regular expressions inside string literals are NEVER modified
- Single-quoted string literals are protected during all transformations
- This ensures regex patterns like `'^[0-9]{4}-[0-9]{2}-[0-9]{2}$'` remain intact

### Parameter Detection
- Queries containing parameter tokens (`{%...%}`, `@param`, `:param`, `$1`, `?`) are skipped
- String literals are excluded from parameter detection to avoid false positives
- PostgreSQL type casts (`::text`) are not treated as parameters

### CTE Support
- Common Table Expressions (CTEs) are properly recognized
- CTE names and columns are excluded from "unknown table/column" validation

## Dependencies

- **NPOI**: Excel file reading
- **Microsoft.SqlServer.TransactSql.ScriptDom**: SQL parsing
- **System.Text.Json**: JSON serialization for rule parameters

## Version Control

The project uses Git for version control. Key branches:
- `master`: Main branch
- `rod787`: Branch for rod787 dataset

## Troubleshooting

### Double Quotes Issue
If you see `""columnName""` instead of `"columnName"`:
- Check that column names aren't being processed twice
- Ensure negative lookbehind/lookahead patterns are working correctly

### Column Not Found
If columns aren't being recognized:
- Verify the column exists in the DDL file
- Check that the DDL parser extracted the column correctly
- Ensure column names match case-insensitively

### Table Not Found
If tables aren't being recognized:
- Verify the table exists in the DDL file
- Check that `CREATE TABLE` statements are properly formatted
- Ensure table names are extracted correctly by the parser

## License

[Add your license information here]
