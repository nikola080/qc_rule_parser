using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using NPOI.SS.UserModel;
using NPOI.XSSF.UserModel;
using NPOI.HSSF.UserModel;
using System.Runtime.InteropServices.JavaScript;
using System.Text.Json.Nodes;
using Org.BouncyCastle.Bcpg.OpenPgp;
using System.Text.RegularExpressions;
using NPOI.HSSF.Record;
using NPOI.SS.Formula.PTG;
using Microsoft.SqlServer.TransactSql.ScriptDom;
using System.IO;

namespace ETL_rod787.Services
{
    public class CteBlock
    {
        public string Name { get; set; }
        public string Query { get; set; }
    }

    public class SqlAnalysisResult
    {
        public List<CteBlock> Ctes { get; set; } = new();
        public string[] UnknownTables { get; set; } = Array.Empty<string>();
        public string[] UnknownColumns { get; set; } = Array.Empty<string>();
        public string[] NonDatasetSchemas { get; set; } = Array.Empty<string>();
        public string[] AllTempColumns { get; set; } = Array.Empty<string>();
        public string[] TableNames { get; set; } = Array.Empty<string>();
        public string[] ParseErrors { get; set; } = Array.Empty<string>();
    }

    public class QCTransformator14
    {
        protected readonly IWorkbook? workbook;

        protected JsonArray qcRules = new();

        // Schema-specific dictionaries: schema name -> dictionaries
        protected Dictionary<string, Dictionary<string, string[]>> schemaTableColumns = 
            new Dictionary<string, Dictionary<string, string[]>>(StringComparer.OrdinalIgnoreCase);
        
        protected Dictionary<string, Dictionary<string, string>> schemaColumnNameMaps = 
            new Dictionary<string, Dictionary<string, string>>(StringComparer.OrdinalIgnoreCase);
        
        protected Dictionary<string, Dictionary<string, Dictionary<string, (string Schema, string Table, string Column)>>> schemaForeignKeyMaps = 
            new Dictionary<string, Dictionary<string, Dictionary<string, (string, string, string)>>>(StringComparer.OrdinalIgnoreCase);
        
        protected Dictionary<string, Dictionary<string, string>> schemaColumnTypes = 
            new Dictionary<string, Dictionary<string, string>>(StringComparer.OrdinalIgnoreCase);

        // Legacy single-schema dictionaries (for backward compatibility, populated from all schemas)
        protected Dictionary<string, string[]> table_columns = new Dictionary<string, string[]>(StringComparer.OrdinalIgnoreCase);

        private int expressionsStartRow;
        private int expressionsEndRow;

        /// <summary>
        /// Column name mapping to CamelCase format used in DDL (lowercase -> CamelCase)
        /// Merged from all schemas for backward compatibility
        /// </summary>
        protected Dictionary<string, string> ColumnNameMap = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);

        /// <summary>
        /// Foreign key mapping based on DDL schema
        /// Maps table -> column -> (schema, table, column)
        /// Merged from all schemas for backward compatibility
        /// </summary>
        protected Dictionary<string, Dictionary<string, (string Schema, string Table, string Column)>> ForeignKeyMap = 
            new Dictionary<string, Dictionary<string, (string, string, string)>>(StringComparer.OrdinalIgnoreCase);

        /// <summary>
        /// Schema name mapping (source schema -> target schema)
        /// Example: { "dataset_93286": "rod14_wise6" }
        /// </summary>
        protected Dictionary<string, string> SchemaMap = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);

        /// <summary>
        /// Column types dictionary (merged from all schemas for backward compatibility)
        /// </summary>
        protected Dictionary<string, string> _columnTypes = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);

        public QCTransformator14(string path, string ddl, int expressionsStartRow, int expressionsEndRow, Dictionary<string, string>? schemaMap = null)
        {
            try
            {
                using var stream = File.OpenRead(path);
                if (Path.GetExtension(path).Equals(".xls", System.StringComparison.OrdinalIgnoreCase))
                {
                    workbook = new HSSFWorkbook(stream);
                }
                else
                {
                    workbook = new XSSFWorkbook(stream);
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine("Error reading Excel file: " + ex.Message);
            }

            this.expressionsStartRow = expressionsStartRow;
            this.expressionsEndRow = expressionsEndRow;

            // Set schema mapping
            if (schemaMap != null)
            {
                SchemaMap = new Dictionary<string, string>(schemaMap, StringComparer.OrdinalIgnoreCase);
            }

            // Parse DDL to build dictionaries
            ParseDdl(ddl);
        }

        /// <summary>
        /// Parses DDL to extract table columns, column types, foreign keys, and column name mappings
        /// Supports multiple schemas in a single DDL file
        /// </summary>
        private void ParseDdl(string ddl)
        {
            if (string.IsNullOrWhiteSpace(ddl))
            {
                Console.WriteLine("Warning: DDL is empty, dictionaries will not be populated");
                return;
            }

            // Track current schema being parsed
            string currentSchema = null;
            string currentTable = null;
            var currentColumns = new List<string>();
            bool inTableDef = false;
            int parenDepth = 0;
            
            var lines = ddl.Split(new[] { '\r', '\n' }, StringSplitOptions.None);
            
            foreach (var line in lines)
            {
                var trimmed = line.Trim();
                if (string.IsNullOrWhiteSpace(trimmed) || trimmed.StartsWith("--"))
                {
                    if (inTableDef) continue; // Still in table definition
                    continue;
                }
                
                // Match CREATE SCHEMA to track schema boundaries
                var schemaMatch = Regex.Match(trimmed, @"CREATE\s+SCHEMA\s+(?:(\w+)\.)?(?:""([^""]+)""|(\w+))", RegexOptions.IgnoreCase);
                if (schemaMatch.Success)
                {
                    // Save previous table if any
                    if (currentSchema != null && currentTable != null && currentColumns.Count > 0)
                    {
                        SaveTableToSchema(currentSchema, currentTable, currentColumns);
                    }
                    
                    currentSchema = schemaMatch.Groups[2].Success ? schemaMatch.Groups[2].Value : schemaMatch.Groups[3].Value;
                    currentTable = null;
                    currentColumns = new List<string>();
                    inTableDef = false;
                    Console.WriteLine($"\nParsing schema: {currentSchema}");
                    continue;
                }
                
                // Match CREATE TABLE with optional schema prefix
                var createMatch = Regex.Match(trimmed, @"CREATE\s+TABLE\s+(?:(\w+)\.)?(?:""([^""]+)""|(\w+))", RegexOptions.IgnoreCase);
                if (createMatch.Success)
                {
                    // Save previous table
                    if (currentSchema != null && currentTable != null && currentColumns.Count > 0)
                    {
                        SaveTableToSchema(currentSchema, currentTable, currentColumns);
                    }
                    
                    // Use schema from CREATE TABLE if specified, otherwise use current schema
                    string tableSchema = createMatch.Groups[1].Success ? createMatch.Groups[1].Value : currentSchema;
                    if (tableSchema != null)
                    {
                        currentSchema = tableSchema;
                    }
                    
                    currentTable = createMatch.Groups[2].Success ? createMatch.Groups[2].Value : createMatch.Groups[3].Value;
                    currentColumns = new List<string>();
                    inTableDef = true;
                    parenDepth = CountChar(trimmed, '(') - CountChar(trimmed, ')');
                    continue;
                }
                
                if (!inTableDef) continue;
                
                // Track parentheses
                parenDepth += CountChar(trimmed, '(') - CountChar(trimmed, ')');
                
                // Check if table definition ended
                if (parenDepth <= 0 && trimmed.Contains(")"))
                {
                    inTableDef = false;
                    if (currentSchema != null && currentTable != null && currentColumns.Count > 0)
                    {
                        SaveTableToSchema(currentSchema, currentTable, currentColumns);
                    }
                    continue;
                }
                
                // Extract column: "columnName" type or columnName type
                if (trimmed.Contains("\"") && !trimmed.StartsWith("CONSTRAINT", StringComparison.OrdinalIgnoreCase))
                {
                    var colMatch = Regex.Match(trimmed, @"""?([^""\s,()]+)""?\s+(\w+(?:\([^)]+\))?)", RegexOptions.IgnoreCase);
                    if (colMatch.Success)
                    {
                        string colName = colMatch.Groups[1].Value.Trim();
                        string colType = colMatch.Groups[2].Value;
                        currentColumns.Add(colName);
                        
                        if (currentSchema != null)
                        {
                            // Store column type per schema
                            if (!schemaColumnTypes.ContainsKey(currentSchema))
                            {
                                schemaColumnTypes[currentSchema] = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
                            }
                            schemaColumnTypes[currentSchema][colName] = NormalizeColumnType(colType);
                            
                            // Store column name mapping per schema
                            if (!schemaColumnNameMaps.ContainsKey(currentSchema))
                            {
                                schemaColumnNameMaps[currentSchema] = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
                            }
                            schemaColumnNameMaps[currentSchema][colName.ToLowerInvariant()] = colName;
                            
                            // Also add to merged dictionary for backward compatibility
                            ColumnNameMap[colName.ToLowerInvariant()] = colName;
                        }
                    }
                }
                else if (!trimmed.StartsWith("CONSTRAINT", StringComparison.OrdinalIgnoreCase) && 
                         !trimmed.StartsWith("PRIMARY", StringComparison.OrdinalIgnoreCase))
                {
                    var colMatch = Regex.Match(trimmed, @"^([a-zA-Z_][a-zA-Z0-9_]*)\s+(\w+(?:\([^)]+\))?)", RegexOptions.IgnoreCase);
                    if (colMatch.Success)
                    {
                        string colName = colMatch.Groups[1].Value.Trim();
                        string colType = colMatch.Groups[2].Value;
                        currentColumns.Add(colName);
                        
                        if (currentSchema != null)
                        {
                            // Store column type per schema
                            if (!schemaColumnTypes.ContainsKey(currentSchema))
                            {
                                schemaColumnTypes[currentSchema] = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
                            }
                            schemaColumnTypes[currentSchema][colName] = NormalizeColumnType(colType);
                            
                            // Store column name mapping per schema
                            if (!schemaColumnNameMaps.ContainsKey(currentSchema))
                            {
                                schemaColumnNameMaps[currentSchema] = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
                            }
                            schemaColumnNameMaps[currentSchema][colName.ToLowerInvariant()] = colName;
                            
                            // Also add to merged dictionary for backward compatibility
                            ColumnNameMap[colName.ToLowerInvariant()] = colName;
                        }
                    }
                }
            }
            
            // Save last table
            if (currentSchema != null && currentTable != null && currentColumns.Count > 0)
            {
                SaveTableToSchema(currentSchema, currentTable, currentColumns);
            }
            
            int CountChar(string str, char ch) => str.Count(c => c == ch);
            
            // Print summary per schema
            Console.WriteLine($"\n=== DDL Parsing Summary ===");
            foreach (var schemaKvp in schemaTableColumns)
            {
                Console.WriteLine($"\nSchema: {schemaKvp.Key}");
                Console.WriteLine($"  Tables: {schemaKvp.Value.Count}");
                foreach (var tableKvp in schemaKvp.Value)
                {
                    Console.WriteLine($"    {tableKvp.Key}: {tableKvp.Value.Length} columns");
                }
            }
            
            // Print merged summary for backward compatibility
            Console.WriteLine($"\nTotal tables parsed (merged): {table_columns.Count}");
            Console.WriteLine($"Total columns (merged): {ColumnNameMap.Count}");

            // Parse ALTER TABLE ... ADD CONSTRAINT ... FOREIGN KEY statements (schema-aware)
            // Pattern: ALTER TABLE [schema.]"table" ADD CONSTRAINT name FOREIGN KEY ("column") REFERENCES [schema.]"table"("column")
            // Updated pattern to handle composite foreign keys: FOREIGN KEY ("col1",col2) REFERENCES ...
            var fkPattern = new Regex(@"ALTER\s+TABLE\s+(?:(\w+)\.)?(?:""([^""]+)""|(\w+))\s+ADD\s+CONSTRAINT\s+[^\s]+\s+FOREIGN\s+KEY\s*\(([^)]+)\)\s+REFERENCES\s+(?:(\w+)\.)?(?:""([^""]+)""|(\w+))\s*\(([^)]+)\)", RegexOptions.IgnoreCase);
            var fkMatches = fkPattern.Matches(ddl);

            foreach (Match fkMatch in fkMatches)
            {
                string sourceSchema = fkMatch.Groups[1].Success ? fkMatch.Groups[1].Value : "";
                string sourceTable = fkMatch.Groups[2].Success ? fkMatch.Groups[2].Value : fkMatch.Groups[3].Value;
                string sourceColumnsStr = fkMatch.Groups[4].Value.Trim(); // Can be "col1",col2 or col1,col2
                string targetSchema = fkMatch.Groups[5].Success ? fkMatch.Groups[5].Value : "";
                string targetTable = fkMatch.Groups[6].Success ? fkMatch.Groups[6].Value : fkMatch.Groups[7].Value;
                string targetColumnsStr = fkMatch.Groups[8].Value.Trim(); // Can be "col1",col2 or col1,col2
                
                // Extract first column from source (for single-column FKs, this is the only column)
                // Handle quoted and unquoted columns: "columnName" or columnName
                var sourceColMatch = Regex.Match(sourceColumnsStr, @"(?:""([^""]+)""|([a-zA-Z_][a-zA-Z0-9_]*))");
                var targetColMatch = Regex.Match(targetColumnsStr, @"(?:""([^""]+)""|([a-zA-Z_][a-zA-Z0-9_]*))");
                
                if (sourceColMatch.Success && targetColMatch.Success)
                {
                    string sourceColumn = sourceColMatch.Groups[1].Success ? sourceColMatch.Groups[1].Value : sourceColMatch.Groups[2].Value;
                    string targetColumn = targetColMatch.Groups[1].Success ? targetColMatch.Groups[1].Value : targetColMatch.Groups[2].Value;
                    
                    // Store foreign key per schema
                    if (!string.IsNullOrEmpty(sourceSchema))
                    {
                        if (!schemaForeignKeyMaps.ContainsKey(sourceSchema))
                        {
                            schemaForeignKeyMaps[sourceSchema] = new Dictionary<string, Dictionary<string, (string, string, string)>>(StringComparer.OrdinalIgnoreCase);
                        }
                        if (!schemaForeignKeyMaps[sourceSchema].ContainsKey(sourceTable))
                        {
                            schemaForeignKeyMaps[sourceSchema][sourceTable] = new Dictionary<string, (string, string, string)>(StringComparer.OrdinalIgnoreCase);
                        }
                        schemaForeignKeyMaps[sourceSchema][sourceTable][sourceColumn] = (targetSchema, targetTable, targetColumn);
                    }
                    
                    // Also store in merged dictionary for backward compatibility
                    if (!ForeignKeyMap.ContainsKey(sourceTable))
                    {
                        ForeignKeyMap[sourceTable] = new Dictionary<string, (string, string, string)>(StringComparer.OrdinalIgnoreCase);
                    }
                    ForeignKeyMap[sourceTable][sourceColumn] = (targetSchema, targetTable, targetColumn);
                }
            }

            // Store column types in a protected field for QCTransformatorMSSQL to access
            // Merge all schema column types into a single dictionary for backward compatibility
            _columnTypes = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
            foreach (var schemaTypes in schemaColumnTypes.Values)
            {
                foreach (var kvp in schemaTypes)
                {
                    _columnTypes[kvp.Key] = kvp.Value;
                }
            }
        }
        
        /// <summary>
        /// Helper method to save table information to schema-specific dictionaries
        /// </summary>
        private void SaveTableToSchema(string schema, string tableName, List<string> columns)
        {
            if (string.IsNullOrEmpty(schema)) return;
            
            // Initialize schema dictionaries if needed
            if (!schemaTableColumns.ContainsKey(schema))
            {
                schemaTableColumns[schema] = new Dictionary<string, string[]>(StringComparer.OrdinalIgnoreCase);
            }
            
            // Save table columns per schema
            schemaTableColumns[schema][tableName] = columns.ToArray();
            
            // Also save to merged dictionary for backward compatibility
            table_columns[tableName] = columns.ToArray();
        }

        /// <summary>
        /// Normalizes PostgreSQL column types to standard format
        /// </summary>
        private string NormalizeColumnType(string type)
        {
            string typeLower = type.ToLowerInvariant();
            
            if (typeLower.StartsWith("numeric") || typeLower.StartsWith("decimal"))
            {
                return "numeric";
            }
            if (typeLower.StartsWith("int4") || typeLower == "int" || typeLower == "integer")
            {
                return "int4";
            }
            if (typeLower == "bool" || typeLower == "boolean")
            {
                return "bool";
            }
            if (typeLower == "date")
            {
                return "date";
            }
            if (typeLower == "text" || typeLower.StartsWith("varchar") || typeLower.StartsWith("char"))
            {
                return "text";
            }
            if (typeLower == "uuid")
            {
                return "uuid";
            }
            
            return typeLower;
        }

        // visitor to collect references across script
        private class ReferenceCollector : TSqlFragmentVisitor
        {
            public HashSet<string> Tables { get; } = new(StringComparer.OrdinalIgnoreCase);
            public HashSet<string> Columns { get; } = new(StringComparer.OrdinalIgnoreCase);
            public HashSet<string> Schemas { get; } = new(StringComparer.OrdinalIgnoreCase);
            public Dictionary<string,string> Aliases { get; } = new(StringComparer.OrdinalIgnoreCase);

            public override void Visit(NamedTableReference node)
            {
                if (node == null) return;
                if (node.SchemaObject != null)
                {
                    var ids = node.SchemaObject.Identifiers;
                    if (ids != null && ids.Count > 0)
                    {
                        // last identifier is table name
                        var tableName = ids.Last().Value;
                        Tables.Add(tableName);

                        if (ids.Count >= 2)
                        {
                            // second-to-last is schema (for 2-part or more identifiers)
                            var schemaName = ids[ids.Count - 2].Value;
                            Schemas.Add(schemaName);
                        }

                        if (node.Alias != null && !string.IsNullOrEmpty(node.Alias.Value))
                        {
                            Aliases[node.Alias.Value] = tableName;
                            // also expose alias as a table-like reference
                            Tables.Add(node.Alias.Value);
                        }
                    }
                }

                base.Visit(node);
            }

            public override void Visit(ColumnReferenceExpression node)
            {
                if (node == null) return;
                if (node.MultiPartIdentifier != null && node.MultiPartIdentifier.Identifiers.Count > 0)
                {
                    var colName = node.MultiPartIdentifier.Identifiers.Last().Value;
                    Columns.Add(colName);

                    // if column reference includes a table/schema prefix, capture schema if present
                    var ids = node.MultiPartIdentifier.Identifiers;
                    if (ids.Count >= 2)
                    {
                        // if there are at least 2 parts, the penultimate could be table or schema depending; capture as table as well
                        var possibleTable = ids[ids.Count - 2].Value;
                        Tables.Add(possibleTable);

                        if (ids.Count >= 3)
                        {
                            var possibleSchema = ids[ids.Count - 3].Value;
                            Schemas.Add(possibleSchema);
                        }
                    }
                }

                base.Visit(node);
            }
        }

        public virtual List<CteBlock> parseSQL(string sql)
        {
            // Return CTEs without printing (analysis is silent)
            var analysis = AnalyzeSql(sql);
            return analysis.Ctes;
        }

        /// <summary>
        /// Detects if the SQL query is already in PostgreSQL format by checking for SQL Server-specific functions and keywords.
        /// If SQL Server-specific functions are found, it's definitely SQL Server, not PostgreSQL.
        /// Otherwise, attempts to parse with SQL Server parser to determine dialect.
        /// </summary>
        protected bool IsPostgresQuery(string sql)
        {
            if (string.IsNullOrEmpty(sql)) return false;
            
            // Check for SQL Server-specific functions - if found, it's definitely SQL Server, not PostgreSQL
            var sqlServerFunctions = new[]
            {
                @"\bISNUMERIC\s*\(",
                @"\bISDATE\s*\(",
                @"\bGETDATE\s*\(",
                @"\bISNULL\s*\(",
                @"\bTOP\s+\d+",  // TOP n (SQL Server) vs LIMIT n (PostgreSQL)
                @"\bDATEPART\s*\(",
                @"\bDATEDIFF\s*\(",
                @"\bDATEADD\s*\(",
                @"\bLEN\s*\(",  // SQL Server LEN vs PostgreSQL LENGTH
                @"\bSUBSTRING\s*\([^,]+,\s*\d+\s*,\s*\d+",  // SQL Server SUBSTRING(expr, start, length) vs PostgreSQL SUBSTRING(expr FROM start FOR length)
                @"\[.*?\]",  // Square brackets for identifiers (SQL Server) vs double quotes (PostgreSQL)
            };
            
            foreach (var pattern in sqlServerFunctions)
            {
                if (Regex.IsMatch(sql, pattern, RegexOptions.IgnoreCase))
                {
                    return false; // Definitely SQL Server
                }
            }
            
            // Check for PostgreSQL-specific functions/features - if found, likely PostgreSQL
            var postgresFunctions = new[]
            {
                @"\bREGEXP_LIKE\s*\(",
                @"\bREGEXP_MATCHES\s*\(",
                @"\bREGEXP_REPLACE\s*\(",
                @"\bTO_DATE\s*\(",
                @"\bTO_TIMESTAMP\s*\(",
                @"\bEXTRACT\s*\(",
                @"\bDATE_PART\s*\(",
                @"::\s*(text|int|numeric|date|timestamp)",  // PostgreSQL type casting syntax
                @"\bLIMIT\s+\d+",  // PostgreSQL LIMIT vs SQL Server TOP
                @"\bILIKE\s+",  // PostgreSQL case-insensitive LIKE
            };
            
            bool hasPostgresFeatures = false;
            foreach (var pattern in postgresFunctions)
            {
                if (Regex.IsMatch(sql, pattern, RegexOptions.IgnoreCase))
                {
                    hasPostgresFeatures = true;
                    break;
                }
            }
            
            // If it has PostgreSQL features but no SQL Server features, it's likely PostgreSQL
            if (hasPostgresFeatures)
            {
                return true;
            }
            
            // Fix standalone ISNUMERIC before testing (in case it's SQL Server with missing = 1)
            var testSql = FixStandaloneIsNumeric(sql);
            
            // Normalize for parser
            var normalized = NormalizeForParser(testSql);
            
            // Try to parse with SQL Server parser
            var parser = new TSql150Parser(true);
            IList<ParseError> errors;
            
            try
            {
                var tree = parser.Parse(new StringReader(normalized), out errors);
                
                // If we got a parse tree (even with errors), it's likely SQL Server syntax
                // If tree is null and we have critical syntax errors, it's likely not SQL Server
                if (tree == null)
                {
                    // No parse tree at all - likely not SQL Server
                    return true;
                }
                
                // If we have parse errors, check if they're critical syntax errors
                if (errors != null && errors.Count > 0)
                {
                    // Check for critical syntax errors that suggest non-SQL Server syntax
                    var criticalErrors = errors.Where(e => 
                        e.Message.Contains("syntax") || 
                        e.Message.Contains("Incorrect syntax") ||
                        e.Message.Contains("Unexpected") ||
                        e.Message.Contains("Expecting")).ToList();
                    
                    // If all errors are critical syntax errors, likely not SQL Server
                    if (criticalErrors.Count == errors.Count && criticalErrors.Count > 0)
                    {
                        // Try fallback parse with original SQL
                        try
                        {
                            IList<ParseError> errorsOrig;
                            var treeOrig = parser.Parse(new StringReader(testSql), out errorsOrig);
                            if (treeOrig == null || (errorsOrig != null && errorsOrig.Count > 0 && 
                                errorsOrig.All(e => e.Message.Contains("syntax") || e.Message.Contains("Incorrect syntax"))))
                            {
                                return true; // Likely PostgreSQL
                            }
                        }
                        catch
                        {
                            return true; // Parse exception suggests non-SQL Server syntax
                        }
                    }
                }
                
                // Successfully parsed or only validation errors - it's SQL Server
                return false;
            }
            catch
            {
                // Parse exception - likely not SQL Server syntax
                return true;
            }
        }

        protected SqlAnalysisResult AnalyzeSql(string sql, System.Text.StringBuilder? debugOutput = null)
        {
            void DebugWriteLine(string message)
            {
                if (debugOutput != null)
                {
                    debugOutput.AppendLine(message);
                }
                Console.WriteLine(message);
            }

            var result = new SqlAnalysisResult();

            // Fix standalone ISNUMERIC expressions BEFORE parsing to ensure parser receives valid SQL
            sql = FixStandaloneIsNumeric(sql);

            var parser = new TSql150Parser(true);
            IList<ParseError> errors;

            // normalize SQL before parsing to avoid hidden unicode characters causing lexer errors
            var normalized = NormalizeForParser(sql);

            var tree = parser.Parse(new StringReader(normalized), out errors);
            
            // Check if parsing failed completely - if so, likely PostgreSQL
            if (tree == null || (errors != null && errors.Count > 0 && 
                errors.All(e => e.Message.Contains("syntax") || e.Message.Contains("Incorrect syntax") || 
                                e.Message.Contains("Unexpected") || e.Message.Contains("Expecting"))))
            {
                // Try fallback parse to confirm
                try
                {
                    IList<ParseError> errorsOrig;
                    var treeOrig = parser.Parse(new StringReader(sql), out errorsOrig);
                    if (treeOrig == null || (errorsOrig != null && errorsOrig.Count > 0 && 
                        errorsOrig.All(e => e.Message.Contains("syntax") || e.Message.Contains("Incorrect syntax"))))
                    {
                        // Cannot parse with SQL Server parser - likely PostgreSQL
                        // Analyze PostgreSQL query using regex-based extraction
                        return AnalyzePostgresSql(sql, debugOutput);
                    }
                    // Fallback succeeded, use it
                    tree = treeOrig as TSqlScript;
                    errors = errorsOrig;
                }
                catch
                {
                    // Parse exception - likely PostgreSQL, analyze as PostgreSQL
                    return AnalyzePostgresSql(sql, debugOutput);
                }
            }

            if (errors != null && errors.Count > 0)
            {
                // attempt fallback parses to be more tolerant
                var fallbackMessages = new List<string>();

                // 1) try parsing the original (non-normalized) SQL
                try
                {
                    IList<ParseError> errorsOrig;
                    var treeOrig = parser.Parse(new StringReader(sql), out errorsOrig);
                    if (errorsOrig == null || errorsOrig.Count == 0)
                    {
                        // success with original SQL
                        tree = treeOrig as TSqlScript;
                    }
                    else
                    {
                        fallbackMessages.AddRange(errorsOrig.Select(pe => $"Orig: Line {pe.Line}, Col {pe.Column}: {pe.Message}"));
                    }
                }
                catch (Exception ex)
                {
                    fallbackMessages.Add("Orig parse exception: " + ex.Message);
                }

                // 2) try parsing with quoted identifiers disabled
                try
                {
                    var parserNoQuoted = new TSql150Parser(false);
                    IList<ParseError> errorsNoQuoted;
                    var treeNoQuoted = parserNoQuoted.Parse(new StringReader(normalized), out errorsNoQuoted);
                    if (errorsNoQuoted == null || errorsNoQuoted.Count == 0)
                    {
                        tree = treeNoQuoted as TSqlScript;
                        errors = null;
                    }
                    else
                    {
                        fallbackMessages.AddRange(errorsNoQuoted.Select(pe => $"NoQuoted: Line {pe.Line}, Col {pe.Column}: {pe.Message}"));
                    }
                }
                catch (Exception ex)
                {
                    fallbackMessages.Add("NoQuoted parse exception: " + ex.Message);
                }

                // if still errors (tree null or errors present), populate ParseErrors and return
                if (!(tree is TSqlScript) || (errors != null && errors.Count > 0))
                {
                    var primary = (errors ?? Enumerable.Empty<ParseError>()).Select(pe => $"Norm: Line {pe.Line}, Column {pe.Column}: {pe.Message}");
                    result.ParseErrors = primary.Concat(fallbackMessages).ToArray();
                    return result;
                }
            }

            var generator = new Sql150ScriptGenerator(new SqlScriptGeneratorOptions { KeywordCasing = KeywordCasing.Uppercase });

            if (!(tree is TSqlScript script)) return result;

            // map of CTE name => columns
            var cteColsMap = new Dictionary<string, List<string>>(StringComparer.OrdinalIgnoreCase);
            var cteList = new List<CteBlock>();

            foreach (var batch in script.Batches)
            {
                foreach (var stmt in batch.Statements)
                {
                    void ProcessWith(WithCtesAndXmlNamespaces with)
                    {
                        if (with == null) return;
                        foreach (var cte in with.CommonTableExpressions)
                        {
                            string generatedSql;
                            generator.GenerateScript(cte.QueryExpression, out generatedSql);

                            var cteName = cte.ExpressionName?.Value ?? ($"cte_{cteColsMap.Count}");
                            var cols = new List<string>();

                            // attempt to extract columns
                            if (cte.QueryExpression != null)
                            {
                                // re-use local extraction logic
                                List<string> ExtractSelectedColumns(TSqlFragment fragment)
                                {
                                    var colsLocal = new List<string>();
                                    if (fragment == null) return colsLocal;

                                    if (fragment is QuerySpecification qs)
                                    {
                                        foreach (var sel in qs.SelectElements)
                                        {
                                            if (sel is SelectStarExpression)
                                            {
                                                colsLocal.Add("*");
                                                continue;
                                            }

                                            if (sel is SelectScalarExpression sse)
                                            {
                                                string colName = null;
                                                if (sse.ColumnName != null) colName = sse.ColumnName.Value;

                                                if (string.IsNullOrEmpty(colName))
                                                {
                                                    if (sse.Expression is ColumnReferenceExpression cre)
                                                    {
                                                        if (cre.MultiPartIdentifier != null)
                                                        {
                                                            colName = string.Join('.', cre.MultiPartIdentifier.Identifiers.Select(id => id.Value));
                                                        }
                                                        else colName = cre.ToString();
                                                    }
                                                    else if (sse.Expression is FunctionCall fc)
                                                    {
                                                        colName = fc.FunctionName?.Value ?? fc.ToString();
                                                    }
                                                    else colName = sse.Expression?.ToString();
                                                }

                                                if (!string.IsNullOrEmpty(colName)) colsLocal.Add(colName);
                                            }
                                        }
                                    }
                                    else if (fragment is QueryParenthesisExpression qpe)
                                    {
                                        colsLocal.AddRange(ExtractSelectedColumns(qpe.QueryExpression));
                                    }
                                    else if (fragment is BinaryQueryExpression bqe)
                                    {
                                        colsLocal.AddRange(ExtractSelectedColumns(bqe.FirstQueryExpression));
                                        colsLocal.AddRange(ExtractSelectedColumns(bqe.SecondQueryExpression));
                                    }
                                    else if (fragment is SelectStatement selStmt)
                                    {
                                        colsLocal.AddRange(ExtractSelectedColumns(selStmt.QueryExpression));
                                    }

                                    return colsLocal.Distinct().ToList();
                                }

                                cols = ExtractSelectedColumns(cte.QueryExpression);
                            }

                            cteColsMap[cteName] = cols;
                            cteList.Add(new CteBlock { Name = cteName, Query = generatedSql });
                        }
                    }

                    if (stmt is SelectStatement selectStmt && selectStmt.WithCtesAndXmlNamespaces != null)
                        ProcessWith(selectStmt.WithCtesAndXmlNamespaces);
                    else if (stmt is InsertStatement insertStmt && insertStmt.WithCtesAndXmlNamespaces != null)
                        ProcessWith(insertStmt.WithCtesAndXmlNamespaces);
                    else if (stmt is UpdateStatement updateStmt && updateStmt.WithCtesAndXmlNamespaces != null)
                        ProcessWith(updateStmt.WithCtesAndXmlNamespaces);
                    else if (stmt is DeleteStatement deleteStmt && deleteStmt.WithCtesAndXmlNamespaces != null)
                        ProcessWith(deleteStmt.WithCtesAndXmlNamespaces);
                }
            }

            var cteColumnsAll = cteColsMap.SelectMany(kv => kv.Value).Distinct(StringComparer.OrdinalIgnoreCase).ToArray();
            // Get all columns from DDL tables (lowercase keys from ColumnNameMap)
            var ddlColumns = ColumnNameMap.Keys.ToArray();
            var tempColumns = ddlColumns.Concat(cteColumnsAll).Distinct(StringComparer.OrdinalIgnoreCase).ToArray();
            // Get all table names from DDL (lowercase keys from table_columns)
            var ddlTableNames = table_columns.Keys.Select(t => t.ToLowerInvariant()).ToArray();
            var tableNames = cteColsMap.Keys.Concat(ddlTableNames).Distinct(StringComparer.OrdinalIgnoreCase).ToArray();

            // collect referenced tables/columns/schemas
            var referencedTables = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
            var referencedColumns = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
            var referencedSchemas = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
            var referencedAliases = new HashSet<string>(StringComparer.OrdinalIgnoreCase);

            foreach (var batch2 in script.Batches)
            {
                foreach (var stmt2 in batch2.Statements)
                {
                    var collector = new ReferenceCollector();
                    stmt2.Accept(collector);

                    foreach (var t in collector.Tables) referencedTables.Add(t);
                    foreach (var c in collector.Columns) referencedColumns.Add(c);
                    foreach (var s in collector.Schemas) referencedSchemas.Add(s);
                    foreach (var a in collector.Aliases.Keys) referencedAliases.Add(a);
                }
            }

            // collect select aliases
            List<string> ExtractSelectAliases(TSqlFragment fragment)
            {
                var aliases = new List<string>();
                if (fragment is QuerySpecification qs)
                {
                    foreach (var sel in qs.SelectElements)
                    {
                        if (sel is SelectScalarExpression sse && sse.ColumnName != null)
                        {
                            aliases.Add(sse.ColumnName.Value);
                        }
                    }
                }
                else if (fragment is QueryParenthesisExpression qpe)
                {
                    aliases.AddRange(ExtractSelectAliases(qpe.QueryExpression));
                }
                else if (fragment is BinaryQueryExpression bqe)
                {
                    aliases.AddRange(ExtractSelectAliases(bqe.FirstQueryExpression));
                    aliases.AddRange(ExtractSelectAliases(bqe.SecondQueryExpression));
                }
                else if (fragment is SelectStatement selStmt)
                {
                    aliases.AddRange(ExtractSelectAliases(selStmt.QueryExpression));
                }

                return aliases.Distinct(StringComparer.OrdinalIgnoreCase).ToList();
            }

            var topLevelSelectAliases = new List<string>();
            foreach (var batch3 in script.Batches)
            {
                foreach (var stmt3 in batch3.Statements)
                {
                    if (stmt3 is SelectStatement sel)
                    {
                        topLevelSelectAliases.AddRange(ExtractSelectAliases(sel.QueryExpression));
                    }
                }
            }

            var allowedTables = new HashSet<string>(tableNames, StringComparer.OrdinalIgnoreCase);
            foreach (var a in referencedAliases) allowedTables.Add(a);

            var allowedColumns = new HashSet<string>(tempColumns, StringComparer.OrdinalIgnoreCase);
            foreach (var a in topLevelSelectAliases) allowedColumns.Add(a);
            allowedColumns.Add("record_id");

            var unknownTables = referencedTables.Where(t => !allowedTables.Contains(t)).Distinct(StringComparer.OrdinalIgnoreCase).ToArray();
            var unknownColumns = referencedColumns.Where(c => !allowedColumns.Contains(c)).Distinct(StringComparer.OrdinalIgnoreCase).ToArray();
            // Check against SchemaMap keys (source schemas) and values (target schemas)
            var nonDatasetSchemas = referencedSchemas.Where(s => 
                !SchemaMap.ContainsKey(s) && !SchemaMap.Values.Contains(s, StringComparer.OrdinalIgnoreCase))
                .Distinct(StringComparer.OrdinalIgnoreCase).ToArray();

            result.Ctes = cteList;
            result.UnknownTables = unknownTables;
            result.UnknownColumns = unknownColumns;
            result.NonDatasetSchemas = nonDatasetSchemas;
            result.AllTempColumns = tempColumns;
            result.TableNames = tableNames;

            return result;
        }

        /// <summary>
        /// Analyzes PostgreSQL SQL queries using regex-based extraction since we can't use SQL Server parser.
        /// Extracts and validates schema names, table names, and column names.
        /// </summary>
        protected SqlAnalysisResult AnalyzePostgresSql(string sql, System.Text.StringBuilder? debugOutput = null)
        {
            void DebugWriteLine(string message)
            {
                if (debugOutput != null)
                {
                    debugOutput.AppendLine(message);
                }
                Console.WriteLine(message);
            }

            var result = new SqlAnalysisResult();
            
            if (string.IsNullOrEmpty(sql)) return result;

            // Extract CTEs and their columns using regex
            // Handle nested parentheses in CTEs by matching balanced parentheses
            var cteColsMap = new Dictionary<string, List<string>>(StringComparer.OrdinalIgnoreCase);
            var cteList = new List<CteBlock>();
            
            // Find all CTEs by matching WITH ... AS ( ... )
            // Need to find the main SELECT that's not inside a CTE
            var withPattern = @"WITH\s+";
            var withMatch = Regex.Match(sql, withPattern, RegexOptions.IgnoreCase);
            if (withMatch.Success)
            {
                int ctesStart = withMatch.Index + withMatch.Length;
                int ctesEnd = ctesStart;
                
                // Find where CTEs end by finding the main SELECT (not inside parentheses)
                int parenDepth = 0;
                bool inString = false;
                char stringDelimiter = '\0';
                bool foundMainSelect = false;
                
                for (int i = ctesStart; i < sql.Length; i++)
                {
                    char ch = sql[i];
                    
                    // Track string literals
                    if (!inString && (ch == '\'' || ch == '"'))
                    {
                        inString = true;
                        stringDelimiter = ch;
                    }
                    else if (inString && ch == stringDelimiter)
                    {
                        if (i + 1 < sql.Length && sql[i + 1] == stringDelimiter)
                        {
                            i++; // Skip escaped quote
                        }
                        else
                        {
                            inString = false;
                            stringDelimiter = '\0';
                        }
                    }
                    else if (!inString)
                    {
                        if (ch == '(') parenDepth++;
                        else if (ch == ')') parenDepth--;
                        else if (parenDepth == 0 && i + 6 < sql.Length && 
                                 sql.Substring(i, 7).Equals("SELECT ", StringComparison.OrdinalIgnoreCase))
                        {
                            // Found main SELECT outside of parentheses
                            ctesEnd = i;
                            foundMainSelect = true;
                            break;
                        }
                    }
                }
                
                if (!foundMainSelect)
                {
                    ctesEnd = sql.Length;
                }
                
                var ctesSection = sql.Substring(ctesStart, ctesEnd - ctesStart);
                
                // Extract individual CTEs - handle nested parentheses and commas between CTEs
                // Pattern: match CTE name (can be quoted or unquoted, can contain underscores and numbers)
                // Matches: CTE_12422_dataflowmetadata AS ( or "CTE_12422_dataflowmetadata" AS ( or ,CTE_12422_dataflowmetadata AS (
                // First CTE after WITH doesn't have comma, so match start of string OR comma
                var cteNamePattern = @"(?:^|,)\s*(?:""?([^""\s,()]+)""?|([a-zA-Z_][a-zA-Z0-9_]*))\s+AS\s*\(";
                var cteNameMatches = Regex.Matches(ctesSection, cteNamePattern, RegexOptions.IgnoreCase | RegexOptions.Multiline);
                
                foreach (Match cteNameMatch in cteNameMatches)
                {
                    // Get CTE name from either group (quoted or unquoted)
                    var cteName = cteNameMatch.Groups[1].Success ? cteNameMatch.Groups[1].Value : 
                                  cteNameMatch.Groups[2].Success ? cteNameMatch.Groups[2].Value : 
                                  cteNameMatch.Groups[0].Value;
                    cteName = cteName.Trim('"', ' ');
                    var startPos = cteNameMatch.Index + cteNameMatch.Length;
                    var parenDepth2 = 1;
                    var endPos = startPos;
                    
                    // Find matching closing parenthesis
                    bool inString2 = false;
                    char stringDelimiter2 = '\0';
                    while (endPos < ctesSection.Length && parenDepth2 > 0)
                    {
                        char ch = ctesSection[endPos];
                        
                        if (!inString2 && (ch == '\'' || ch == '"'))
                        {
                            inString2 = true;
                            stringDelimiter2 = ch;
                        }
                        else if (inString2 && ch == stringDelimiter2)
                        {
                            if (endPos + 1 < ctesSection.Length && ctesSection[endPos + 1] == stringDelimiter2)
                            {
                                endPos++; // Skip escaped quote
                            }
                            else
                            {
                                inString2 = false;
                                stringDelimiter2 = '\0';
                            }
                        }
                        else if (!inString2)
                        {
                            if (ch == '(') parenDepth2++;
                            else if (ch == ')') parenDepth2--;
                        }
                        endPos++;
                    }
                    
                    if (parenDepth2 == 0)
                    {
                        var cteQuery = ctesSection.Substring(startPos, endPos - startPos - 1);
                        
                        // Extract columns from SELECT in CTE
                        var selectPattern = @"SELECT\s+(?<columns>.*?)\s+FROM";
                        var selectMatch = Regex.Match(cteQuery, selectPattern, RegexOptions.IgnoreCase | RegexOptions.Singleline);
                        var cols = new List<string>();
                        
                        if (selectMatch.Success)
                        {
                            var columnsStr = selectMatch.Groups["columns"].Value;
                            // Extract column aliases - look for AS [alias] or AS "alias"
                            // Exclude numeric literals and boolean literals
                            var aliasPattern = @"\s+AS\s+(\[?(\w+)\]?|""([^""]+)"")";
                            var aliasMatches = Regex.Matches(columnsStr, aliasPattern, RegexOptions.IgnoreCase);
                            var aliasNumericPattern = @"^\d+$";
                            var aliasBooleanLiterals = new HashSet<string>(new[] { "true", "false" }, StringComparer.OrdinalIgnoreCase);
                            
                            foreach (Match aliasMatch in aliasMatches)
                            {
                                var alias = aliasMatch.Groups[2].Success ? aliasMatch.Groups[2].Value : 
                                           aliasMatch.Groups[3].Success ? aliasMatch.Groups[3].Value : 
                                           aliasMatch.Groups[1].Value.Trim('[', ']', '"');
                                
                                // Skip numeric literals and boolean literals
                                if (!string.IsNullOrEmpty(alias) && 
                                    !Regex.IsMatch(alias, aliasNumericPattern) && 
                                    !aliasBooleanLiterals.Contains(alias))
                                {
                                    cols.Add(alias);
                                }
                            }
                        }
                        
                        cteColsMap[cteName] = cols;
                        cteList.Add(new CteBlock { Name = cteName, Query = cteQuery });
                        DebugWriteLine($"[DEBUG AnalyzePostgresSql] Extracted CTE: '{cteName}' with columns: {string.Join(", ", cols)}");
                    }
                }
            }
            
            DebugWriteLine($"[DEBUG AnalyzePostgresSql] Total CTEs extracted: {cteColsMap.Count}");
            foreach (var kvp in cteColsMap)
            {
                DebugWriteLine($"[DEBUG AnalyzePostgresSql] CTE '{kvp.Key}' -> columns: {string.Join(", ", kvp.Value)}");
            }

            var cteColumnsAll = cteColsMap.SelectMany(kv => kv.Value).Distinct(StringComparer.OrdinalIgnoreCase).ToArray();
            // Get all columns from DDL tables (lowercase keys from ColumnNameMap)
            var ddlColumns = ColumnNameMap.Keys.ToArray();
            var tempColumns = ddlColumns.Concat(cteColumnsAll).Distinct(StringComparer.OrdinalIgnoreCase).ToArray();
            // Get all table names from DDL (lowercase keys from table_columns)
            var ddlTableNames = table_columns.Keys.Select(t => t.ToLowerInvariant()).ToArray();
            var tableNames = cteColsMap.Keys.Concat(ddlTableNames).Distinct(StringComparer.OrdinalIgnoreCase).ToArray();

            // Extract CTE aliases first (before table extraction)
            // Also extract CTE names from the SQL directly to catch any that might have been missed
            var referencedAliases = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
            foreach (var cteName in cteColsMap.Keys)
            {
                referencedAliases.Add(cteName);
            }
            
            // Also extract CTE names directly from SQL to ensure we catch all of them
            // Pattern: WITH cteName AS ( or , cteName AS ( or WITH "cteName" AS (
            var directCtePattern = @"(?:WITH|,)\s+(?:""?([^""\s,()]+)""?|(\w+))\s+AS\s*\(";
            var directCteMatches = Regex.Matches(sql, directCtePattern, RegexOptions.IgnoreCase);
            foreach (Match cteMatch in directCteMatches)
            {
                var cteName = cteMatch.Groups[1].Success ? cteMatch.Groups[1].Value : 
                              cteMatch.Groups[2].Success ? cteMatch.Groups[2].Value : "";
                cteName = cteName.Trim('"', ' ');
                if (!string.IsNullOrEmpty(cteName))
                {
                    referencedAliases.Add(cteName);
                }
            }

            // Extract schema.table.column references using regex
            var referencedSchemas = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
            var referencedTables = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
            var referencedColumns = new HashSet<string>(StringComparer.OrdinalIgnoreCase);

            // First, find all function calls and mark their arguments as non-table contexts
            // This prevents matching FROM inside EXTRACT(YEAR FROM "column")
            var functionCallPattern = @"\b(EXTRACT|TO_DATE|CAST|SUBSTRING|POSITION|CONCAT|REGEXP_LIKE|REGEXP_MATCHES|ISDATE|COALESCE|ISNULL|GETDATE|YEAR|MONTH|DAY|DATE_PART)\s*\(";
            var functionMatches = Regex.Matches(sql, functionCallPattern, RegexOptions.IgnoreCase);
            var functionArgRangesForTables = new List<Tuple<int, int>>();
            
            foreach (Match funcMatch in functionMatches)
            {
                int startPos = funcMatch.Index;
                int parenDepth = 1;
                int endPos = startPos + funcMatch.Length;
                
                // Find matching closing parenthesis, handling string literals
                bool inString = false;
                char stringDelimiter = '\0';
                while (endPos < sql.Length && parenDepth > 0)
                {
                    char ch = sql[endPos];
                    if (!inString && (ch == '\'' || ch == '"'))
                    {
                        inString = true;
                        stringDelimiter = ch;
                    }
                    else if (inString && ch == stringDelimiter)
                    {
                        if (endPos + 1 < sql.Length && sql[endPos + 1] == stringDelimiter)
                        {
                            endPos++; // Skip escaped quote
                        }
                        else
                        {
                            inString = false;
                            stringDelimiter = '\0';
                        }
                    }
                    else if (!inString)
                    {
                        if (ch == '(') parenDepth++;
                        else if (ch == ')') parenDepth--;
                    }
                    endPos++;
                }
                
                if (parenDepth == 0)
                {
                    functionArgRangesForTables.Add(new Tuple<int, int>(startPos, endPos - 1));
                }
            }

            // Pattern for schema.table or table references
            // Matches: schema."table", "schema"."table", schema.[table], [table], table, "table"
            // IMPORTANT: Only match FROM/JOIN that are NOT inside function calls
            var tableRefPattern = @"(?:FROM|JOIN)\s+(?:(?:(\w+|""[^""]+"")\.)?(\[?(\w+)\]?|""([^""]+)""))";
            var tableMatches = Regex.Matches(sql, tableRefPattern, RegexOptions.IgnoreCase);
            DebugWriteLine($"[DEBUG AnalyzePostgresSql] Found {tableMatches.Count} potential table references in FROM/JOIN");
            foreach (Match match in tableMatches)
            {
                // Skip if this FROM/JOIN is inside a function call
                bool isInFunction = functionArgRangesForTables.Any(range => match.Index >= range.Item1 && match.Index <= range.Item2);
                if (isInFunction)
                {
                    DebugWriteLine($"[DEBUG AnalyzePostgresSql] Skipped table match at position {match.Index} - inside function call");
                    continue;
                }

                if (match.Groups[1].Success)
                {
                    var schema = match.Groups[1].Value.Trim('"');
                    referencedSchemas.Add(schema);
                    DebugWriteLine($"[DEBUG AnalyzePostgresSql] Found schema: '{schema}'");
                }
                var table = match.Groups[3].Success ? match.Groups[3].Value : 
                           match.Groups[4].Success ? match.Groups[4].Value : 
                           match.Groups[2].Value.Trim('[', ']', '"');
                
                DebugWriteLine($"[DEBUG AnalyzePostgresSql] Found table reference: '{table}' (is CTE: {referencedAliases.Contains(table)})");
                
                // Skip CTE names - they're already in referencedAliases
                if (!referencedAliases.Contains(table))
                {
                    referencedTables.Add(table);
                    DebugWriteLine($"[DEBUG AnalyzePostgresSql] Added '{table}' to referencedTables");
                }
                else
                {
                    DebugWriteLine($"[DEBUG AnalyzePostgresSql] Skipped '{table}' - it's a CTE");
                }
            }

            // Extract column references - handle "column" in WHERE clauses, SELECT lists, etc.
            // Exclude: CTE names, numeric literals, boolean literals, function arguments
            // Only match quoted identifiers that appear in column contexts (not function arguments)
            
            // First, find all function calls and mark their arguments as non-columns
            var functionCallPatternForColumns = @"\b(SUBSTRING|POSITION|CONCAT|CAST|EXTRACT|YEAR|TO_DATE|REGEXP_LIKE|REGEXP_MATCHES|ISDATE|COALESCE|ISNULL|GETDATE)\s*\(";
            var functionMatchesForColumns = Regex.Matches(sql, functionCallPatternForColumns, RegexOptions.IgnoreCase);
            var functionArgRanges = new List<Tuple<int, int>>();
            
            foreach (Match funcMatch in functionMatchesForColumns)
            {
                int startPos = funcMatch.Index + funcMatch.Length;
                int parenDepth = 1;
                int endPos = startPos;
                
                // Find matching closing parenthesis
                while (endPos < sql.Length && parenDepth > 0)
                {
                    if (sql[endPos] == '(') parenDepth++;
                    else if (sql[endPos] == ')') parenDepth--;
                    endPos++;
                }
                
                if (parenDepth == 0)
                {
                    functionArgRanges.Add(new Tuple<int, int>(startPos, endPos - 1));
                }
            }
            
            // Pattern: "column" followed by operators or SQL keywords (but not inside function calls)
            var columnRefPattern = @"""([^""]+)""";
            var columnMatches = Regex.Matches(sql, columnRefPattern, RegexOptions.IgnoreCase);
            var sqlKeywords = new HashSet<string>(new[] { "SELECT", "FROM", "WHERE", "GROUP", "ORDER", "HAVING", "WITH", "AS", "AND", "OR", "NOT", "IN", "EXISTS", "CASE", "WHEN", "THEN", "ELSE", "END", "NULL", "IS", "LIKE", "BETWEEN", "CROSS", "JOIN", "INNER", "OUTER", "LEFT", "RIGHT", "FULL", "ON", "EXTRACT", "YEAR", "TO_DATE", "CAST", "CONCAT", "SUBSTRING", "POSITION", "REGEXP_LIKE", "REGEXP_MATCHES", "ISDATE", "UNION" }, StringComparer.OrdinalIgnoreCase);
            
            // Also exclude numeric literals and boolean literals
            var numericPattern = @"^\d+$";
            var booleanLiterals = new HashSet<string>(new[] { "true", "false" }, StringComparer.OrdinalIgnoreCase);
            
            foreach (Match match in columnMatches)
            {
                var column = match.Groups[1].Value;
                int matchIndex = match.Index;
                int matchEnd = matchIndex + match.Length;
                
                // Skip if inside a function call (function argument)
                bool isInFunctionArg = functionArgRanges.Any(range => matchIndex >= range.Item1 && matchEnd <= range.Item2);
                if (isInFunctionArg)
                {
                    continue;
                }
                
                // Skip SQL keywords, function names, numeric literals, boolean literals, CTE names, and TABLE NAMES
                if (string.IsNullOrEmpty(column) || 
                    sqlKeywords.Contains(column.ToUpper()) ||
                    Regex.IsMatch(column, numericPattern) ||
                    booleanLiterals.Contains(column) ||
                    referencedAliases.Contains(column) ||
                    referencedTables.Contains(column)) // Exclude table names from columns
                {
                    continue;
                }
                
                // Only add if it's followed by a column-like context (operators, keywords, etc.)
                string afterMatch = matchEnd < sql.Length ? sql.Substring(matchEnd, Math.Min(20, sql.Length - matchEnd)) : "";
                var columnContextPattern = @"^\s*(?:,|\)|$|AS|FROM|WHERE|GROUP|ORDER|HAVING|AND|OR|<>|=|<|>|<=|>=|~|!~|IN|IS|NOT|LIKE|BETWEEN)";
                if (!Regex.IsMatch(afterMatch, columnContextPattern, RegexOptions.IgnoreCase))
                {
                    continue;
                }
                
                referencedColumns.Add(column);
            }

            // Extract SELECT aliases
            var selectAliasPattern = @"SELECT\s+.*?\s+AS\s+(\w+|""[^""]+"")";
            var selectAliasMatches = Regex.Matches(sql, selectAliasPattern, RegexOptions.IgnoreCase);
            var topLevelSelectAliases = new List<string>();
            foreach (Match match in selectAliasMatches)
            {
                topLevelSelectAliases.Add(match.Groups[1].Value.Trim('"'));
            }

            var allowedTables = new HashSet<string>(tableNames, StringComparer.OrdinalIgnoreCase);
            foreach (var a in referencedAliases) allowedTables.Add(a);

            var allowedColumns = new HashSet<string>(tempColumns, StringComparer.OrdinalIgnoreCase);
            foreach (var a in topLevelSelectAliases) allowedColumns.Add(a);
            allowedColumns.Add("record_id");
            
            // Also allow CTE column aliases from all CTEs
            foreach (var cteCols in cteColsMap.Values)
            {
                foreach (var col in cteCols)
                {
                    allowedColumns.Add(col);
                }
            }

            // Exclude CTE names from unknown tables check
            // Also check if table name matches any CTE name (case-insensitive)
            DebugWriteLine($"[DEBUG AnalyzePostgresSql] Checking unknown tables. referencedTables: {string.Join(", ", referencedTables)}");
            DebugWriteLine($"[DEBUG AnalyzePostgresSql] allowedTables (first 10): {string.Join(", ", allowedTables.Take(10))}...");
            DebugWriteLine($"[DEBUG AnalyzePostgresSql] referencedAliases (CTEs): {string.Join(", ", referencedAliases)}");
            var unknownTables = referencedTables.Where(t => 
                !allowedTables.Contains(t) && 
                !referencedAliases.Contains(t) &&
                !cteColsMap.Keys.Any(cte => cte.Equals(t, StringComparison.OrdinalIgnoreCase)))
                .Distinct(StringComparer.OrdinalIgnoreCase).ToArray();
            DebugWriteLine($"[DEBUG AnalyzePostgresSql] unknownTables result: {string.Join(", ", unknownTables)}");
            
            // Exclude CTE names from unknown columns check
            // Also check if column name matches any CTE name (case-insensitive) - CTEs can be used as table aliases
            DebugWriteLine($"[DEBUG AnalyzePostgresSql] Checking unknown columns. referencedColumns (first 10): {string.Join(", ", referencedColumns.Take(10))}...");
            DebugWriteLine($"[DEBUG AnalyzePostgresSql] allowedColumns (first 10): {string.Join(", ", allowedColumns.Take(10))}...");
            var unknownColumns = referencedColumns.Where(c => 
                !allowedColumns.Contains(c) && 
                !referencedAliases.Contains(c) &&
                !cteColsMap.Keys.Any(cte => cte.Equals(c, StringComparison.OrdinalIgnoreCase)))
                .Distinct(StringComparer.OrdinalIgnoreCase).ToArray();
            DebugWriteLine($"[DEBUG AnalyzePostgresSql] unknownColumns result: {string.Join(", ", unknownColumns)}");
            
            // Check against SchemaMap keys (source schemas) and values (target schemas)
            DebugWriteLine($"[DEBUG AnalyzePostgresSql] Checking schemas. referencedSchemas: {string.Join(", ", referencedSchemas)}");
            DebugWriteLine($"[DEBUG AnalyzePostgresSql] SchemaMap keys: {string.Join(", ", SchemaMap.Keys)}");
            DebugWriteLine($"[DEBUG AnalyzePostgresSql] SchemaMap values: {string.Join(", ", SchemaMap.Values)}");
            var nonDatasetSchemas = referencedSchemas.Where(s => 
                !SchemaMap.ContainsKey(s) && !SchemaMap.Values.Contains(s, StringComparer.OrdinalIgnoreCase))
                .Distinct(StringComparer.OrdinalIgnoreCase).ToArray();
            DebugWriteLine($"[DEBUG AnalyzePostgresSql] nonDatasetSchemas result: {string.Join(", ", nonDatasetSchemas)}");

            result.Ctes = cteList;
            result.UnknownTables = unknownTables;
            result.UnknownColumns = unknownColumns;
            result.NonDatasetSchemas = nonDatasetSchemas;
            result.AllTempColumns = tempColumns;
            result.TableNames = tableNames;

            return result;
        }

        /// <summary>
        /// STRICT RULE: NEVER modify anything inside single-quoted string literals.
        /// This protects regex patterns and other string content from being changed.
        /// </summary>
        private string ProtectStringLiterals(string sql, Func<string, string> transform)
        {
            if (string.IsNullOrEmpty(sql)) return sql;
            
            var result = new StringBuilder();
            var stringLiterals = new List<string>();
            bool inSingleQuote = false;
            int stringStart = -1;
            
            for (int i = 0; i < sql.Length; i++)
            {
                char ch = sql[i];
                
                if (ch == '\'')
                {
                    if (!inSingleQuote)
                    {
                        // Start of string literal
                        inSingleQuote = true;
                        stringStart = i;
                    }
                    else
                    {
                        // Check for escaped quote
                        if (i + 1 < sql.Length && sql[i + 1] == '\'')
                        {
                            i++; // Skip escaped quote, stay in string
                        }
                        else
                        {
                            // End of string literal
                            inSingleQuote = false;
                            stringLiterals.Add(sql.Substring(stringStart, i - stringStart + 1));
                            // Use a unique placeholder that won't be affected by transformations
                            result.Append($"__STR_LIT_{stringLiterals.Count - 1}__");
                        }
                    }
                }
                else if (!inSingleQuote)
                {
                    result.Append(ch);
                }
                // If inSingleQuote is true and ch is not ', append to string (will be captured when closing)
            }
            
            // Handle unclosed string literal (append remaining if any)
            if (inSingleQuote)
            {
                stringLiterals.Add(sql.Substring(stringStart));
                result.Append($"__STR_LIT_{stringLiterals.Count - 1}__");
            }
            
            // Apply transformation to non-string parts
            string transformed = transform(result.ToString());
            
            // Restore string literals unchanged - restore in reverse order
            for (int i = stringLiterals.Count - 1; i >= 0; i--)
            {
                transformed = transformed.Replace($"__STR_LIT_{i}__", stringLiterals[i]);
            }
            
            return transformed;
        }

        protected string NormalizeSql(string sql)
        {
            if (string.IsNullOrEmpty(sql)) return sql;

            // First, normalize Unicode quotes to ASCII (this must happen BEFORE protecting string literals)
            // because we need to identify string literals by ASCII single quotes
            sql = sql.Replace('\u2018', '\'').Replace('\u2019', '\''); // ' ' -> '
            sql = sql.Replace('\u201C', '"').Replace('\u201D', '"'); // " " -> "

            // STRICT RULE: Protect single-quoted string literals from ALL modifications
            // Extract string literals FIRST before any transformations
            var stringLiterals = new List<string>();
            var result = new StringBuilder();
            bool inSingleQuote = false;
            int stringStart = -1;
            
            for (int i = 0; i < sql.Length; i++)
            {
                char ch = sql[i];
                
                if (ch == '\'')
                {
                    if (!inSingleQuote)
                    {
                        // Start of string literal
                        inSingleQuote = true;
                        stringStart = i;
                    }
                    else
                    {
                        // Check for escaped quote
                        if (i + 1 < sql.Length && sql[i + 1] == '\'')
                        {
                            i++; // Skip escaped quote, stay in string
                        }
                        else
                        {
                            // End of string literal
                            inSingleQuote = false;
                            stringLiterals.Add(sql.Substring(stringStart, i - stringStart + 1));
                            // Use a unique placeholder that won't be affected by transformations
                            result.Append($"__STR_LIT_{stringLiterals.Count - 1}__");
                        }
                    }
                }
                else if (!inSingleQuote)
                {
                    result.Append(ch);
                }
            }
            
            // Handle unclosed string literal
            if (inSingleQuote)
            {
                stringLiterals.Add(sql.Substring(stringStart));
                result.Append($"__STR_LIT_{stringLiterals.Count - 1}__");
            }
            
            // Now apply all transformations to non-string parts
            string nonStringSql = result.ToString();
            
            // remove BOM if present
            nonStringSql = nonStringSql.TrimStart('\uFEFF');

            // remove directional and zero-width characters that often come from Excel/copy-paste
            var invisibleChars = new[] { '\u200E', '\u200F', '\u200D', '\u2060', '\uFEFF', '\u2028', '\u2029' };
            foreach (var ch in invisibleChars) nonStringSql = nonStringSql.Replace(ch.ToString(), string.Empty);

            // replace non-breaking space and other non-standard spaces with normal space
            nonStringSql = nonStringSql.Replace('\u00A0', ' ');
            nonStringSql = nonStringSql.Replace('\u2007', ' ').Replace('\u202F', ' ');

            // normalize CRLF (before restoring, so placeholders are safe)
            nonStringSql = nonStringSql.Replace("\r\n", "\n");
            nonStringSql = nonStringSql.Replace("\r", "\n");

            // CRITICAL: Restore string literals FIRST, then immediately re-protect them for remaining transformations
            // Restore in reverse order to avoid issues if one placeholder contains another
            for (int i = stringLiterals.Count - 1; i >= 0; i--)
            {
                nonStringSql = nonStringSql.Replace($"__STR_LIT_{i}__", stringLiterals[i]);
            }

            // NOW re-protect string literals for the remaining transformations that add spaces and convert quotes
            nonStringSql = ProtectStringLiterals(nonStringSql, (sqlWithoutStrings) =>
            {
                // remove control characters except newline and tab
                sqlWithoutStrings = Regex.Replace(sqlWithoutStrings, "[\u0000-\u0008\u000B\u000C\u000E-\u001F]", string.Empty);
                
                // collapse multiple spaces/tabs to single space where appropriate
                sqlWithoutStrings = Regex.Replace(sqlWithoutStrings, "[ \t]{2,}", " ");
                
                // ensure there's a space around parentheses and operators to avoid token merging
                sqlWithoutStrings = Regex.Replace(sqlWithoutStrings, "([(),])", " $1 ");
                
                // convert double-quoted identifiers to bracketed form for SQL Server parser compatibility
                // e.g. "schema"."table" or "column" -> [schema].[table] or [column]
                // This regex will NOT match inside string literals because they've been replaced with placeholders
                sqlWithoutStrings = Regex.Replace(sqlWithoutStrings, "\"(?<id>[^\"]+)\"", "[$1]");
                
                return sqlWithoutStrings;
            });

            // Trim surrounding whitespace and normalize newlines to CRLF for parser readability
            nonStringSql = nonStringSql.Trim();
            nonStringSql = nonStringSql.Replace("\n", "\r\n");

            return nonStringSql;
        }

        protected string NormalizeForParser(string sql)
        {
            if (string.IsNullOrEmpty(sql)) return sql;

            // remove BOM if present
            sql = sql.TrimStart('\uFEFF');

            // replace smart single/double quotes with ASCII equivalents
            sql = sql.Replace('\u2018', '\'').Replace('\u2019', '\'');
            sql = sql.Replace('\u201C', '"').Replace('\u201D', '"');

            // remove directional and zero-width characters
            var invisibleChars = new[] { '\u200E', '\u200F', '\u200D', '\u2060', '\uFEFF', '\u2028', '\u2029' };
            foreach (var ch in invisibleChars) sql = sql.Replace(ch.ToString(), string.Empty);

            // replace non-breaking space and other non-standard spaces with normal space
            sql = sql.Replace('\u00A0', ' ');
            sql = sql.Replace('\u2007', ' ').Replace('\u202F', ' ');

            // normalize newlines
            sql = sql.Replace("\r\n", "\n");
            sql = sql.Replace("\r", "\n");

            // remove control characters except newline and tab
            sql = Regex.Replace(sql, "[\u0000-\u0008\u000B\u000C\u000E-\u001F]", string.Empty);

            return sql.Trim();
        }

        public void generateQCRUles()
        {
            
            ISheet sheet = workbook.GetSheetAt(0);

            foreach (IRow row in sheet)
            {
                if (row.RowNum < 3) continue;
                JsonObject rule = new JsonObject
                {
                    ["table_name"] = row.GetCell(0)?.ToString(),
                    ["column_name"] = row.GetCell(1)?.ToString(),
                    ["code"] = row.GetCell(2)?.ToString(),
                    ["rule_param"] = row.GetCell(5)?.ToString(),
                    ["rule_level"] = row.GetCell(6)?.ToString(),
                    ["severity"] = row.GetCell(7)?.ToString(),

                };
                qcRules.Add(rule);

            }

            
            string sql = "";
            foreach (JsonObject rule in qcRules)
            {
                
                if (qcRules.IndexOf(rule) < 98) continue;

                if (rule["rule_param"].ToString() != null)
                {
                    //sql = parseSQL(rule["rule_param"].ToString());
                }
            }
        }

        /// <summary>
        /// Fixes standalone ISNUMERIC expressions by adding = 1 check.
        /// Detects ISNUMERIC(expr) that is not already part of a comparison and adds = 1.
        /// This method can be used by subclasses to fix queries before transformation.
        /// </summary>
        protected string FixStandaloneIsNumeric(string sql)
        {
            if (string.IsNullOrEmpty(sql)) return sql;

            var result = new StringBuilder();
            int i = 0;
            
            while (i < sql.Length)
            {
                // Look for ISNUMERIC (case-insensitive)
                var isnumericMatch = Regex.Match(sql.Substring(i), @"\bISNUMERIC\s*\(", RegexOptions.IgnoreCase);
                if (!isnumericMatch.Success)
                {
                    // No more ISNUMERIC found, append rest of string
                    result.Append(sql.Substring(i));
                    break;
                }

                // Append everything before the match
                result.Append(sql.Substring(i, isnumericMatch.Index));

                int startPos = i + isnumericMatch.Index + isnumericMatch.Length - 1; // Position of opening (
                int parenDepth = 1;
                int exprEnd = startPos + 1;

                // Find matching closing parenthesis
                while (exprEnd < sql.Length && parenDepth > 0)
                {
                    if (sql[exprEnd] == '(') parenDepth++;
                    else if (sql[exprEnd] == ')') parenDepth--;
                    exprEnd++;
                }

                if (parenDepth == 0)
                {
                    // Found matching closing parenthesis
                    string isnumericExpr = sql.Substring(i + isnumericMatch.Index, exprEnd - (i + isnumericMatch.Index));
                    
                    // Check what comes after the closing parenthesis
                    int afterPos = exprEnd;
                    // Skip whitespace
                    while (afterPos < sql.Length && char.IsWhiteSpace(sql[afterPos])) afterPos++;
                    
                    // Check if it's already part of a comparison (followed by =, <>, !=, <, >, <=, >=)
                    bool isPartOfComparison = false;
                    if (afterPos < sql.Length)
                    {
                        string remaining = sql.Substring(afterPos);
                        // Check for comparison operators (any comparison means it's already checked)
                        if (Regex.IsMatch(remaining, @"^(=|<>|!=|<|>|<=|>=)"))
                        {
                            isPartOfComparison = true;
                        }
                    }

                    if (!isPartOfComparison)
                    {
                        // Add = 1 after the closing parenthesis
                        result.Append(isnumericExpr);
                        result.Append(" = 1");
                        i = exprEnd;
                    }
                    else
                    {
                        // Already has comparison, keep as is
                        result.Append(isnumericExpr);
                        i = exprEnd;
                    }
                }
                else
                {
                    // Unmatched parentheses, skip this ISNUMERIC and continue
                    result.Append(sql.Substring(i + isnumericMatch.Index, isnumericMatch.Length));
                    i += isnumericMatch.Index + isnumericMatch.Length;
                }
            }

            return result.ToString();
        }

        public virtual string TransformToPostgres(string sql)
        {
            // Default implementation: perform no transformation but run parseSQL for validation/logging.
            try
            {
                parseSQL(sql);
            }
            catch
            {
                // swallow parse exceptions in base implementation — subclass may provide stronger behavior
            }

            return sql;
        }

        public List<(int Row, string Original, string Transformed)> ProcessExpressionsFromExcel(int expressionColumnIndex = 5)
        {
            var outputs = new List<(int Row, string Original, string Transformed)>();
            if (workbook == null) return outputs;

            // Create debug output file
            var debugOutput = new System.Text.StringBuilder();
            void DebugWriteLine(string message)
            {
                debugOutput.AppendLine(message);
                Console.WriteLine(message); // Also write to console
            }

            var sheet = workbook.GetSheetAt(0);
            for (int r = expressionsStartRow; r <= expressionsEndRow; r++)
            {
                var row = sheet.GetRow(r);
                if (row == null) continue;
                var cell = row.GetCell(expressionColumnIndex);
                if (cell == null) continue;

                var expr = cell.ToString();
                if (string.IsNullOrWhiteSpace(expr)) continue;

                // skip parameterized/template queries (e.g. containing tokens like {%...%}, @param, :param, $1)
                // But exclude string literals from the check to avoid false positives
                bool IsParameterized(string s)
                {
                    if (string.IsNullOrEmpty(s)) return false;
                    
                    // Remove only single-quoted string literals to avoid false positives
                    // In PostgreSQL: single quotes ('...') are string literals, double quotes ("...") are identifiers
                    // Match: '...' where ... can contain escaped quotes (\')
                    var withoutStringLiterals = Regex.Replace(s, @"'(?:[^'\\]|\\.)*'", "");
                    
                    // {%TOKEN%}
                    if (Regex.IsMatch(withoutStringLiterals, @"\{%.*?%\}")) return true;
                    // @param (SQL Server parameter)
                    if (Regex.IsMatch(withoutStringLiterals, @"@[A-Za-z0-9_]+")) return true;
                    // :param (Oracle/Named parameter) - but NOT :: (PostgreSQL type cast)
                    // Use negative lookbehind to ensure : is not preceded by another :
                    if (Regex.IsMatch(withoutStringLiterals, @"(?<!:):[A-Za-z0-9_]+")) return true;
                    // $1, $2 positional (but not inside string literals)
                    if (Regex.IsMatch(withoutStringLiterals, @"\$[0-9]+")) return true;
                    // JDBC-style ? (but not inside string literals)
                    if (Regex.IsMatch(withoutStringLiterals, @"\?")) return true;
                    return false;
                }

                if (IsParameterized(expr))
                {
                    DebugWriteLine($"Row {r}: ERROR: contains parameter/template tokens");
                    continue;
                }

                SqlAnalysisResult analysis;
                try
                {
                    analysis = AnalyzeSql(expr, debugOutput);
                }
                catch (Exception ex)
                {
                    DebugWriteLine($"Error parsing expression at row {r}: {ex.Message}");
                    continue;
                }

                if (analysis.ParseErrors != null && analysis.ParseErrors.Length > 0)
                {
                    var msg = string.Join("; ", analysis.ParseErrors);
                    DebugWriteLine($"Row {r}: ERROR: {msg}");
                    continue;
                }

                // DEBUG: Print analysis results
                DebugWriteLine($"\n=== DEBUG Row {r} ===");
                DebugWriteLine($"CTEs found: {string.Join(", ", analysis.Ctes?.Select(c => c.Name) ?? new string[0])}");
                DebugWriteLine($"UnknownTables: {string.Join(", ", analysis.UnknownTables ?? new string[0])}");
                DebugWriteLine($"UnknownColumns: {string.Join(", ", analysis.UnknownColumns ?? new string[0])}");
                DebugWriteLine($"NonDatasetSchemas: {string.Join(", ", analysis.NonDatasetSchemas ?? new string[0])}");
                DebugWriteLine($"TableNames (allowed): {string.Join(", ", analysis.TableNames?.Take(10) ?? new string[0])}...");
                DebugWriteLine($"AllTempColumns (allowed): {string.Join(", ", analysis.AllTempColumns?.Take(10) ?? new string[0])}...");
                DebugWriteLine($"===================\n");

                if (analysis.UnknownTables.Length == 0 && analysis.UnknownColumns.Length == 0 && analysis.NonDatasetSchemas.Length == 0)
                {
                    // safe to transform
                    var transformed = TransformToPostgres(expr);
                    outputs.Add((r, expr, transformed));

                    // Print formatted result
                    Console.WriteLine($"row {r}:");
                    Console.WriteLine();
                    Console.WriteLine("====================================================");
                    Console.WriteLine(transformed);
                    Console.WriteLine("====================================================");
                    Console.WriteLine();
                }
                else
                {
                    var parts = new List<string>();
                    if (analysis.UnknownTables.Length > 0) parts.Add($"unknown tables: {string.Join(',', analysis.UnknownTables)}");
                    if (analysis.UnknownColumns.Length > 0) parts.Add($"unknown columns: {string.Join(',', analysis.UnknownColumns)}");
                    if (analysis.NonDatasetSchemas.Length > 0) parts.Add($"non-dataset schemas: {string.Join(',', analysis.NonDatasetSchemas)}");
                    var msg = parts.Count == 0 ? "unknown references" : string.Join("; ", parts);
                    DebugWriteLine($"Row {r}: ERROR: {msg}");
                }
            }

            // Write debug output to file
            File.WriteAllText("output_debugg.txt", debugOutput.ToString());
            Console.WriteLine($"Debug output written to output_debugg.txt");

            return outputs;
        }


        /// <summary>
        /// Converts a column name to CamelCase format used in DDL
        /// Example: "waterbodyidentifier" -> "waterBodyIdentifier"
        /// </summary>
        private string ConvertToCamelCase(string columnName)
        {
            if (string.IsNullOrWhiteSpace(columnName)) return columnName;
            
            // Check if it's already in CamelCase (has uppercase letters)
            if (columnName.Any(char.IsUpper))
            {
                return columnName;
            }
            
            // Use mapping if available
            string columnNameLower = columnName.ToLowerInvariant();
            if (ColumnNameMap.ContainsKey(columnNameLower))
            {
                return ColumnNameMap[columnNameLower];
            }
            
            // Fallback: return as-is (shouldn't happen if schema is correct)
            return columnName;
        }


        /// <summary>
        /// Generates INSERT SQL statements for qc_rules_internal table and writes them to output.txt
        /// Handles both SQL queries (transformed) and non-SQL expressions (patterns, JSON)
        /// </summary>
        public void GenerateInsertStatements(string outputPath = "output_etl.txt")
        {
            if (workbook == null)
            {
                Console.WriteLine("Error: Workbook is null");
                return;
            }

            var transformed = ProcessExpressionsFromExcel();
            var transformedByRow = transformed.ToDictionary(x => x.Row, x => x.Transformed);
            
            var sheet = workbook.GetSheetAt(0);
            var insertStatements = new List<string>();

            // Process all rows in the Excel file (from expressionsStartRow to expressionsEndRow)
            for (int r = expressionsStartRow; r <= expressionsEndRow; r++)
            {
                var row = sheet.GetRow(r);
                if (row == null) continue;

                // Get columns: A=0 (table_name), B=1 (column_name), C=2 (code), E=4 (description), F=5 (rule_param/expression), H=7 (severity), I=8 (additional context)
                string tableName = row.GetCell(0)?.ToString()?.Trim() ?? "";
                string columnName = row.GetCell(1)?.ToString()?.Trim() ?? "";
                string code = row.GetCell(2)?.ToString()?.Trim() ?? "";
                string description = row.GetCell(4)?.ToString()?.Trim() ?? "";
                string expression = row.GetCell(5)?.ToString()?.Trim() ?? "";
                string severity = row.GetCell(7)?.ToString()?.Trim() ?? "";
                string additionalContext = row.GetCell(8)?.ToString()?.Trim() ?? "";

                // Skip if required fields are empty
                if (string.IsNullOrWhiteSpace(tableName) || 
                    string.IsNullOrWhiteSpace(code))
                {
                    continue;
                }

                // Validate table name against predefined schema (case-insensitive)
                string tableNameKey = tableName; // Keep original case for lookup
                if (!table_columns.ContainsKey(tableNameKey))
                {
                    // Try lowercase version
                    tableNameKey = tableName.ToLowerInvariant();
                    if (!table_columns.ContainsKey(tableNameKey))
                    {
                        Console.WriteLine($"Skipping row {r}: Table '{tableName}' not in predefined schema");
                        continue;
                    }
                }

                // Validate column name if provided
                // Pattern: Excel column name (lowercase) -> DDL column name (CamelCase)
                if (!string.IsNullOrWhiteSpace(columnName))
                {
                    // Step 1: Convert Excel column name to lowercase for lookup
                    string columnNameLower = columnName.ToLowerInvariant();
                    
                    // Step 2: Look up in ColumnNameMap to get DDL column name (lowercase -> CamelCase)
                    string ddlColumnName = null;
                    if (ColumnNameMap.ContainsKey(columnNameLower))
                    {
                        ddlColumnName = ColumnNameMap[columnNameLower];
                    }
                    else
                    {
                        // Fallback: try ConvertToCamelCase
                        ddlColumnName = ConvertToCamelCase(columnName);
                    }
                    
                    // Step 3: Validate that this DDL column name exists in the table (case-insensitive)
                    bool columnFound = table_columns[tableNameKey].Any(col => 
                        col.Equals(ddlColumnName, StringComparison.OrdinalIgnoreCase));
                    
                    if (!columnFound)
                    {
                        // Last resort: direct case-insensitive lookup
                        columnFound = table_columns[tableNameKey].Any(col => 
                            col.ToLowerInvariant() == columnNameLower);
                        
                        if (columnFound)
                        {
                            ddlColumnName = table_columns[tableNameKey].First(col => 
                                col.ToLowerInvariant() == columnNameLower);
                        }
                        else
                        {
                            // Debug: show what we're looking for and what's available
                            Console.WriteLine($"\nDEBUG Row {r}: Column validation failed");
                            Console.WriteLine($"  Excel column name: '{columnName}'");
                            Console.WriteLine($"  Lowercase: '{columnNameLower}'");
                            Console.WriteLine($"  ColumnNameMap contains key? {ColumnNameMap.ContainsKey(columnNameLower)}");
                            if (ColumnNameMap.ContainsKey(columnNameLower))
                            {
                                Console.WriteLine($"  ColumnNameMap lookup: '{ColumnNameMap[columnNameLower]}'");
                            }
                            Console.WriteLine($"  ConvertToCamelCase result: '{ConvertToCamelCase(columnName)}'");
                            Console.WriteLine($"  Table '{tableName}' (key: '{tableNameKey}') has {table_columns[tableNameKey].Length} columns:");
                            foreach (var col in table_columns[tableNameKey])
                            {
                                bool matches = col.ToLowerInvariant() == columnNameLower;
                                Console.WriteLine($"    - '{col}' (lowercase: '{col.ToLowerInvariant()}') {(matches ? " <-- MATCHES!" : "")}");
                            }
                            Console.WriteLine($"Skipping row {r}: Column '{columnName}' not in predefined schema for table '{tableName}'");
                            continue;
                        }
                    }
                    else
                    {
                        // Get the exact DDL column name (preserves exact CamelCase from DDL)
                        ddlColumnName = table_columns[tableNameKey].First(col => 
                            col.Equals(ddlColumnName, StringComparison.OrdinalIgnoreCase));
                    }
                    
                    // Step 4: Update columnName to the correct DDL column name (CamelCase)
                    columnName = ddlColumnName;
                }

                string operatorCode;
                string? jsonString = null;
                bool ruleParamIsNull = false;
                string ruleLevel = "COLUMN";

                // CRITICAL: Check if this row has a transformed SQL query FIRST
                // ALL SQL expressions MUST have operator_code = 'SQL'
                if (transformedByRow.ContainsKey(r))
                {
                    // SQL query case - wrap in {"sql": "..."}
                    operatorCode = "SQL";
                    var jsonObject = new JsonObject
                    {
                        ["sql"] = transformedByRow[r]
                    };
                    jsonString = jsonObject.ToJsonString();
                }
                // Check for MATCH patterns in Column F (expression) - PATTERN operator_code
                else if (!string.IsNullOrWhiteSpace(expression) && expression.Contains("MATCH", StringComparison.OrdinalIgnoreCase))
                {
                    // Extract all MATCH patterns from expression
                    // Pattern: ( columnName MATCH "pattern" ) OR ( columnName MATCH "pattern" )
                    var matchPattern = new Regex(@"MATCH\s+""([^""]+)""", RegexOptions.IgnoreCase);
                    var matches = matchPattern.Matches(expression);
                    
                    if (matches.Count > 0)
                    {
                        operatorCode = "PATTERN";
                        var patterns = new List<string>();
                        foreach (Match match in matches)
                        {
                            patterns.Add(match.Groups[1].Value);
                        }
                        
                        // Create JSON with pattern(s)
                        var jsonObject = new JsonObject();
                        if (patterns.Count == 1)
                        {
                            jsonObject["pattern"] = patterns[0];
                        }
                        else
                        {
                            // Multiple patterns - store as array
                            var patternsArray = new JsonArray();
                            foreach (var pattern in patterns)
                            {
                                patternsArray.Add(pattern);
                            }
                            jsonObject["patterns"] = patternsArray;
                        }
                        jsonString = jsonObject.ToJsonString();
                    }
                    else
                    {
                        // MATCH keyword found but no valid pattern extracted - skip
                        Console.WriteLine($"Skipping row {r}: MATCH keyword found but no valid pattern extracted from expression: {expression}");
                        continue;
                    }
                }
                // Determine operator_code based on Column E (description) ONLY if NOT SQL and NOT PATTERN
                else
                {
                    string descriptionLower = description.ToLowerInvariant();
                    
                    // Check for UNIQUE constraint
                    // Pattern: "Checks if ColumnX, ColumnY, and ColumnZ are unique(s) within (the) table"
                    if (descriptionLower.StartsWith("checks if ") && (descriptionLower.Contains("unique") && descriptionLower.Contains("within")))
                    {
                        // Extract column names from description
                        operatorCode = "UNIQUE";
                        ruleLevel = "ROW";
                        columnName = null; // Set to NULL for ROW-level rules
                        
                        // Extract column names from description
                        // Pattern: "Checks if col1, col2, col3 and col4 are uniques within table"
                        var columnNames = new List<string>();
                        
                        // Try to extract from description - look for text between "Checks if " and " are unique"
                        // Handle both "are unique" and "are uniques" (plural)
                        var uniqueMatch = Regex.Match(description, @"checks\s+if\s+(.+?)\s+are\s+uniques?\s+within", RegexOptions.IgnoreCase);
                        if (uniqueMatch.Success)
                        {
                            string columnsText = uniqueMatch.Groups[1].Value;
                            
                            // Split by comma and "and" - handle patterns like "col1, col2, col3 and col4"
                            // Replace " and " with "," to normalize
                            columnsText = Regex.Replace(columnsText, @"\s+and\s+", ", ", RegexOptions.IgnoreCase);
                            
                            // Split by comma and clean up
                            var cols = columnsText.Split(',')
                                .Select(c => c.Trim())
                                .Where(c => !string.IsNullOrWhiteSpace(c))
                                .ToList();
                            
                            // Validate all columns exist in the table
                            bool allColumnsValid = true;
                            foreach (var col in cols)
                            {
                                string colLower = col.ToLowerInvariant();
                                bool colFound = table_columns[tableNameKey].Any(tc => tc.ToLowerInvariant() == colLower);
                                if (!colFound)
                                {
                                    Console.WriteLine($"Skipping row {r}: Column '{col}' in UNIQUE constraint not in predefined schema for table '{tableName}'");
                                    allColumnsValid = false;
                                    break;
                                }
                                // Find the actual column name from the table_columns (preserves CamelCase)
                                string actualColName = table_columns[tableNameKey].First(tc => tc.ToLowerInvariant() == colLower);
                                columnNames.Add(actualColName);
                            }
                            
                            if (!allColumnsValid)
                            {
                                continue;
                            }
                        }
                        else
                        {
                            // Try alternative pattern or use columnName if single column
                            if (!string.IsNullOrWhiteSpace(columnName))
                            {
                                columnNames.Add(ConvertToCamelCase(columnName));
                            }
                            else
                            {
                                Console.WriteLine($"Skipping row {r}: Could not extract column names from UNIQUE description: {description}");
                                continue;
                            }
                        }
                        
                        // Create JSON with "unique" key containing columns array
                        var jsonObject = new JsonObject();
                        var columnsArray = new JsonArray();
                        foreach (var col in columnNames)
                        {
                            columnsArray.Add(col);
                        }
                        jsonObject["unique"] = columnsArray;
                        jsonString = jsonObject.ToJsonString();
                    }
                else if (descriptionLower.Contains("missing or empty") || descriptionLower.Contains("field is missing"))
                {
                    // NOT_NULL_NOT_EMPTY - expression should be empty
                    operatorCode = "NOT_NULL_NOT_EMPTY";
                    ruleParamIsNull = true;
                }
                    else if (descriptionLower.Contains("valid date") || descriptionLower.Contains("is a valid date"))
                {
                    // IS_DATE - check if Column I has date pattern info
                    operatorCode = "IS_DATE";
                    if (additionalContext.ToLowerInvariant().Contains("yyyy-mm-dd") || 
                        additionalContext.ToLowerInvariant().Contains("valid date"))
                    {
                        var jsonObject = new JsonObject
                        {
                            ["pattern"] = "[0-9]{4}-[0-9]{2}-[0-9]{2}"
                        };
                        jsonString = jsonObject.ToJsonString();
                    }
                    else if (!string.IsNullOrWhiteSpace(expression))
                    {
                        // Try to parse expression as JSON, or use it as pattern
                        try
                        {
                            var testJson = JsonObject.Parse(expression);
                            jsonString = expression;
                        }
                        catch
                        {
                            var jsonObject = new JsonObject
                            {
                                ["pattern"] = expression
                            };
                            jsonString = jsonObject.ToJsonString();
                        }
                    }
                    else
                    {
                        var jsonObject = new JsonObject
                        {
                            ["pattern"] = "[0-9]{4}-[0-9]{2}-[0-9]{2}"
                        };
                        jsonString = jsonObject.ToJsonString();
                    }
                }
                    else if (descriptionLower.Contains("valid number") && descriptionLower.Contains("decimal"))
                {
                    // IS_DECIMAL
                    operatorCode = "IS_DECIMAL";
                    ruleParamIsNull = true;
                }
                    else if (descriptionLower.Contains("number") && descriptionLower.Contains("integer"))
                {
                    // IS_INTEGER
                    operatorCode = "IS_INTEGER";
                    ruleParamIsNull = true;
                }
                    else if (descriptionLower.Contains("valid link") || descriptionLower.Contains("singleselect_codelist"))
                {
                    // FOREIGN_KEY - need to look up from DDL
                    operatorCode = "FOREIGN_KEY";
                    
                    // Find foreign key mapping
                    if (ForeignKeyMap.ContainsKey(tableName) && 
                        ForeignKeyMap[tableName].ContainsKey(columnName))
                    {
                        var fk = ForeignKeyMap[tableName][columnName];
                        var jsonObject = new JsonObject
                        {
                            ["table"] = $"\"{fk.Table}\"",
                            ["column"] = $"\"{fk.Column}\"",
                            ["schema"] = fk.Schema
                        };
                        jsonString = jsonObject.ToJsonString();
                    }
                    else
                    {
                        // No foreign key found, skip
                        Console.WriteLine($"Skipping row {r}: No foreign key mapping found for {tableName}.{columnName}");
                        continue;
                    }
                    }
                    else
                    {
                        // If expression exists but not SQL, try to parse as JSON
                        if (!string.IsNullOrWhiteSpace(expression))
                        {
                            try
                            {
                                var testJson = JsonObject.Parse(expression);
                                jsonString = expression;
                                // If it's valid JSON but we don't know the operator, skip it
                                Console.WriteLine($"Skipping row {r}: Expression is JSON but no matching operator_code found for description: {description}");
                                continue;
                            }
                            catch
                            {
                                // Not valid JSON and not SQL - skip
                                Console.WriteLine($"Skipping row {r}: Expression is not SQL, not JSON, and no matching operator_code found for description: {description}");
                                continue;
                            }
                        }
                        else
                        {
                            // No expression and no matching description - skip
                            Console.WriteLine($"Skipping row {r}: No expression and no matching operator_code found for description: {description}");
                            continue;
                        }
                    }
                }
                
                // Build INSERT statement
                string ruleParamValue;
                if (ruleParamIsNull)
                {
                    ruleParamValue = "NULL";
                }
                else if (jsonString != null)
                {
                    // Escape single quotes for SQL string literal
                    string escapedJson = jsonString.Replace("'", "''");
                    ruleParamValue = $"'{escapedJson}'::jsonb";
                }
                else
                {
                    ruleParamValue = "NULL";
                }

                // Get target schema from SchemaMap (use first target schema if available)
                string targetSchema = SchemaMap.Values.FirstOrDefault() ?? "rod14_wise6"; // fallback for backward compatibility
                string insertSql = $"INSERT INTO {targetSchema}.qc_rules_internal (code, table_name, column_name, rule_level, operator_code, rule_param, severity, enabled) VALUES ('{code.Replace("'", "''")}', '{tableName.Replace("'", "''")}', {(string.IsNullOrWhiteSpace(columnName) ? "NULL" : $"'{columnName.Replace("'", "''")}'")}, '{ruleLevel}', '{operatorCode}', {ruleParamValue}, {(string.IsNullOrWhiteSpace(severity) ? "NULL" : $"'{severity.Replace("'", "''")}'")}, true);";

                insertStatements.Add(insertSql);
            }

            // Write to file
            File.WriteAllLines(outputPath, insertStatements);
            Console.WriteLine($"Generated {insertStatements.Count} INSERT statements in {outputPath}");
        }

    }
}