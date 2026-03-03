using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;

namespace ETL_rod787.Services
{
    public class QCTransformatorMSSQL : QCTransformator14
    {
        /// <summary>
        /// Converts [identifier] to "identifier" but STRICTLY skips string literals to preserve regex patterns.
        /// </summary>
        private string ProtectStringLiteralsForBracketConversion(string sql)
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
                        inSingleQuote = true;
                        stringStart = i;
                    }
                    else
                    {
                        if (i + 1 < sql.Length && sql[i + 1] == '\'')
                        {
                            i++; // Skip escaped quote
                        }
                        else
                        {
                            inSingleQuote = false;
                            stringLiterals.Add(sql.Substring(stringStart, i - stringStart + 1));
                            result.Append($"__STR_LIT_{stringLiterals.Count - 1}__");
                        }
                    }
                }
                else if (!inSingleQuote)
                {
                    result.Append(ch);
                }
            }
            
            if (inSingleQuote)
            {
                stringLiterals.Add(sql.Substring(stringStart));
                result.Append($"__STR_LIT_{stringLiterals.Count - 1}__");
            }
            
            // Convert brackets to quotes (only outside string literals)
            // CRITICAL: The placeholders don't contain brackets, so this regex will only match
            // actual SQL identifier brackets like [table] or [schema].[table], NOT regex patterns
            // which are safely inside the placeholders
            string transformed = Regex.Replace(result.ToString(), @"\[(?<id>[^\]]+)\]", "\"${id}\"");
            
            // Restore string literals BEFORE any control character removal
            for (int i = stringLiterals.Count - 1; i >= 0; i--)
            {
                transformed = transformed.Replace($"__STR_LIT_{i}__", stringLiterals[i]);
            }
            
            return transformed;
        }

        // Column type dictionary - uses instance dictionary from base class
        protected Dictionary<string, string> ColumnTypes => _columnTypes;

        public QCTransformatorMSSQL(string path, string ddl, int expressionsStartRow, int expressionsEndRow, Dictionary<string, string>? schemaMap = null,
            Dictionary<(string Schema, string Table), string>? schemaTableMap = null,
            int? columnIndexTableName = null, int? columnIndexColumnName = null, int? columnIndexCode = null,
            int? columnIndexDescription = null, int? columnIndexExpression = null, int? columnIndexSeverity = null, int? columnIndexAdditionalContext = null) 
            : base(path, ddl, expressionsStartRow, expressionsEndRow, schemaMap, schemaTableMap,
                columnIndexTableName, columnIndexColumnName, columnIndexCode, columnIndexDescription, columnIndexExpression, columnIndexSeverity, columnIndexAdditionalContext)
        {
        }

        /// <summary>
        /// Fixes numeric and boolean column comparisons by removing quotes from numeric/boolean literals and removing empty string checks.
        /// Changes "columnName" = '0' to "columnName" = 0 for numeric columns.
        /// Changes "columnName" = '0' to "columnName" = false and "columnName" = '1' to "columnName" = true for boolean columns.
        /// Removes "columnName" = '' for both numeric and boolean columns.
        /// Uses the column type dictionary to identify column types dynamically.
        /// </summary>
        private string FixNumericColumnComparisons(string sql)
        {
            if (string.IsNullOrEmpty(sql)) return sql;

            // Get numeric columns from the type dictionary (int4, numeric types)
            var numericColumns = ColumnTypes
                .Where(kv => kv.Value == "int4" || kv.Value.StartsWith("numeric", StringComparison.OrdinalIgnoreCase))
                .Select(kv => kv.Key)
                .ToHashSet(StringComparer.OrdinalIgnoreCase);

            // Get boolean columns from the type dictionary
            var booleanColumns = ColumnTypes
                .Where(kv => kv.Value == "bool")
                .Select(kv => kv.Key)
                .ToHashSet(StringComparer.OrdinalIgnoreCase);

            // For each numeric column, fix comparisons
            foreach (var colName in numericColumns)
            {
                var escapedColName = Regex.Escape(colName);
                
                // Pattern: "columnName" operator 'number' -> "columnName" operator number
                // Handles: =, <>, !=, <, >, <=, >=
                // Matches: "columnName" = '0', "columnName" ='0', "columnName"='0', "columnName" = '-1.5'
                // Handles positive/negative integers and decimals
                var quotedNumberPattern = $@"""({escapedColName})""\s*(=|<>|!=|<|>|<=|>=)\s*'([+-]?[0-9]+(?:\.[0-9]+)?)'";
                sql = Regex.Replace(sql, quotedNumberPattern, @"""$1"" $2 $3", RegexOptions.IgnoreCase);

                // DO NOT remove empty string checks - they will be handled by AddTextCastsForStringComparisons
                // Empty string comparisons (= '', <>, != '') will be cast to ::text automatically
            }

            // For each boolean column, fix comparisons (convert '0' to false, '1' to true)
            foreach (var colName in booleanColumns)
            {
                var escapedColName = Regex.Escape(colName);
                
                // Pattern: "columnName" = '0' -> "columnName" = false
                var boolFalsePattern = $@"""({escapedColName})""\s*=\s*'0'";
                sql = Regex.Replace(sql, boolFalsePattern, @"""$1"" = false", RegexOptions.IgnoreCase);
                
                // Pattern: "columnName" = '1' -> "columnName" = true
                var boolTruePattern = $@"""({escapedColName})""\s*=\s*'1'";
                sql = Regex.Replace(sql, boolTruePattern, @"""$1"" = true", RegexOptions.IgnoreCase);
                
                // Pattern: "columnName" <> '0' -> "columnName" <> false (which is = true)
                var boolNotFalsePattern = $@"""({escapedColName})""\s*<>\s*'0'";
                sql = Regex.Replace(sql, boolNotFalsePattern, @"""$1"" = true", RegexOptions.IgnoreCase);
                
                // Pattern: "columnName" <> '1' -> "columnName" <> true (which is = false)
                var boolNotTruePattern = $@"""({escapedColName})""\s*<>\s*'1'";
                sql = Regex.Replace(sql, boolNotTruePattern, @"""$1"" = false", RegexOptions.IgnoreCase);
                
                // Pattern: "columnName" != '0' -> "columnName" != false (which is = true)
                var boolNotEqualFalsePattern = $@"""({escapedColName})""\s*!=\s*'0'";
                sql = Regex.Replace(sql, boolNotEqualFalsePattern, @"""$1"" = true", RegexOptions.IgnoreCase);
                
                // Pattern: "columnName" != '1' -> "columnName" != true (which is = false)
                var boolNotEqualTruePattern = $@"""({escapedColName})""\s*!=\s*'1'";
                sql = Regex.Replace(sql, boolNotEqualTruePattern, @"""$1"" = false", RegexOptions.IgnoreCase);

                // DO NOT remove empty string checks - they will be handled by AddTextCastsForStringComparisons
                // Empty string comparisons (= '', <>, != '') will be cast to ::text automatically
            }

            // Clean up any double OR/AND operators or malformed conditions that might result from removals
            sql = Regex.Replace(sql, @"\s+OR\s+OR\s+", " OR ", RegexOptions.IgnoreCase);
            sql = Regex.Replace(sql, @"\s+AND\s+OR\s+", " OR ", RegexOptions.IgnoreCase);
            sql = Regex.Replace(sql, @"\s+OR\s+AND\s+", " AND ", RegexOptions.IgnoreCase);
            sql = Regex.Replace(sql, @"\s+AND\s+AND\s+", " AND ", RegexOptions.IgnoreCase);
            sql = Regex.Replace(sql, @"\band\s+AND\s+", " AND ", RegexOptions.IgnoreCase);
            sql = Regex.Replace(sql, @"\bAND\s+and\s+", " AND ", RegexOptions.IgnoreCase);
            sql = Regex.Replace(sql, @"\bor\s+OR\s+", " OR ", RegexOptions.IgnoreCase);
            sql = Regex.Replace(sql, @"\bOR\s+or\s+", " OR ", RegexOptions.IgnoreCase);
            sql = Regex.Replace(sql, @"\(\s+OR\s+", "(", RegexOptions.IgnoreCase);
            sql = Regex.Replace(sql, @"\s+OR\s+\)", ")", RegexOptions.IgnoreCase);
            sql = Regex.Replace(sql, @"\(\s+AND\s+", "(", RegexOptions.IgnoreCase);
            sql = Regex.Replace(sql, @"\s+AND\s+\)", ")", RegexOptions.IgnoreCase);
            sql = Regex.Replace(sql, @"WHERE\s+OR\s+", "WHERE ", RegexOptions.IgnoreCase);
            sql = Regex.Replace(sql, @"WHERE\s+AND\s+", "WHERE ", RegexOptions.IgnoreCase);
            sql = Regex.Replace(sql, @"\band\s+AND\s+", " AND ", RegexOptions.IgnoreCase);
            sql = Regex.Replace(sql, @"\s+and\s+AND\s+", " AND ", RegexOptions.IgnoreCase);
            sql = Regex.Replace(sql, @"\s+AND\s+and\s+", " AND ", RegexOptions.IgnoreCase);

            return sql;
        }

        /// <summary>
        /// Removes numeric regex pattern checks for numeric and boolean columns.
        /// DISABLED: ISNUMERIC checks should always be transformed and never removed.
        /// </summary>
        private string RemoveIsNumericForNumericColumns(string sql)
        {
            // Do nothing - ISNUMERIC checks should always be kept, even for numeric columns
            return sql;
        }

        /// <summary>
        /// Removes invalid CAST operations based on column types.
        /// - Boolean columns cannot be cast to numeric
        /// - Casting to the same type is redundant
        /// - Other invalid casts based on PostgreSQL type compatibility
        /// </summary>
        private string RemoveInvalidBooleanCasts(string sql)
        {
            if (string.IsNullOrEmpty(sql)) return sql;

            // Process all columns in the type dictionary
            foreach (var kv in ColumnTypes)
            {
                var colName = kv.Key;
                var columnType = kv.Value;
                var escapedColName = Regex.Escape(colName);
                
                // Pattern to match CAST("columnName" AS type)
                var castPattern = $@"CAST\s*\(\s*""{escapedColName}""\s+AS\s+(\w+)\s*(?:\(\s*[^)]+\s*\))?\s*\)";
                var matches = Regex.Matches(sql, castPattern, RegexOptions.IgnoreCase);
                
                // Process matches in reverse order to maintain indices
                for (int i = matches.Count - 1; i >= 0; i--)
                {
                    var match = matches[i];
                    string targetType = match.Groups[1].Value.ToLower();
                    
                    bool shouldRemove = false;
                    
                    // Boolean columns cannot be cast to numeric
                    if (columnType == "bool" && (targetType == "numeric" || targetType.StartsWith("numeric")))
                    {
                        shouldRemove = true;
                    }
                    // Numeric columns cannot be cast to boolean
                    else if ((columnType == "int4" || columnType.StartsWith("numeric", StringComparison.OrdinalIgnoreCase)) && targetType == "bool")
                    {
                        shouldRemove = true;
                    }
                    // Casting to the same type is redundant (but keep NUMERIC casts - they may have precision/scale)
                    else if ((columnType == "int4" && targetType == "int4") ||
                             (columnType == "bool" && targetType == "bool") ||
                             (columnType == "text" && (targetType == "text" || targetType == "varchar")))
                    {
                        shouldRemove = true;
                    }
                    // DO NOT remove NUMERIC casts - they may specify precision/scale like NUMERIC(32,16)
                    
                    if (shouldRemove)
                    {
                        // Replace CAST with just the column name
                        sql = sql.Substring(0, match.Index) + $"\"{colName}\"" + sql.Substring(match.Index + match.Length);
                    }
                }
            }

            return sql;
        }

        /// <summary>
        /// Adds ::text casts to column comparisons with empty strings or text literals.
        /// Pattern: "columnName" = '' -> "columnName"::text = ''
        /// Pattern: "columnName" LIKE '' -> "columnName"::text LIKE ''
        /// Pattern: "columnName" = 'text' -> "columnName"::text = 'text'
        /// Avoids double-casting if column already has ::text cast.
        /// </summary>
        private string AddTextCastsForStringComparisons(string sql)
        {
            if (string.IsNullOrEmpty(sql)) return sql;

            // Get all column names from ColumnNameMap (these are the actual database column names after transformation)
            var columnNames = new HashSet<string>(ColumnNameMap.Values, StringComparer.OrdinalIgnoreCase);
            // Also include keys (original lowercase names) in case they weren't transformed
            foreach (var kv in ColumnNameMap)
            {
                columnNames.Add(kv.Key);
            }
            
            foreach (var colName in columnNames)
            {
                var escapedColName = Regex.Escape(colName);
                
                // Pattern: "columnName" = '' -> "columnName"::text = '' (but not if already has ::text)
                sql = Regex.Replace(sql, $@"""({escapedColName})""(?!\s*::text)\s*=\s*''", @"""$1""::text = ''", RegexOptions.IgnoreCase);
                
                // Pattern: "columnName" = 'text' -> "columnName"::text = 'text' (but not if already has ::text)
                // Match quoted column followed by = and a string literal
                sql = Regex.Replace(sql, $@"""({escapedColName})""(?!\s*::text)\s*=\s*('[^']*')", @"""$1""::text = $2", RegexOptions.IgnoreCase);
                
                // Pattern: "columnName" LIKE '' -> "columnName"::text LIKE '' (but not if already has ::text)
                sql = Regex.Replace(sql, $@"""({escapedColName})""(?!\s*::text)\s+LIKE\s+''", @"""$1""::text LIKE ''", RegexOptions.IgnoreCase);
                
                // Pattern: "columnName" LIKE 'text' -> "columnName"::text LIKE 'text' (but not if already has ::text)
                sql = Regex.Replace(sql, $@"""({escapedColName})""(?!\s*::text)\s+LIKE\s+('[^']*')", @"""$1""::text LIKE $2", RegexOptions.IgnoreCase);
                
                // Pattern: "columnName" <> '' -> "columnName"::text <> '' (but not if already has ::text)
                sql = Regex.Replace(sql, $@"""({escapedColName})""(?!\s*::text)\s*<>\s*''", @"""$1""::text <> ''", RegexOptions.IgnoreCase);
                
                // Pattern: "columnName" != '' -> "columnName"::text != '' (but not if already has ::text)
                sql = Regex.Replace(sql, $@"""({escapedColName})""(?!\s*::text)\s*!=\s*''", @"""$1""::text != ''", RegexOptions.IgnoreCase);
                
                // Pattern: "columnName" <> 'text' -> "columnName"::text <> 'text' (but not if already has ::text)
                sql = Regex.Replace(sql, $@"""({escapedColName})""(?!\s*::text)\s*<>\s*('[^']*')", @"""$1""::text <> $2", RegexOptions.IgnoreCase);
                
                // Pattern: "columnName" != 'text' -> "columnName"::text != 'text' (but not if already has ::text)
                sql = Regex.Replace(sql, $@"""({escapedColName})""(?!\s*::text)\s*!=\s*('[^']*')", @"""$1""::text != $2", RegexOptions.IgnoreCase);
            }
            
            return sql;
        }

        /// <summary>
        /// Removes quotes from unqualified CTE references in FROM/JOIN clauses.
        /// Rule: If a table reference has no schema prefix, it's a CTE and should be unquoted.
        /// Pattern: FROM "cteName" -> FROM cteName
        /// Pattern: JOIN "cteName" -> JOIN cteName
        /// </summary>
        private string RemoveQuotesFromUnqualifiedCteReferences(string sql, HashSet<string> cteNames)
        {
            if (string.IsNullOrEmpty(sql) || cteNames == null || cteNames.Count == 0) return sql;

            // Pattern to match FROM/JOIN with quoted table name (no schema prefix)
            // Match: FROM "tableName" or JOIN "tableName" (but not schema."tableName")
            // Process all CTE names case-insensitively
            foreach (var cteName in cteNames)
            {
                var escapedCteName = Regex.Escape(cteName);
                
                // Pattern: FROM "cteName" -> FROM cteName (but not if it has schema prefix before)
                // Use negative lookbehind to ensure no schema prefix (no word character or dot before FROM/JOIN)
                // Match: FROM "disaggregateddata" AS -> FROM disaggregateddata AS
                sql = Regex.Replace(sql, $@"(?<![\w\.])(FROM|JOIN)\s+""({escapedCteName})""(?=\s|$|AS)", 
                    $"$1 $2", RegexOptions.IgnoreCase);
            }

            return sql;
        }

        /// <summary>
        /// Basic transformation of an MSSQL query into a Postgres-compatible query.
        /// This performs shallow, heuristic rewrites only (identifiers, some functions, TOP -> LIMIT, dbo schema -> dataset_93286).
        /// Then maps schema/table/column names to rod14_wise6 DDL naming.
        /// </summary>
        public override string TransformToPostgres(string mssqlSql)
        {
            // Check if query is already PostgreSQL - if so, only do schema/table/column name mappings
            bool isPostgres = IsPostgresQuery(mssqlSql);
            
            // Extract CTE names from original SQL BEFORE any transformations
            // This is needed to distinguish CTE references from actual table references
            // Store both original case and lowercase versions to protect CTE names from transformation
            var cteNames = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
            var cteNamesOriginalCase = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
            
            // Match: WITH "cteName" AS ( or WITH cteName AS (
            var withPattern = new Regex(@"WITH\s+(\w+|""[^""]+"")\s+AS\s*\(", RegexOptions.IgnoreCase);
            var withMatches = withPattern.Matches(mssqlSql);
            foreach (Match match in withMatches)
            {
                string cteName = match.Groups[1].Value.Trim('"');
                if (!string.IsNullOrEmpty(cteName))
                {
                    cteNames.Add(cteName);
                    cteNames.Add(cteName.ToLowerInvariant());
                    cteNamesOriginalCase[cteName.ToLowerInvariant()] = cteName; // Store original case
                }
            }
            
            // Match: , "cteName" AS ( or , cteName AS (
            var cteListPattern = new Regex(@",\s*(\w+|""[^""]+"")\s+AS\s*\(", RegexOptions.IgnoreCase);
            var cteListMatches = cteListPattern.Matches(mssqlSql);
            foreach (Match match in cteListMatches)
            {
                string cteName = match.Groups[1].Value.Trim('"');
                if (!string.IsNullOrEmpty(cteName))
                {
                    cteNames.Add(cteName);
                    cteNames.Add(cteName.ToLowerInvariant());
                    cteNamesOriginalCase[cteName.ToLowerInvariant()] = cteName; // Store original case
                }
            }
            
            string sql = mssqlSql;
            
            // If already PostgreSQL, only do minimal transformations
            if (isPostgres)
            {
                // Convert REGEXP_MATCHES to REGEXP_LIKE (REGEXP_LIKE stays the same)
                sql = Regex.Replace(sql, @"\bREGEXP_MATCHES\b", "REGEXP_LIKE", RegexOptions.IgnoreCase);
                
                // Replace template tokens with hardcoded values
                sql = Regex.Replace(sql, @"'\{%R3_COUNTRY_CODE%\}'", "'ME'", RegexOptions.IgnoreCase);
            }
            
            // If already PostgreSQL, skip NormalizeSql entirely to avoid mangling regex patterns
            // Only do minimal normalization for PostgreSQL queries that STRICTLY preserves string literals
            string normalizedInput;
            if (isPostgres)
            {
                // Extract string literals first
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
                            inSingleQuote = true;
                            stringStart = i;
                        }
                        else
                        {
                            if (i + 1 < sql.Length && sql[i + 1] == '\'')
                            {
                                i++; // Skip escaped quote
                            }
                            else
                            {
                                inSingleQuote = false;
                                stringLiterals.Add(sql.Substring(stringStart, i - stringStart + 1));
                                result.Append($"__STR_LIT_{stringLiterals.Count - 1}__");
                            }
                        }
                    }
                    else if (!inSingleQuote)
                    {
                        result.Append(ch);
                    }
                }
                if (inSingleQuote)
                {
                    stringLiterals.Add(sql.Substring(stringStart));
                    result.Append($"__STR_LIT_{stringLiterals.Count - 1}__");
                }
                
                // Do minimal normalization on non-string parts only
                string nonStringSql = result.ToString();
                nonStringSql = nonStringSql.TrimStart('\uFEFF');
                nonStringSql = nonStringSql.Replace('\u2018', '\'').Replace('\u2019', '\'');
                nonStringSql = nonStringSql.Replace('\u201C', '"').Replace('\u201D', '"');
                var invisibleChars = new[] { '\u200E', '\u200F', '\u200D', '\u2060', '\uFEFF', '\u2028', '\u2029' };
                foreach (var ch in invisibleChars) nonStringSql = nonStringSql.Replace(ch.ToString(), string.Empty);
                nonStringSql = nonStringSql.Replace('\u00A0', ' ');
                nonStringSql = nonStringSql.Replace("\r\n", "\n").Replace("\r", "\n");
                nonStringSql = Regex.Replace(nonStringSql, "[ \t]{2,}", " ");
                nonStringSql = nonStringSql.Trim();
                
                // Restore string literals BEFORE removing control characters
                for (int i = stringLiterals.Count - 1; i >= 0; i--)
                {
                    nonStringSql = nonStringSql.Replace($"__STR_LIT_{i}__", stringLiterals[i]);
                }
                
                // Now remove control characters (but string literals are already restored)
                normalizedInput = Regex.Replace(nonStringSql, "[\u0000-\u0008\u000B\u000C\u000E-\u001F]", string.Empty);
            }
            else
            {
                // For SQL Server, use full NormalizeSql
                normalizedInput = NormalizeSql(sql);
            }

            // let base parse and validate CTEs using normalized SQL (will skip parsing if PostgreSQL)
            var ctes = parseSQL(normalizedInput);

            sql = normalizedInput;
            
            // If already PostgreSQL, skip SQL Server to PostgreSQL transformations
            if (isPostgres)
            {
                // For PostgreSQL, we've already done minimal normalization that preserves string literals
                // No need to convert brackets - PostgreSQL uses double quotes for identifiers
                // Only apply schema/table/column name mappings for PostgreSQL queries
                // Skip steps 1-4 (SQL Server specific transformations)
                goto ApplyNameMappings;
            }

            // 1) Replace square-bracket identifiers [name] -> "name"
            // BUT skip string literals to preserve regex patterns
            sql = ProtectStringLiteralsForBracketConversion(sql);

            // 2) Replace common dbo. schema usages with first source schema from SchemaMap
            // Use the first source schema (key) from SchemaMap as the replacement target
            string dboReplacementSchema = SchemaMap.Keys.FirstOrDefault() ?? "dataset_93286"; // fallback for backward compatibility
            sql = sql.Replace($"dbo.\"", $"{dboReplacementSchema}.\"", StringComparison.OrdinalIgnoreCase);
            sql = Regex.Replace(sql, @"\bdbo\.", $"{dboReplacementSchema}.", RegexOptions.IgnoreCase);

            // 2.5) Convert ISNUMERIC(expr) -> (expr::text ~ 'numeric-regex') to emulate SQL Server ISNUMERIC in Postgres
            // Always transform ISNUMERIC to regex pattern check
            // Handle ISNUMERIC by finding matching parentheses
            var isnumericPattern = new Regex(@"\bISNUMERIC\s*\(", RegexOptions.IgnoreCase);
            var matches = isnumericPattern.Matches(sql);
            for (int i = matches.Count - 1; i >= 0; i--)
            {
                var match = matches[i];
                int startPos = match.Index + match.Length;
                int parenDepth = 1;
                int endPos = startPos;
                
                // Find matching closing parenthesis, handling string literals
                bool inString = false;
                char stringDelimiter = '\0';
                while (endPos < sql.Length && parenDepth > 0)
                {
                    char ch = sql[endPos];
                    
                    // Track string literals
                    if (!inString && (ch == '\'' || ch == '"'))
                    {
                        inString = true;
                        stringDelimiter = ch;
                    }
                    else if (inString && ch == stringDelimiter)
                    {
                        // Check for escaped quote
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
                    // Extract the expression
                    string expr = sql.Substring(startPos, endPos - startPos - 1).Trim();
                    
                    // Replace ISNUMERIC(expr) with (expr::text ~ E'numeric-regex')
                    // Pattern: ^\s*[+-]?((\d+(\.\d*)?)|(\.\d+))([eE][+-]?\d+)?\s*$
                    string replacement = $"({expr}::text ~ E'^\\\\s*[+-]?((\\\\d+(\\\\.\\\\d*)?)|(\\\\.\\\\d+))([eE][+-]?\\\\d+)?\\\\s*$')";
                    sql = sql.Substring(0, match.Index) + replacement + sql.Substring(endPos);
                }
            }

            // 2.6) Convert ISDATE(expr) -> (expr::text ~ E'date-regex') to emulate SQL Server ISDATE in Postgres
            // Always transform ISDATE to regex pattern check
            // Handle ISDATE by finding matching parentheses
            var isdatePattern = new Regex(@"\bISDATE\s*\(", RegexOptions.IgnoreCase);
            var isdateMatches = isdatePattern.Matches(sql);
            for (int i = isdateMatches.Count - 1; i >= 0; i--)
            {
                var match = isdateMatches[i];
                int startPos = match.Index + match.Length;
                int parenDepth = 1;
                int endPos = startPos;
                
                // Find matching closing parenthesis, handling string literals
                bool inString = false;
                char stringDelimiter = '\0';
                while (endPos < sql.Length && parenDepth > 0)
                {
                    char ch = sql[endPos];
                    
                    // Track string literals
                    if (!inString && (ch == '\'' || ch == '"'))
                    {
                        inString = true;
                        stringDelimiter = ch;
                    }
                    else if (inString && ch == stringDelimiter)
                    {
                        // Check for escaped quote
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
                    // Extract the expression
                    string expr = sql.Substring(startPos, endPos - startPos - 1).Trim();
                    
                    // Replace ISDATE(expr) with PostgreSQL date validation
                    // Use specific regex pattern: E'^(19|20)\\d\\d-(0[1-9]|1[0-2])-(0[1-9]|[12]\\d|3[01])$'
                    string replacement = $"({expr}::text ~ E'^(19|20)\\\\d\\\\d-(0[1-9]|1[0-2])-(0[1-9]|[12]\\\\d|3[01])$')";
                    sql = sql.Substring(0, match.Index) + replacement + sql.Substring(endPos);
                }
            }

            // 2.7) Convert TO_DATE(value, format, 1) -> TO_DATE(value::text, format)
            // Handle TO_DATE with 3 parameters, removing the third parameter and adding ::text cast to first parameter
            var toDatePattern = new Regex(@"\bTO_DATE\s*\(", RegexOptions.IgnoreCase);
            var toDateMatches = toDatePattern.Matches(sql);
            for (int i = toDateMatches.Count - 1; i >= 0; i--)
            {
                var match = toDateMatches[i];
                int startPos = match.Index + match.Length;
                int parenDepth = 1;
                int endPos = startPos;
                int commaCount = 0;
                int firstCommaPos = -1;
                int secondCommaPos = -1;
                
                // Find matching closing parenthesis, handling nested parentheses and string literals
                bool inString = false;
                char stringDelimiter = '\0';
                while (endPos < sql.Length && parenDepth > 0)
                {
                    char ch = sql[endPos];
                    
                    // Track string literals
                    if (!inString && (ch == '\'' || ch == '"'))
                    {
                        inString = true;
                        stringDelimiter = ch;
                    }
                    else if (inString && ch == stringDelimiter)
                    {
                        // Check for escaped quote
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
                        else if (ch == ',' && parenDepth == 1)
                        {
                            commaCount++;
                            if (commaCount == 1) firstCommaPos = endPos;
                            else if (commaCount == 2) secondCommaPos = endPos;
                        }
                    }
                    endPos++;
                }
                
                if (parenDepth == 0 && commaCount == 2)
                {
                    // TO_DATE has 3 parameters - transform to 2 parameters with ::text cast
                    string firstParam = sql.Substring(startPos, firstCommaPos - startPos).Trim();
                    string secondParam = sql.Substring(firstCommaPos + 1, secondCommaPos - firstCommaPos - 1).Trim();
                    
                    // Add ::text cast to first parameter if not already present
                    if (!firstParam.Contains("::text"))
                    {
                        // Check if it already has a cast or is a simple identifier
                        if (firstParam.StartsWith("\"") && firstParam.EndsWith("\""))
                        {
                            firstParam = firstParam + "::text";
                        }
                        else if (Regex.IsMatch(firstParam, @"^""[^""]+""$"))
                        {
                            firstParam = firstParam + "::text";
                        }
                        else
                        {
                            firstParam = $"({firstParam})::text";
                        }
                    }
                    
                    string replacement = $"TO_DATE({firstParam}, {secondParam})";
                    sql = sql.Substring(0, match.Index) + replacement + sql.Substring(endPos);
                }
            }

            // 3) Replace GETDATE() -> CURRENT_TIMESTAMP
            sql = Regex.Replace(sql, @"\bGETDATE\s*\(\s*\)", "CURRENT_TIMESTAMP", RegexOptions.IgnoreCase);

            // 4) Replace ISNULL(a, b) -> COALESCE(a, b)
            sql = Regex.Replace(sql, @"\bISNULL\s*\(", "COALESCE(", RegexOptions.IgnoreCase);

            // 5) Convert TOP n in SELECT to LIMIT n at the end (simple heuristic, handles single-statement queries)
            var topMatch = Regex.Match(sql, @"^\s*SELECT\s+TOP\s+([0-9]+)\s+", RegexOptions.IgnoreCase);
            if (topMatch.Success)
            {
                var n = topMatch.Groups[1].Value;
                // remove TOP n
                sql = Regex.Replace(sql, @"(SELECT)\s+TOP\s+[0-9]+\s+", "$1 ", RegexOptions.IgnoreCase);
                // append LIMIT if not already present
                if (!Regex.IsMatch(sql, "\bLIMIT\b", RegexOptions.IgnoreCase))
                {
                    sql = sql.Trim();
                    if (!sql.EndsWith(";")) sql += " ";
                    sql += $"LIMIT {n};";
                }
            }

            // 6) Remove GO batch separators
            sql = Regex.Replace(sql, @"^\s*GO\s*$", "", RegexOptions.IgnoreCase | RegexOptions.Multiline);

            // 6.5) Replace template tokens with hardcoded values
            sql = Regex.Replace(sql, @"'\{%R3_COUNTRY_CODE%\}'", "'ME'", RegexOptions.IgnoreCase);

            ApplyNameMappings:
            // Protect CTE names in WITH clause definitions from transformation
            // Replace CTE names in WITH clause with placeholders, transform, then restore
            var ctePlaceholders = new Dictionary<string, string>();
            foreach (var cteName in cteNames)
            {
                if (cteNamesOriginalCase.ContainsKey(cteName))
                {
                    string originalCteName = cteNamesOriginalCase[cteName];
                    string placeholder = $"__CTE_{ctePlaceholders.Count}__";
                    ctePlaceholders[placeholder] = originalCteName;
                    // Replace CTE name in WITH clause: WITH "cteName" AS or WITH cteName AS
                    sql = Regex.Replace(sql, $@"(WITH|,)\s+""?{Regex.Escape(originalCteName)}""?\s+AS\s*\(", 
                        $"$1 {placeholder} AS (", RegexOptions.IgnoreCase);
                }
            }
            
            // 7) Map schema names and table names (schema-aware)
            // Process schema.table patterns first, mapping both schema and table names
            // Pattern: schema."table" or schema.table or "schema"."table"
            // IMPORTANT: Schema-qualified tables are ALWAYS real database tables, never CTEs - transform them!
            // Support multiple source schemas mapping to same target schema (e.g., dataset_93286 and dataset_93287 -> rod14_wise6)
            var schemaTablePattern = new Regex(@"(\w+)\.(?:""([^""]+)""|(\w+))", RegexOptions.IgnoreCase);
            var schemaTableMatches = schemaTablePattern.Matches(sql);
            var processedPositions = new HashSet<int>();
            
            // Process schema-qualified tables (work backwards to preserve indices)
            for (int i = schemaTableMatches.Count - 1; i >= 0; i--)
            {
                var match = schemaTableMatches[i];
                if (processedPositions.Contains(match.Index)) continue;
                
                string schemaName = match.Groups[1].Value;
                string tableName = match.Groups[2].Success ? match.Groups[2].Value : match.Groups[3].Value;
                
                // Check schema+table mapping first (overrides schema-only mapping)
                string targetSchema = schemaName;
                string sourceSchema = null;
                
                // First check SchemaTableMap for (schema, table) combination
                var schemaTableKey = (schemaName, tableName);
                if (SchemaTableMap.ContainsKey(schemaTableKey))
                {
                    sourceSchema = schemaName;
                    targetSchema = SchemaTableMap[schemaTableKey];
                }
                else
                {
                    // Check if schemaName is a target schema (value in SchemaMap)
                    foreach (var kvp in SchemaMap)
                    {
                        if (kvp.Value.Equals(schemaName, StringComparison.OrdinalIgnoreCase))
                        {
                            sourceSchema = kvp.Key;
                            targetSchema = schemaName; // Already target schema
                            break;
                        }
                    }
                    
                    // If not found as target, check if it's a source schema
                    if (sourceSchema == null && SchemaMap.ContainsKey(schemaName))
                    {
                        sourceSchema = schemaName;
                        targetSchema = SchemaMap[schemaName];
                    }
                }
                
                // Find the table in ALL schemas (could be in any source schema)
                string lowerTableName = tableName.ToLowerInvariant();
                string camelCaseTable = null;
                
                // Search all schemas for the table
                foreach (var schemaKvp in schemaTableColumns)
                {
                    var schemaTables = schemaKvp.Value;
                    foreach (var tableEntry in schemaTables)
                    {
                        if (tableEntry.Key.Equals(lowerTableName, StringComparison.OrdinalIgnoreCase))
                        {
                            camelCaseTable = tableEntry.Key; // Already CamelCase from DDL
                            break;
                        }
                    }
                    if (camelCaseTable != null) break;
                }
                
                if (camelCaseTable != null)
                {
                    // Replace: schema.table -> targetSchema."CamelCaseTable"
                    // Schema-qualified tables are ALWAYS real tables, transform them!
                    string replacement = $"{targetSchema}.\"{camelCaseTable}\"";
                    sql = sql.Substring(0, match.Index) + replacement + sql.Substring(match.Index + match.Length);
                    processedPositions.Add(match.Index);
                }
                else if (sourceSchema != null)
                {
                    // Schema maps to target but table not found - map schema and keep table as-is
                    string replacement = $"{targetSchema}.{match.Groups[0].Value.Substring(schemaName.Length + 1)}";
                    sql = sql.Substring(0, match.Index) + replacement + sql.Substring(match.Index + match.Length);
                    processedPositions.Add(match.Index);
                }
            }
            
            // Map remaining standalone schema names that weren't part of schema.table patterns
            foreach (var schemaMapping in SchemaMap)
            {
                string sourceSchema = schemaMapping.Key;
                string targetSchema = schemaMapping.Value;
                // Replace standalone schema names (not already part of processed schema.table patterns)
                sql = Regex.Replace(sql, $@"\b{Regex.Escape(sourceSchema)}\b(?!\.)", targetSchema, RegexOptions.IgnoreCase);
            }
            
            // Process standalone tables (not schema-qualified) - check all schemas
            // IMPORTANT: Only skip transformation if it's actually a CTE reference (no schema prefix)
            // Schema-qualified tables are already handled above and are ALWAYS transformed
            foreach (var schemaKvp in schemaTableColumns)
            {
                string sourceSchema = schemaKvp.Key;
                // For standalone tables, use schema mapping (schema+table mapping doesn't apply without schema prefix)
                string targetSchema = SchemaMap.ContainsKey(sourceSchema) ? SchemaMap[sourceSchema] : sourceSchema;
                
                foreach (var tableNameEntry in schemaKvp.Value)
                {
                    string originalTableName = tableNameEntry.Key; // CamelCase from DDL
                    string lowerTableName = originalTableName.ToLowerInvariant();
                    
                    // Check if this table name matches a CTE name (case-insensitive)
                    bool isCte = cteNames.Contains(lowerTableName) || cteNames.Contains(originalTableName);
                    
                    if (!isCte)
                    {
                        // Real table reference (not a CTE) - add schema prefix for FROM/JOIN clauses
                        // Pattern: FROM "table" or JOIN "table" (lowercase) - transform and add schema
                        sql = Regex.Replace(sql, $@"(FROM|JOIN)\s+""{Regex.Escape(lowerTableName)}""(?=\s|$|AS)", 
                            $"$1 {targetSchema}.\"{originalTableName}\"", RegexOptions.IgnoreCase);
                        // Pattern: FROM table or JOIN table (unquoted, lowercase) - transform and add schema
                        sql = Regex.Replace(sql, $@"(FROM|JOIN)\s+(?<![""\.\w]){Regex.Escape(lowerTableName)}\b(?=\s|$|AS)", 
                            $"$1 {targetSchema}.\"{originalTableName}\"", RegexOptions.IgnoreCase);
                        
                        // Pattern: FROM "Table" or JOIN "Table" (already CamelCase, no schema) - add schema
                        sql = Regex.Replace(sql, $@"(FROM|JOIN)\s+""{Regex.Escape(originalTableName)}""(?=\s|$|AS)(?!\.)", 
                            $"$1 {targetSchema}.\"{originalTableName}\"", RegexOptions.IgnoreCase);
                    }
                    // If it's a CTE, skip transformation - CTE names should never be changed
                }
            }

            // 9) Map column names (lowercase gathered) to CamelCase per DDL (schema-aware)
            // Special case: always translate record_id to _id (but not if it's a CTE name)
            // Check if "record_id" or "recordid" is a CTE name before transforming
            if (!cteNames.Contains("record_id") && !cteNames.Contains("recordid"))
            {
                sql = Regex.Replace(sql, $@"""record_id""", $@"""_id""", RegexOptions.IgnoreCase);
                sql = Regex.Replace(sql, $@"\brecord_id\b", @"""_id""", RegexOptions.IgnoreCase);
                sql = Regex.Replace(sql, $@"""recordid""", $@"""_id""", RegexOptions.IgnoreCase);
                sql = Regex.Replace(sql, $@"\brecordid\b", @"""_id""", RegexOptions.IgnoreCase);
            }

            // Use schema-specific column name maps - process all schemas
            // This handles columns from all schemas, including when multiple source schemas map to same target
            // Transform all column names normally - CTE column references will be handled separately if needed
            foreach (var schemaKvp in schemaColumnNameMaps)
            {
                foreach (var kv in schemaKvp.Value)
                {
                    // Step 1: Replace quoted lowercase column names: "season" -> "Season"
                    sql = Regex.Replace(sql, $"\"{Regex.Escape(kv.Key)}\"", $"\"{kv.Value}\"", RegexOptions.IgnoreCase);
                    
                    // Step 2: Replace unquoted lowercase column names - wrap in quotes for PostgreSQL
                    sql = Regex.Replace(sql, $@"(?<!"")\b{Regex.Escape(kv.Key)}\b(?!\"")", $"\"{kv.Value}\"", RegexOptions.IgnoreCase);
                }
            }

            // 10) Fix numeric column comparisons: remove quotes from numeric literals and remove empty string checks
            sql = FixNumericColumnComparisons(sql);

            // 11) Add ::text casts for string comparisons (column = '' or column = 'text')
            sql = AddTextCastsForStringComparisons(sql);

            // 12) Remove invalid CAST operations: CAST(boolean_column AS NUMERIC) - boolean cannot be cast to numeric
            sql = RemoveInvalidBooleanCasts(sql);
            
            // Restore CTE names from placeholders (restore original case, WITHOUT quotes)
            // CTE names should never be quoted in PostgreSQL
            foreach (var kvp in ctePlaceholders)
            {
                // Remove quotes from CTE name if present
                string cteName = kvp.Value;
                if (cteName.StartsWith("\"") && cteName.EndsWith("\""))
                {
                    cteName = cteName.Substring(1, cteName.Length - 2);
                }
                sql = sql.Replace(kvp.Key, cteName);
            }

            // Remove quotes from unqualified CTE references in FROM/JOIN clauses
            // Rule: If a table reference has no schema prefix, it's a CTE and should be unquoted
            sql = RemoveQuotesFromUnqualifiedCteReferences(sql, cteNames);

            // Final trim
            sql = sql.Trim();

            return sql;
        }
    }
}
