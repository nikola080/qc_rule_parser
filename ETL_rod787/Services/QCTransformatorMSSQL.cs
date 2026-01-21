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

        public QCTransformatorMSSQL(string path, string ddl, int expressionsStartRow, int expressionsEndRow, Dictionary<string, string>? schemaMap = null) 
            : base(path, ddl, expressionsStartRow, expressionsEndRow, schemaMap)
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

                // Remove empty string checks for numeric columns (= '', <>, != '')
                // Pattern: OR "columnName" = '' (with optional whitespace)
                var emptyStringPatternOr = $@"\s+OR\s+""{escapedColName}""\s*=\s*''";
                sql = Regex.Replace(sql, emptyStringPatternOr, "", RegexOptions.IgnoreCase);
                
                // Pattern: "columnName" = '' OR (at start or middle)
                var emptyStringPatternStart = $@"""{escapedColName}""\s*=\s*''\s+OR\s+";
                sql = Regex.Replace(sql, emptyStringPatternStart, "", RegexOptions.IgnoreCase);
                
                // Pattern: AND "columnName" = '' (standalone condition)
                var emptyStringPatternAnd = $@"\s+AND\s+""{escapedColName}""\s*=\s*''";
                sql = Regex.Replace(sql, emptyStringPatternAnd, "", RegexOptions.IgnoreCase);
                
                // Pattern: "columnName" = '' AND (at end)
                var emptyStringPatternEnd = $@"""{escapedColName}""\s*=\s*''\s+AND\s+";
                sql = Regex.Replace(sql, emptyStringPatternEnd, "", RegexOptions.IgnoreCase);
                
                // Pattern: "columnName" <> '' or "columnName" != '' (standalone)
                var notEmptyStringPattern = $@"""{escapedColName}""\s*(<>|!=)\s*''";
                sql = Regex.Replace(sql, notEmptyStringPattern, "", RegexOptions.IgnoreCase);
                
                // Pattern: OR "columnName" <> '' or OR "columnName" != ''
                var notEmptyStringPatternOr = $@"\s+OR\s+""{escapedColName}""\s*(<>|!=)\s*''";
                sql = Regex.Replace(sql, notEmptyStringPatternOr, "", RegexOptions.IgnoreCase);
                
                // Pattern: AND "columnName" <> '' or AND "columnName" != ''
                var notEmptyStringPatternAnd = $@"\s+AND\s+""{escapedColName}""\s*(<>|!=)\s*''";
                sql = Regex.Replace(sql, notEmptyStringPatternAnd, "", RegexOptions.IgnoreCase);
                
                // Pattern: "columnName" <> '' AND or "columnName" != '' AND
                var notEmptyStringPatternEnd = $@"""{escapedColName}""\s*(<>|!=)\s*''\s+AND\s+";
                sql = Regex.Replace(sql, notEmptyStringPatternEnd, "", RegexOptions.IgnoreCase);
                
                // Pattern: "columnName" <> '' OR or "columnName" != '' OR
                var notEmptyStringPatternEndOr = $@"""{escapedColName}""\s*(<>|!=)\s*''\s+OR\s+";
                sql = Regex.Replace(sql, notEmptyStringPatternEndOr, "", RegexOptions.IgnoreCase);
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

                // Remove empty string checks for boolean columns (= '', <>, != '')
                // Pattern: OR "columnName" = '' (with optional whitespace)
                var emptyStringPatternOr = $@"\s+OR\s+""{escapedColName}""\s*=\s*''";
                sql = Regex.Replace(sql, emptyStringPatternOr, "", RegexOptions.IgnoreCase);
                
                // Pattern: "columnName" = '' OR (at start or middle)
                var emptyStringPatternStart = $@"""{escapedColName}""\s*=\s*''\s+OR\s+";
                sql = Regex.Replace(sql, emptyStringPatternStart, "", RegexOptions.IgnoreCase);
                
                // Pattern: AND "columnName" = '' (standalone condition)
                var emptyStringPatternAnd = $@"\s+AND\s+""{escapedColName}""\s*=\s*''";
                sql = Regex.Replace(sql, emptyStringPatternAnd, "", RegexOptions.IgnoreCase);
                
                // Pattern: "columnName" = '' AND (at end)
                var emptyStringPatternEnd = $@"""{escapedColName}""\s*=\s*''\s+AND\s+";
                sql = Regex.Replace(sql, emptyStringPatternEnd, "", RegexOptions.IgnoreCase);
                
                // Pattern: "columnName" <> '' or "columnName" != '' (standalone)
                var notEmptyStringPattern = $@"""{escapedColName}""\s*(<>|!=)\s*''";
                sql = Regex.Replace(sql, notEmptyStringPattern, "", RegexOptions.IgnoreCase);
                
                // Pattern: OR "columnName" <> '' or OR "columnName" != ''
                var notEmptyStringPatternOr = $@"\s+OR\s+""{escapedColName}""\s*(<>|!=)\s*''";
                sql = Regex.Replace(sql, notEmptyStringPatternOr, "", RegexOptions.IgnoreCase);
                
                // Pattern: AND "columnName" <> '' or AND "columnName" != ''
                var notEmptyStringPatternAnd = $@"\s+AND\s+""{escapedColName}""\s*(<>|!=)\s*''";
                sql = Regex.Replace(sql, notEmptyStringPatternAnd, "", RegexOptions.IgnoreCase);
                
                // Pattern: "columnName" <> '' AND or "columnName" != '' AND
                var notEmptyStringPatternEnd = $@"""{escapedColName}""\s*(<>|!=)\s*''\s+AND\s+";
                sql = Regex.Replace(sql, notEmptyStringPatternEnd, "", RegexOptions.IgnoreCase);
                
                // Pattern: "columnName" <> '' OR or "columnName" != '' OR
                var notEmptyStringPatternEndOr = $@"""{escapedColName}""\s*(<>|!=)\s*''\s+OR\s+";
                sql = Regex.Replace(sql, notEmptyStringPatternEndOr, "", RegexOptions.IgnoreCase);
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
        /// Removes ISNUMERIC checks for numeric and boolean columns.
        /// This must be called AFTER column name transformation so column names match ColumnTypes dictionary.
        /// </summary>
        private string RemoveIsNumericForNumericColumns(string sql)
        {
            if (string.IsNullOrEmpty(sql)) return sql;

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
                    
                    // Check if the expression is a simple column reference (not a function call or complex expression)
                    // Handle quoted identifiers like "columnName" or [columnName]
                    string columnName = null;
                    string trimmedExpr = expr.Trim();
                    
                    // Check if it's a quoted identifier
                    if (trimmedExpr.StartsWith("\"", StringComparison.Ordinal) && trimmedExpr.EndsWith("\"", StringComparison.Ordinal))
                    {
                        columnName = trimmedExpr.Substring(1, trimmedExpr.Length - 2).Trim();
                    }
                    else if (trimmedExpr.StartsWith("[", StringComparison.Ordinal) && trimmedExpr.EndsWith("]", StringComparison.Ordinal))
                    {
                        columnName = trimmedExpr.Substring(1, trimmedExpr.Length - 2).Trim();
                    }
                    else if (!trimmedExpr.Contains("(") && !trimmedExpr.Contains(" ") && !trimmedExpr.Contains("."))
                    {
                        // Simple unquoted identifier (no function calls, no spaces, no table.column)
                        columnName = trimmedExpr;
                    }
                    
                    // Check if this column is numeric or boolean type - ISNUMERIC doesn't make sense for these
                    // ColumnTypes uses StringComparer.OrdinalIgnoreCase, so ContainsKey works case-insensitively
                    bool shouldRemoveCheck = false;
                    if (!string.IsNullOrEmpty(columnName) && ColumnTypes.ContainsKey(columnName))
                    {
                        string columnType = ColumnTypes[columnName];
                        // Remove ISNUMERIC check for numeric and boolean columns
                        // Also check for other numeric types like int, integer, decimal, float, double, etc.
                        shouldRemoveCheck = columnType == "int4" || 
                                          columnType == "int" ||
                                          columnType == "integer" ||
                                          columnType.StartsWith("numeric", StringComparison.OrdinalIgnoreCase) ||
                                          columnType.StartsWith("decimal", StringComparison.OrdinalIgnoreCase) ||
                                          columnType.StartsWith("float", StringComparison.OrdinalIgnoreCase) ||
                                          columnType.StartsWith("double", StringComparison.OrdinalIgnoreCase) ||
                                          columnType.StartsWith("real", StringComparison.OrdinalIgnoreCase) ||
                                          columnType.StartsWith("smallint", StringComparison.OrdinalIgnoreCase) ||
                                          columnType.StartsWith("bigint", StringComparison.OrdinalIgnoreCase) ||
                                          columnType == "bool" ||
                                          columnType == "boolean";
                    }
                    
                    if (shouldRemoveCheck)
                    {
                        // Remove the entire ISNUMERIC check - it's redundant for numeric and boolean columns
                        // Need to handle AND/OR operators around it properly
                        int beforeStart = match.Index;
                        int afterEnd = endPos;
                        
                        string before = sql.Substring(0, beforeStart);
                        string after = sql.Substring(afterEnd);
                        
                        // Check if there's AND/OR before the ISNUMERIC
                        var beforeMatch = Regex.Match(before, @"\s+(AND|OR)\s*$", RegexOptions.IgnoreCase);
                        bool hasBeforeOp = beforeMatch.Success;
                        
                        // Check if there's AND/OR after the ISNUMERIC
                        var afterMatch = Regex.Match(after, @"^\s*(AND|OR)\s+", RegexOptions.IgnoreCase);
                        bool hasAfterOp = afterMatch.Success;
                        
                        // Remove the ISNUMERIC check
                        if (hasBeforeOp)
                        {
                            // Remove AND/OR before
                            before = before.Substring(0, before.Length - beforeMatch.Length).TrimEnd();
                        }
                        
                        if (hasAfterOp)
                        {
                            // Remove AND/OR after
                            after = after.Substring(afterMatch.Length);
                        }
                        
                        // If we had operators on both sides, we need to keep one to connect the conditions
                        // If only one side had an operator, we removed it (which is correct)
                        // If both sides had operators, we removed both, so we need to add one back
                        if (hasBeforeOp && hasAfterOp)
                        {
                            // Both sides had operators - keep the one that makes sense (usually AND)
                            // Add AND between the conditions
                            sql = before + " AND " + after;
                        }
                        else
                        {
                            // Only one side had operator (or neither) - just concatenate
                            sql = before + after;
                        }
                    }
                }
            }

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
                    // Casting to the same type is redundant
                    else if ((columnType == "int4" && targetType == "int4") ||
                             (columnType.StartsWith("numeric", StringComparison.OrdinalIgnoreCase) && targetType.StartsWith("numeric")) ||
                             (columnType == "bool" && targetType == "bool") ||
                             (columnType == "text" && (targetType == "text" || targetType == "varchar")))
                    {
                        shouldRemove = true;
                    }
                    
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
        /// Basic transformation of an MSSQL query into a Postgres-compatible query.
        /// This performs shallow, heuristic rewrites only (identifiers, some functions, TOP -> LIMIT, dbo schema -> dataset_93286).
        /// Then maps schema/table/column names to rod14_wise6 DDL naming.
        /// </summary>
        public override string TransformToPostgres(string mssqlSql)
        {
            // Check if query is already PostgreSQL - if so, only do schema/table/column name mappings
            bool isPostgres = IsPostgresQuery(mssqlSql);
            
            string sql = mssqlSql;
            
            // If already PostgreSQL, only do minimal transformations
            if (isPostgres)
            {
                // Convert REGEXP_MATCHES to REGEXP_LIKE (REGEXP_LIKE stays the same)
                sql = Regex.Replace(sql, @"\bREGEXP_MATCHES\b", "REGEXP_LIKE", RegexOptions.IgnoreCase);
                
                // Convert isdate(expr) to PostgreSQL date validation
                // isdate returns boolean, so we'll use a regex pattern to check if it's a valid date
                var isdatePattern = new Regex(@"\bisdate\s*\(", RegexOptions.IgnoreCase);
                var isdateMatches = isdatePattern.Matches(sql);
                for (int i = isdateMatches.Count - 1; i >= 0; i--)
                {
                    var match = isdateMatches[i];
                    int startPos = match.Index + match.Length;
                    int parenDepth = 1;
                    int endPos = startPos;
                    
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
                        }
                        endPos++;
                    }
                    
                    if (parenDepth == 0)
                    {
                        // Extract the expression
                        string expr = sql.Substring(startPos, endPos - startPos - 1).Trim();
                        
                        // Replace isdate(expr) with PostgreSQL date validation
                        // Use a regex pattern to check if the string matches common date formats
                        string replacement = $"(CASE WHEN {expr} ~ '^[0-9]{{4}}-[0-9]{{2}}-[0-9]{{2}}$' AND ({expr}::date IS NOT NULL) THEN true ELSE false END)";
                        sql = sql.Substring(0, match.Index) + replacement + sql.Substring(endPos);
                    }
                }
                
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

            // 2.5) Convert ISNUMERIC(expr) -> (expr ~ 'numeric-regex') to emulate SQL Server ISNUMERIC in Postgres
            // Note: Removal of ISNUMERIC for numeric columns happens later after column name transformation
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
                    
                    // Replace ISNUMERIC(expr) with (expr ~ 'numeric-regex')
                    // Note: We'll remove ISNUMERIC for numeric columns later after column name transformation
                    string replacement = $"({expr} ~ '^[+-]?([0-9]+(\\.[0-9]*)?|\\.[0-9]+)$')";
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
            // 7) Map schema names using SchemaMap dictionary
            foreach (var schemaMapping in SchemaMap)
            {
                string sourceSchema = schemaMapping.Key;
                string targetSchema = schemaMapping.Value;
                // Replace schema name in patterns like: schema.table, schema."table", "schema"."table"
                sql = Regex.Replace(sql, $@"\b{Regex.Escape(sourceSchema)}\b", targetSchema, RegexOptions.IgnoreCase);
            }

            // 8) Map table names to CamelCase as in DDL (both quoted and unquoted)
            // Use table_columns dictionary populated by DDL parsing
            foreach (var tableNameEntry in table_columns)
            {
                string originalTableName = tableNameEntry.Key; // This is the CamelCase name from DDL
                string lowerTableName = originalTableName.ToLowerInvariant(); // For matching lowercase in query

                // Replace quoted lowercase table names: "monitoringresult" -> "MonitoringResult"
                sql = Regex.Replace(sql, $@"""{Regex.Escape(lowerTableName)}""", $"\"{originalTableName}\"", RegexOptions.IgnoreCase);
                // Replace unquoted lowercase table names - wrap in quotes for PostgreSQL
                // Only match if definitely not inside quotes (not preceded or followed by quote)
                sql = Regex.Replace(sql, $@"(?<![""])\b{Regex.Escape(lowerTableName)}\b(?![""])", $"\"{originalTableName}\"", RegexOptions.IgnoreCase);
            }

            // 9) Map column names (lowercase gathered) to CamelCase per DDL
            // Special case: always translate record_id to _id
            sql = Regex.Replace(sql, $@"""record_id""", $@"""_id""", RegexOptions.IgnoreCase);
            sql = Regex.Replace(sql, $@"\brecord_id\b", @"""_id""", RegexOptions.IgnoreCase);
            sql = Regex.Replace(sql, $@"""recordid""", $@"""_id""", RegexOptions.IgnoreCase);
            sql = Regex.Replace(sql, $@"\brecordid\b", @"""_id""", RegexOptions.IgnoreCase);

            // Use ColumnNameMap dictionary populated by DDL parsing (lowercase -> CamelCase)
            foreach (var kv in ColumnNameMap)
            {
                // Step 1: Replace quoted lowercase column names: "season" -> "Season"
                sql = Regex.Replace(sql, $"\"{Regex.Escape(kv.Key)}\"", $"\"{kv.Value}\"", RegexOptions.IgnoreCase);
                
                // Step 2: Replace unquoted lowercase column names - wrap in quotes for PostgreSQL
                // Match only if NOT inside quotes - check that there's no quote immediately before or after
                // Use a more precise pattern that won't match if the column is between quotes
                sql = Regex.Replace(sql, $@"(?<!"")\b{Regex.Escape(kv.Key)}\b(?!\"")", $"\"{kv.Value}\"", RegexOptions.IgnoreCase);
            }

            // 10) Remove ISNUMERIC checks for numeric/boolean columns (for both SQL Server and PostgreSQL queries)
            // This must happen AFTER column name transformation so column names match ColumnTypes dictionary
            sql = RemoveIsNumericForNumericColumns(sql);

            // 11) Fix numeric column comparisons: remove quotes from numeric literals and remove empty string checks
            sql = FixNumericColumnComparisons(sql);

            // 12) Remove invalid CAST operations: CAST(boolean_column AS NUMERIC) - boolean cannot be cast to numeric
            sql = RemoveInvalidBooleanCasts(sql);

            // Final trim
            sql = sql.Trim();

            return sql;
        }
    }
}
