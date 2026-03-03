using System.Data;
using System.Data.SqlClient;
using ETL_rod787.Services;
using System.IO;

// Read DDL from file or provide as string
string ddl = File.ReadAllText("ddl.sql"); // Or provide DDL string directly

// Define schema name mapping (default mapping for all tables in a schema)
var schemaMap = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase)
{
    { "dataset_93520", "rod184_wise3_new" },
    { "dataset_93519", "rod806_wise5_new" } // Default: all tables in dataset_93519 go to rod806_wise5_new
};
/*var schemaMap = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase)
{
    { "dataset_96269", "rod787" }
};*/

// Define schema+table mapping (overrides schema mapping for specific tables)
var schemaTableMap = new Dictionary<(string Schema, string Table), string>(new SchemaTableComparer())
{
    { ("dataset_93519", "dataflowmetadata"), "rod14_wise6_new" },
    { ("dataset_93519", "observedproperty_qcreference"), "rod14_wise6_new" }
};

// Helper class for schema+table comparer
class SchemaTableComparer : IEqualityComparer<(string Schema, string Table)>
{
    public bool Equals((string Schema, string Table) x, (string Schema, string Table) y)
    {
        return string.Equals(x.Schema, y.Schema, StringComparison.OrdinalIgnoreCase) &&
               string.Equals(x.Table, y.Table, StringComparison.OrdinalIgnoreCase);
    }
    
    public int GetHashCode((string Schema, string Table) obj)
    {
        return StringComparer.OrdinalIgnoreCase.GetHashCode(obj.Schema) ^
               StringComparer.OrdinalIgnoreCase.GetHashCode(obj.Table);
    }
}

Console.WriteLine("DDL: ", ddl);
Console.WriteLine("\n=== Schema Mapping Configuration ===");
Console.WriteLine("Schema mappings (default for all tables in schema):");
foreach (var kvp in schemaMap)
{
    Console.WriteLine($"  {kvp.Key} -> {kvp.Value}");
}
Console.WriteLine("\nSchema+Table mappings (overrides schema mapping):");
foreach (var kvp in schemaTableMap)
{
    Console.WriteLine($"  {kvp.Key.Schema}.{kvp.Key.Table} -> {kvp.Value}");
}

// Prompt for Excel column indices (0-based: A=0, B=1, C=2, D=3, E=4, F=5, G=6, H=7, I=8, etc.)
Console.WriteLine("\n=== Excel Column Configuration ===");
Console.WriteLine("Enter column indices (0-based). Press Enter to use defaults shown in brackets.");

Console.Write($"Table Name column [default: A (0)]: ");
string? input = Console.ReadLine();
int colTableName = string.IsNullOrWhiteSpace(input) ? 0 : int.Parse(input);

Console.Write($"Column Name column [default: B (1)]: ");
input = Console.ReadLine();
int colColumnName = string.IsNullOrWhiteSpace(input) ? 1 : int.Parse(input);

Console.Write($"Code column [default: C (2)]: ");
input = Console.ReadLine();
int colCode = string.IsNullOrWhiteSpace(input) ? 2 : int.Parse(input);

Console.Write($"Description column [default: E (4)]: ");
input = Console.ReadLine();
int colDescription = string.IsNullOrWhiteSpace(input) ? 4 : int.Parse(input);

Console.Write($"SQL Expression column [default: G (6)]: ");
input = Console.ReadLine();
int colExpression = string.IsNullOrWhiteSpace(input) ? 6 : int.Parse(input);

Console.Write($"Severity column [default: H (7)]: ");
input = Console.ReadLine();
int colSeverity = string.IsNullOrWhiteSpace(input) ? 7 : int.Parse(input);

Console.Write($"Additional Context column [default: I (8)]: ");
input = Console.ReadLine();
int colAdditionalContext = string.IsNullOrWhiteSpace(input) ? 8 : int.Parse(input);

// Set row boundaries for Excel processing
Console.Write($"\nStarting row number [default: 1]: ");
input = Console.ReadLine();
int expressionsStartRow = string.IsNullOrWhiteSpace(input) ? 1 : int.Parse(input);

Console.Write($"Ending row number [default: 152]: ");
input = Console.ReadLine();
int expressionsEndRow = string.IsNullOrWhiteSpace(input) ? 152 : int.Parse(input);

Console.WriteLine($"\nUsing columns: Table={colTableName}, Column={colColumnName}, Code={colCode}, Description={colDescription}, Expression={colExpression}, Severity={colSeverity}, AdditionalContext={colAdditionalContext}");
Console.WriteLine($"Processing rows {expressionsStartRow} to {expressionsEndRow}\n");

var transformer = new QCTransformatorMSSQL("C:/Users/Korisnik/Desktop/nikola/rod184/qc_rules.xlsx", ddl, expressionsStartRow, expressionsEndRow, schemaMap, schemaTableMap,
    columnIndexTableName: colTableName, columnIndexColumnName: colColumnName, columnIndexCode: colCode,
    columnIndexDescription: colDescription, columnIndexExpression: colExpression, columnIndexSeverity: colSeverity, columnIndexAdditionalContext: colAdditionalContext);
var transformed = transformer.ProcessExpressionsFromExcel();

foreach (var item in transformed)
{
    Console.WriteLine($"row {item.Row}:");
    Console.WriteLine();
    Console.WriteLine("====================================================");
    Console.WriteLine(item.Transformed);
    Console.WriteLine("====================================================");
    Console.WriteLine();
}

// Generate INSERT statements for qc_rules_internal table
transformer.GenerateInsertStatements("output.txt");