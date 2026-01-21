using System.Data;
using System.Data.SqlClient;
using ETL_rod787.Services;
using System.IO;

// Read DDL from file or provide as string
string ddl = File.ReadAllText("ddl.sql"); // Or provide DDL string directly

// Define schema name mapping
var schemaMap = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase)
{
    { "dataset_93286", "rod14_wise6" },
    { "dataset_93287", "rod14_wise6" }
};
/*var schemaMap = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase)
{
    { "dataset_96269", "rod787" }
};*/

Console.WriteLine("DDL: ", ddl);
// Set row boundaries for Excel processing
int expressionsStartRow = 3;  // Set your starting row number here
int expressionsEndRow = 261;    // Set your ending row number here
var transformer = new QCTransformatorMSSQL("C:/Users/Korisnik/Desktop/nikola/rod14/QC_rules_colored_by_type.xlsx", ddl, expressionsStartRow, expressionsEndRow, schemaMap);
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