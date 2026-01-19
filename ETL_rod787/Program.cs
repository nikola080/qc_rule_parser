using System.Data;
using System.Data.SqlClient;
using ETL_rod787.Services;
using System.IO;

// Read DDL from file or provide as string
string ddl = File.ReadAllText("ddl.sql"); // Or provide DDL string directly

// Define schema name mapping
/*var schemaMap = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase)
{
    { "dataset_93286", "rod14_wise6" }
};*/
var schemaMap = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase)
{
    { "dataset_96269", "rod787" }
};

Console.WriteLine("DDL: ", ddl);
var transformer = new QCTransformatorMSSQL("C:/Users/Korisnik/Desktop/nikola/rod787/QC_Rules.xlsx", ddl, schemaMap);
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