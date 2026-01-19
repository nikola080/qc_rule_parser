using System.Collections.Generic;
using System.IO;
using NPOI.SS.UserModel;
using NPOI.XSSF.UserModel;
using NPOI.HSSF.UserModel;
namespace ETL_rod787.Services;

public class ExcelReader
{

    private Dictionary<string, int> bacteriaQuality = new Dictionary<string, int>();
    IWorkbook? workbook;
    public ExcelReader(string _path)
    {
        using var stream = File.OpenRead(_path);
        if (Path.GetExtension(_path).Equals(".xls", System.StringComparison.OrdinalIgnoreCase))
        {
            workbook = new HSSFWorkbook(stream);
        }
        else
        {
            workbook = new XSSFWorkbook(stream);
        }
        
        bacteriaQuality.Add("odličan kvalitet", 5);
        bacteriaQuality.Add("dobar kvalitet", 4);
        bacteriaQuality.Add("zadovoljavajući", 3);
    }

    public void consoleOut()
    {
        var sheet = workbook.GetSheetAt(0);
        for (int i = 0; i <= sheet.LastRowNum; i++)
        {
            var row = sheet.GetRow(i);
            if (row != null)
            {
                for (int j = 0; j < row.LastCellNum; j++)
                {
                    var cell = row.GetCell(j);
                    if (cell != null)
                    {
                        // Process cell value
                        var cellValue = cell.ToString();
                    }
                }
            }
        }
    }

    public List<Tuple<string, string>> generateSubmissionIds()
    {
        var queryCharaterisationSubId = "SELECT public.test_create_env_dataset_sheet('82c0b876-a267-4d49-8bc1-575a1d5be1e2','Montenegro_787', 'Characterisation');";
        var querySeasonalPeriodSubId = "SELECT public.test_create_env_dataset_sheet('82c0b876-a267-4d49-8bc1-575a1d5be1e2','Montenegro_787', 'SeasonalPeriod');";
        var queryMonitoringResultSubId = "SELECT public.test_create_env_dataset_sheet('82c0b876-a267-4d49-8bc1-575a1d5be1e2','Montenegro_787', 'MonitoringResult');";

        List<Tuple<string, string>> sub_ids = new List<Tuple<string, string>>();

        try
        {
            PostgresConn.Services.PostgresConn.openConnection();
            var connection = PostgresConn.Services.PostgresConn.getConnection();

            using (var command = connection.CreateCommand())
            {
                command.CommandText = queryCharaterisationSubId;
                var result = command.ExecuteScalar();
                sub_ids.Add(new Tuple<string, string>("Characterisation", result.ToString() ?? ""));

                command.CommandText = querySeasonalPeriodSubId;
                result = command.ExecuteScalar();
                sub_ids.Add(new Tuple<string, string>("SeasonalPeriod", result.ToString() ?? ""));

                command.CommandText = queryMonitoringResultSubId;
                result = command.ExecuteScalar();
                sub_ids.Add(new Tuple<string, string>("MonitoringResult", result.ToString() ?? ""));
            }
        }
        catch (System.Exception ex)
        {
            Console.WriteLine("Error executing queries: " + ex.Message);
        }
        finally
        {
            PostgresConn.Services.PostgresConn.closeConnection();
        }

        return sub_ids;
    }

    public List<Tuple<string, string>> generateDummySubmissionIds()
    {
        List<Tuple<string, string>> sub_ids = new List<Tuple<string, string>>();
        sub_ids.Add(new Tuple<string, string>("Characterisation", "dummy-char-id"));
        sub_ids.Add(new Tuple<string, string>("SeasonalPeriod", "dummy-season-id"));
        sub_ids.Add(new Tuple<string, string>("MonitoringResult", "dummy-monitor-id"));
        return sub_ids;
    }
    public int ReadExcel()
    {
        /*
INSERT INTO rod787."MonitoringResult"
"_submission_id", season, "bathingWaterIdentifier", "sampleDate", "intestinalEnterococciValue", "escherichiaColiValue", "sampleStatus", "intestinalEnterococciStatus", "escherichiaColiStatus", remarks)
VALUES(?, '', '', '', 0, 0, '', '', '', '');



         */
        var insertCharQuery = "INSERT INTO rod787.\"Characterisation\" (\"_submission_id\", season, \"bathingWaterIdentifier\", \"qualityClass\", \"geographicalConstraint\") VALUES \n";
        var insertSeasQuery = "INSERT INTO rod787.\"SeasonalPeriod\" (\"_submission_id\", season, \"bathingWaterIdentifier\", \"periodType\", \"startDate\", \"endDate\") VALUES \n";
        var insertMonQuery = "INSERT INTO rod787.\"MonitoringResult\" (\"_submission_id\", season, \"bathingWaterIdentifier\", \"escherichiaColiValue\", \"intestinalEnterococciValue\", \"sampleDate\") VALUES \n";

        //var submission_id = generateSubmissionIds();
        List<Tuple<string, string>> submission_ids = generateDummySubmissionIds();

        var sheet = workbook.GetSheetAt(0);
        var rows = new List<Dictionary<string, object?>>();
        var headerRow = sheet.GetRow(sheet.FirstRowNum);
        if (headerRow == null) return 1;

        var headers = new List<string>();

        for (int r = sheet.FirstRowNum + 1; r <= sheet.LastRowNum; r++)
        {
            insertCharQuery += "('" + submission_ids[0].Item2 + "'::uuid, '2025', ";

            insertSeasQuery += "('" + submission_ids[1].Item2 + "'::uuid, '2025', ";

            insertMonQuery += "('" + submission_ids[2].Item2 + "'::uuid, '2025', ";

            var row = sheet.GetRow(r);
            if (row == null) continue;
            var dict = new Dictionary<string, object?>();

            int qualityValue = 0;
            for (int c = 1; c < headers.Count; c++)
            {
                var cell = row.GetCell(c);
                if (cell == null) continue;

                switch (c)
                {
                    case 2:
                        insertCharQuery += $"'{cell.StringCellValue}', ";
                        insertSeasQuery += $"'{cell.StringCellValue}', ";
                        insertMonQuery += $"'{cell.StringCellValue}', ";
                        break;
                    case 7:
                        insertMonQuery += $"{cell.StringCellValue}, ";
                        break;
                    case 8:
                        insertMonQuery += $"{cell.StringCellValue}, ";
                        break;
                    case 9:
                        qualityValue = bacteriaQuality.GetValueOrDefault(cell.StringCellValue.ToLower(), 0);
                        break;
                    case 10:
                        if (bacteriaQuality.GetValueOrDefault(cell.StringCellValue.ToLower(), 0) < qualityValue)
                        {
                            qualityValue = bacteriaQuality.GetValueOrDefault(cell.StringCellValue.ToLower(), 0);
                        }
                        insertCharQuery += $"{qualityValue}, ";
                        break;
                    case 11:
                        string[] date = cell.StringCellValue.Split('.');
                        insertMonQuery += $"DATE '20{date[2]}-{date[1]}-{date[0]}'),\n";
                        break;
                    case 12:
                        insertCharQuery += "false),\n";
                        insertSeasQuery += "'bathingSeason', DATE '2025-15-05', DATE '2025-15-10'),\n";
                        break;
                }
                
                
            }

            rows.Add(dict);
        }
        Console.WriteLine(insertCharQuery);
        Console.WriteLine(insertSeasQuery);
        Console.WriteLine(insertMonQuery);
        return 1;
    }
}