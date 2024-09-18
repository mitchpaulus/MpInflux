using System.Text;
using InfluxDB.Client;
using InfluxDB.Client.Core.Flux.Domain;
using File = System.IO.File;

namespace MpInflux;

public class Program
{
    public static async Task<int> Main(string[] args)
    {
        if (args.Any(s => s == "--help" || s == "-h"))
        {
            Console.Write("MpInflux < FILE\n");
            Console.Write("\n");
            Console.Write("File Format:\n");
            Console.Write("Bucket\n");
            Console.Write("Start Date (YYYY-MM-DD), inclusive\n");
            Console.Write("End Date (YYYY-MM-DD), exclusive\n");
            Console.Write("Interval (min)\n");
            Console.Write("Trend Name 1\n");
            Console.Write("Trend Name 2\n");
            Console.Write("...\n");
            Console.Write("\n");
            Console.Write("This utilizes a simple walk-back alignment algorithm.\n");

            return 0;
        }

        string inputText;

        if (args.Length > 0)
        {
            // Read input from file
            inputText = await File.ReadAllTextAsync(args[0]);
        }
        else
        {
            // Read input from standard input
            inputText = await Console.In.ReadToEndAsync();
        }

        (string output, bool success) = await GetOutput(inputText);

        Console.Write(output);

        return success ? 0 : 1;
    }

    public static async Task<(string, bool)> GetOutput(string inputText)
    {
        var lines = inputText.Split('\n');

        if (lines.Length < 5)
        {
            await Console.Error.WriteLineAsync("Input file must have at least 5 lines.");
            return ("", false);
        }

        var bucket = lines[0].Trim();
        var startDate = DateTime.Parse(lines[1].Trim());
        var endDate = DateTime.Parse(lines[2].Trim());
        var intervalMinutes = int.Parse(lines[3].Trim());
        var trends = lines[4..].Where(s => s.Trim().Length > 0).ToList();

        // Get Environment Variables for INFLUX_HOST and INFLUX_TOKEN
        var influxHost = Environment.GetEnvironmentVariable("INFLUX_HOST");
        if (influxHost is null)
        {
            await Console.Error.WriteLineAsync("INFLUX_HOST environment variable not found.");
            return ("", false);
        }

        var influxToken = Environment.GetEnvironmentVariable("INFLUX_TOKEN");
        if (influxToken is null)
        {
            await Console.Error.WriteLineAsync("INFLUX_TOKEN environment variable not found.");
            return ("", false);
        }

        var org = Environment.GetEnvironmentVariable("INFLUX_ORG_ID") ?? Environment.GetEnvironmentVariable("INFLUX_ORG");
        if (org is null)
        {
            await Console.Error.WriteLineAsync("INFLUX_ORG or INFLUX_ORG_ID environment variable not found.");
            return ("", false);
        }

        // Connect to InfluxDB
        using var client = new InfluxDBClient(influxHost, influxToken);
        var queryApi = client.GetQueryApi();

        List<string> outLines = new();
        var batchedTrends = trends.Batch(5);
        Dictionary<string, Dictionary<DateTime, object>> data = new();

        foreach (var trendBatch in batchedTrends)
        {
            string orStatement = string.Join(" or ", trendBatch.Select(s => $"r._measurement == \"{s}\""));

            // Generate query based on input and alignment
            var query = $"from(bucket: \"{bucket}\") |> range(start: {startDate:yyyy-MM-dd}, stop: {endDate:yyyy-MM-dd}) |> filter(fn: (r) => {orStatement})";
            List<FluxTable> fluxTables = await queryApi.QueryAsync(query, org);
            string currentMeasurement = "";

            long ticks = (TimeSpan.TicksPerMinute * intervalMinutes);

            foreach (var fluxTable in fluxTables)
            {
                foreach (var fluxRecord in fluxTable.Records)
                {
                    var instant = fluxRecord.GetTime();
                    if (instant is null) continue;
                    DateTime time = instant.Value.ToDateTimeUtc();

                    DateTime truncatedTime = new DateTime(time.Ticks / ticks * ticks);

                    var value = fluxRecord.GetValue();
                    var measurement = fluxRecord.GetMeasurement();
                    if (measurement != currentMeasurement)
                    {
                        if (!data.ContainsKey(measurement))
                        {
                            data.Add(measurement, new Dictionary<DateTime, object>());
                        }
                        currentMeasurement = measurement;
                    }

                    data[measurement].TryAdd(truncatedTime, value);
                }
            }
        }

        DateTime current = startDate;

        List<string> sortedTrends = data.Keys.Order().ToList();

        StringBuilder builder = new();

        builder.Append("DateTime");
        foreach (var t in sortedTrends) builder.Append($"\t{t}");
        builder.Append('\n');

        while (current < endDate)
        {
            builder.Append($"{current:yyyy-MM-dd HH:mm}");
            foreach (var t in sortedTrends)
            {
                var success = data[t].TryGetValue(current, out var value);
                if (success && value is double doubleVal)
                {
                    builder.Append($"\t{doubleVal}");
                }
                else
                {
                    builder.Append('\t');
                }
            }

            builder.Append('\n');
            current = current.AddMinutes(intervalMinutes);
        }

        return (builder.ToString(), true);
    }
}
