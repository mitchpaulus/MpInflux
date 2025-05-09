using System.Text;
using InfluxDB.Client;
using InfluxDB.Client.Core.Flux.Domain;
using NodaTime;
using File = System.IO.File;

namespace MpInflux;

public class Program
{
    public static async Task<int> Main(string[] args)
    {
        Format fmt = Format.Tsv;
        Tz tz = Tz.Utc;

        if (args.Any(s => s == "--help" || s == "-h"))
        {
            Console.Write("MpInflux [--mp] [--cst] < FILE\n");
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
            Console.Write("This utilizes a simple walk-back alignment algorithm. Output time is in UTC.\n");

            return 0;
        }

        int i = 0;

        string? filepath = null;
        while (i < args.Length)
        {
            string arg = args[i++];

            if (arg == "--mp")
            {
                fmt = Format.Mp;
            }
            else if (arg == "--cst")
            {
                tz = Tz.Cst;
            }
            else
            {
                if (i != args.Length)
                {
                    await Console.Error.WriteAsync("Only a single input file expected. Found more than one.");
                    return 1;
                }
                filepath = arg;
            }
        }

        string inputText;
        if (filepath is not null)
        {
            // Read input from file
            inputText = await File.ReadAllTextAsync(filepath);
        }
        else
        {
            // Read input from standard input
            inputText = await Console.In.ReadToEndAsync();
        }

        string output;
        bool success;

        if (fmt == Format.Tsv)
        {
            (output, success) = await GetOutput(inputText, tz);
        }
        else if (fmt == Format.Mp)
        {
            (output, success) = await GetOutputMp(inputText, tz);
        }
        else
        {
            throw new NotImplementedException();
        }

        Console.Write(output);
        return success ? 0 : 1;
    }

    public static async Task<(string, bool)> GetOutputMp(string inputText, Tz tz)
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
        // var intervalMinutes = int.Parse(lines[3].Trim());
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

        var batchedTrends = trends.Batch(5);
        Dictionary<string, Dictionary<DateTime, object>> data = new();

        foreach (var trendBatch in batchedTrends)
        {
            string orStatement = string.Join(" or ", trendBatch.Select(s => $"r._measurement == \"{s}\""));

            // Generate query based on input and alignment
            var query = $"from(bucket: \"{bucket}\") |> range(start: {startDate:yyyy-MM-dd}, stop: {endDate:yyyy-MM-dd}) |> filter(fn: (r) => {orStatement})";
            List<FluxTable> fluxTables = await queryApi.QueryAsync(query, org);
            string currentMeasurement = "";


            var dateTimeZone = DateTimeZoneProviders.Tzdb["America/Chicago"];
            foreach (var fluxTable in fluxTables)
            {
                foreach (var fluxRecord in fluxTable.Records)
                {
                    var instant = fluxRecord.GetTime();
                    if (instant is null) continue;

                    DateTime time;
                    if (tz == Tz.Cst)
                    {
                        time = instant.Value.InZone(dateTimeZone).ToDateTimeUnspecified();
                    }
                    else if (tz == Tz.Utc)
                    {
                        time = instant.Value.ToDateTimeUtc();
                    }
                    else
                    {
                        throw new NotImplementedException();
                    }

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

                    data[measurement].TryAdd(time, value);
                }
            }
        }
        List<string> sortedTrends = data.Keys.Order().ToList();
        StringBuilder builder = new();

        foreach (var t in sortedTrends)
        {
            builder.Append($"{t}\n");

            var datetimes = data[t].Keys.ToList();
            datetimes.Sort();

            foreach (var datetime in datetimes)
            {
                // TODO: sig fig handling.
                builder.Append($"{datetime:yyyy-MM-dd HH:mm}\t{data[t][datetime]}\n");
            }
        }

        return (builder.ToString(), true);

        // builder.Append("DateTime");
        // foreach (var t in sortedTrends) builder.Append($"\t{t}");
        // builder.Append('\n');
        //
        // while (current < endDate)
        // {
        //     builder.Append($"{current:yyyy-MM-dd HH:mm}");
        //     foreach (var t in sortedTrends)
        //     {
        //         var success = data[t].TryGetValue(current, out var value);
        //         if (success && value is double doubleVal)
        //         {
        //             builder.Append($"\t{doubleVal}");
        //         }
        //         else
        //         {
        //             builder.Append('\t');
        //         }
        //     }
        //
        //     builder.Append('\n');
        //     current = current.AddMinutes(intervalMinutes);
        // }
        //
        // return (builder.ToString(), true);
    }


    public static async Task<(string, bool)> GetOutput(string inputText, Tz tz)
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

        var dateTimeZone = DateTimeZoneProviders.Tzdb["America/Chicago"];
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
                    // DateTime time = instant.Value.ToDateTimeUtc();
                    DateTime time;
                    if (tz == Tz.Cst)
                    {
                        time = instant.Value.InZone(dateTimeZone).ToDateTimeUnspecified();
                    }
                    else if (tz == Tz.Utc)
                    {
                        time = instant.Value.ToDateTimeUtc();
                    }
                    else
                    {
                        throw new NotImplementedException();
                    }
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

public enum Format
{
    Tsv,
    Mp,
}

public enum Tz
{
    Utc,
    Cst
}
