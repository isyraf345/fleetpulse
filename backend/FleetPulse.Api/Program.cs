using System.Globalization;
using System.Data;
using System.Text.RegularExpressions;
using Npgsql;
using NpgsqlTypes;

var builder = WebApplication.CreateBuilder(args);
builder.Services.AddSingleton(NpgsqlDataSource.Create(builder.Configuration.GetConnectionString("FleetPulse")
    ?? throw new InvalidOperationException("Configure ConnectionStrings__FleetPulse with the read-only database account.")));
var app = builder.Build();
app.UseDefaultFiles();
app.UseStaticFiles();
app.Use(async (context, next) =>
{
    try { await next(context); }
    catch (OperationCanceledException) when (context.RequestAborted.IsCancellationRequested) { }
    catch (Exception error)
    {
        var (status, title, detail) = error switch
        {
            InputException e => (400, "Invalid request", e.Message),
            MissingException => (404, "Run not found", "No published run exists with this identifier."),
            NpgsqlException => (503, "Database unavailable", "The forecast database is temporarily unavailable."),
            TimeoutException => (503, "Database unavailable", "The forecast database is temporarily unavailable."),
            _ => (500, "Internal error", "The request could not be completed.")
        };
        // Do not log exception messages: provider messages may contain SQL or connection details.
        app.Logger.LogWarning("Request {TraceId} failed with {ErrorType}", context.TraceIdentifier, error.GetType().Name);
        await Results.Problem(statusCode: status, title: title, detail: detail,
            extensions: new Dictionary<string, object?> { ["traceId"] = context.TraceIdentifier }).ExecuteAsync(context);
    }
});
app.MapGet("/health", async (NpgsqlDataSource db, CancellationToken ct) =>
{
    await using var command = db.CreateCommand("SELECT 1");
    await command.ExecuteScalarAsync(ct);
    return Results.Ok(new { status = "ready" });
});
app.MapGet("/api/forecast-runs", async (HttpRequest request, NpgsqlDataSource db, CancellationToken ct) =>
{
    var (limit, offset) = Query.Page(request);
    await using var conn = await db.OpenConnectionAsync(ct);
    await using var tx = await conn.BeginTransactionAsync(IsolationLevel.RepeatableRead, ct);
    await using var countCommand = new NpgsqlCommand("SELECT count(*) FROM fleetpulse.forecast_runs", conn);
    var count = (long)(await countCommand.ExecuteScalarAsync(ct))!;
    await using var activeCommand = new NpgsqlCommand("SELECT run_id FROM fleetpulse.active_releases WHERE slot='historical-demo'", conn);
    var active = await activeCommand.ExecuteScalarAsync(ct) as string;
    var items = await Repository.Runs(conn, "ORDER BY r.published_at_utc DESC,r.run_id DESC LIMIT @limit OFFSET @offset", ct,
        new("limit", limit), new("offset", offset));
    await tx.CommitAsync(ct);
    return Results.Ok(new { items, totalCount = count, limit, offset, activeRunId = active });
});
app.MapGet("/api/forecast-runs/{runId}", async (string runId, NpgsqlDataSource db, CancellationToken ct) =>
{
    Query.Id(runId);
    await using var conn = await db.OpenConnectionAsync(ct);
    return Results.Ok(await Repository.Run(conn, runId, ct));
});
app.MapGet("/api/forecast-runs/{runId}/predictions", async (string runId, HttpRequest request, NpgsqlDataSource db, CancellationToken ct) =>
{
    Query.Id(runId);
    var (limit, offset) = Query.Page(request);
    int? zone = request.Query.ContainsKey("zoneId") ? Query.Integer(request, "zoneId", 1, int.MaxValue, 1) : null;
    var from = Query.Local(request, "fromLocal");
    var to = Query.Local(request, "toLocal");
    await using var conn = await db.OpenConnectionAsync(ct);
    await using var tx = await conn.BeginTransactionAsync(IsolationLevel.RepeatableRead, ct);
    var run = await Repository.Run(conn, runId, ct);
    from ??= DateTime.ParseExact(run.OriginsStartLocal, Query.Format, CultureInfo.InvariantCulture);
    to ??= DateTime.ParseExact(run.OriginsEndExclusiveLocal, Query.Format, CultureInfo.InvariantCulture);
    if (from >= to) throw new InputException("fromLocal must be earlier than toLocal.");
    const string filter = "p.run_id=@id AND (@zone IS NULL OR p.zone_id=@zone) AND p.forecast_origin_local>=@from AND p.forecast_origin_local<@to";
    void Parameters(NpgsqlCommand command)
    {
        command.Parameters.AddWithValue("id", runId);
        command.Parameters.AddWithValue("zone", NpgsqlDbType.Integer, (object?)zone ?? DBNull.Value);
        command.Parameters.AddWithValue("from", NpgsqlDbType.Timestamp, from.Value);
        command.Parameters.AddWithValue("to", NpgsqlDbType.Timestamp, to.Value);
    }
    await using var countCommand = new NpgsqlCommand("SELECT count(*) FROM fleetpulse.predictions p WHERE " + filter, conn);
    Parameters(countCommand);
    var count = (long)(await countCommand.ExecuteScalarAsync(ct))!;
    await using var command = new NpgsqlCommand("""
        SELECT p.zone_id,z.zone_name,z.borough,p.forecast_origin_local,p.target_start_local,p.target_end_local,
               p.predicted_pickups,p.baseline_pickups,p.actual_pickups
        FROM fleetpulse.predictions p JOIN fleetpulse.zones z USING(dataset_id,zone_id) WHERE
        """ + " " + filter + " ORDER BY p.forecast_origin_local,p.zone_id LIMIT @limit OFFSET @offset", conn);
    Parameters(command);
    command.Parameters.AddWithValue("limit", limit);
    command.Parameters.AddWithValue("offset", offset);
    var items = new List<Prediction>();
    await using (var reader = await command.ExecuteReaderAsync(ct))
        while (await reader.ReadAsync(ct))
            items.Add(new(reader.GetInt32(0), reader.GetString(1), reader.GetString(2), Repository.Local(reader,3),
                Repository.Local(reader,4), Repository.Local(reader,5), reader.GetDouble(6), reader.GetDouble(7),
                reader.IsDBNull(8) ? null : reader.GetInt64(8)));
    await tx.CommitAsync(ct);
    return Results.Ok(new { run, items, totalCount = count, limit, offset });
});
app.MapGet("/api/forecast-runs/{runId}/zones", async (string runId, NpgsqlDataSource db, CancellationToken ct) =>
{
    Query.Id(runId);
    await using var conn = await db.OpenConnectionAsync(ct);
    await Repository.Run(conn, runId, ct);
    await using var command = new NpgsqlCommand("""
        SELECT z.zone_id,z.zone_name,z.borough,count(*)
        FROM fleetpulse.predictions p JOIN fleetpulse.zones z USING(dataset_id,zone_id)
        WHERE p.run_id=@id GROUP BY z.zone_id,z.zone_name,z.borough ORDER BY z.zone_name,z.zone_id
        """, conn);
    command.Parameters.AddWithValue("id", runId);
    var items = new List<Zone>();
    await using var reader = await command.ExecuteReaderAsync(ct);
    while (await reader.ReadAsync(ct)) items.Add(new(reader.GetInt32(0),reader.GetString(1),reader.GetString(2),reader.GetInt64(3)));
    return Results.Ok(new { runId, items });
});
app.MapGet("/api/forecast-runs/{runId}/summary", async (string runId, NpgsqlDataSource db, CancellationToken ct) =>
{
    Query.Id(runId);
    await using var conn = await db.OpenConnectionAsync(ct);
    await Repository.Run(conn, runId, ct);
    await using var command = new NpgsqlCommand("""
        SELECT count(*),count(actual_pickups),count(DISTINCT zone_id),sum(actual_pickups)::double precision,
            avg(abs(predicted_pickups-actual_pickups)),avg(abs(baseline_pickups-actual_pickups)),
            sum(abs(predicted_pickups-actual_pickups))/nullif(sum(actual_pickups),0),
            sum(abs(baseline_pickups-actual_pickups))/nullif(sum(actual_pickups),0)
        FROM fleetpulse.predictions WHERE run_id=@id
        """, conn);
    command.Parameters.AddWithValue("id", runId);
    await using var reader = await command.ExecuteReaderAsync(ct);
    await reader.ReadAsync(ct);
    double? Value(int i) => reader.IsDBNull(i) ? null : reader.GetDouble(i);
    return Results.Ok(new { runId, rowCount = reader.GetInt64(0), evaluatedCount = reader.GetInt64(1), zoneCount = reader.GetInt64(2),
        actualPickups = Value(3), modelMae = Value(4), baselineMae = Value(5), modelWape = Value(6), baselineWape = Value(7) });
});
app.Run();

sealed class InputException(string message) : Exception(message);
sealed class MissingException : Exception;
static class Query
{
    public const string Format = "yyyy-MM-dd'T'HH:mm:ss";
    public static void Id(string id) { if (!Regex.IsMatch(id, "\\A[0-9a-f]{64}\\z")) throw new InputException("runId must be a lowercase SHA-256 identifier."); }
    static string? Value(HttpRequest r, string name)
    {
        if (!r.Query.TryGetValue(name, out var values)) return null;
        if (values.Count != 1 || string.IsNullOrEmpty(values[0])) throw new InputException($"{name} requires one value.");
        return values[0];
    }
    public static int Integer(HttpRequest r, string name, int min, int max, int fallback)
    {
        var value = Value(r,name);
        if (value is null) return fallback;
        if (!int.TryParse(value, NumberStyles.None, CultureInfo.InvariantCulture, out var parsed) || parsed < min || parsed > max)
            throw new InputException($"{name} must be an integer between {min} and {max}.");
        return parsed;
    }
    public static (int,int) Page(HttpRequest r) => (Integer(r,"limit",1,500,100),Integer(r,"offset",0,1000000,0));
    public static DateTime? Local(HttpRequest r,string name)
    {
        var value = Value(r,name);
        if (value is null) return null;
        if (!DateTime.TryParseExact(value,Format,CultureInfo.InvariantCulture,DateTimeStyles.None,out var parsed)
            || parsed.Minute != 0 || parsed.Second != 0)
            throw new InputException($"{name} must be an hour-aligned local timestamp (yyyy-MM-ddTHH:mm:ss), without an offset.");
        return parsed;
    }
}
record ForecastRun(string RunId,string DatasetId,string ModelId,string ModelName,string Mode,string SourceSplit,
    string OriginsStartLocal,string OriginsEndExclusiveLocal,string FirstObservedLocal,string LastObservedLocal,
    string PublishedAtUtc,long PredictionCount,bool IsActive,string EventTimeBasis,string CalendarAssumption,
    string FeaturePolicy,string FreshnessStatus,string ExperimentId,string PredictionArtifactSha256,string BaselineArtifactSha256);
record Prediction(int ZoneId,string ZoneName,string Borough,string ForecastOriginLocal,string TargetStartLocal,
    string TargetEndLocal,double PredictedPickups,double BaselinePickups,long? ActualPickups);
record Zone(int ZoneId,string ZoneName,string Borough,long PredictionCount);
static class Repository
{
    public static string Local(NpgsqlDataReader reader,int ordinal) => reader.GetDateTime(ordinal).ToString(Query.Format,CultureInfo.InvariantCulture);
    public static async Task<ForecastRun> Run(NpgsqlConnection conn,string id,CancellationToken ct)
        => (await Runs(conn,"WHERE r.run_id=@id",ct,new NpgsqlParameter("id",id))).SingleOrDefault() ?? throw new MissingException();
    public static async Task<List<ForecastRun>> Runs(NpgsqlConnection conn,string suffix,CancellationToken ct,params NpgsqlParameter[] parameters)
    {
        await using var command = new NpgsqlCommand("""
            SELECT r.run_id,r.dataset_id,r.model_id,m.model_name,r.mode,r.source_split,
                r.origins_start_local,r.origins_end_exclusive_local,d.first_observed_local,d.last_observed_local,
                r.published_at_utc,r.prediction_count,EXISTS(SELECT 1 FROM fleetpulse.active_releases a WHERE a.run_id=r.run_id AND a.slot='historical-demo'),
                d.event_time_basis,d.calendar_assumption,d.feature_policy,m.experiment_id,r.prediction_artifact_sha256,r.baseline_artifact_sha256
            FROM fleetpulse.forecast_runs r JOIN fleetpulse.dataset_snapshots d USING(dataset_id)
            JOIN fleetpulse.model_versions m USING(model_id)
            """ + " " + suffix,conn);
        command.Parameters.AddRange(parameters);
        var items = new List<ForecastRun>();
        await using var reader = await command.ExecuteReaderAsync(ct);
        while (await reader.ReadAsync(ct))
            items.Add(new(reader.GetString(0),reader.GetString(1),reader.GetString(2),reader.GetString(3),reader.GetString(4),reader.GetString(5),
                Local(reader,6),Local(reader,7),Local(reader,8),Local(reader,9),reader.GetDateTime(10).ToUniversalTime().ToString("O",CultureInfo.InvariantCulture),
                reader.GetInt64(11),reader.GetBoolean(12),reader.GetString(13),reader.GetString(14),reader.GetString(15),"historical_only",
                reader.GetString(16),reader.GetString(17),reader.GetString(18)));
        return items;
    }
}
