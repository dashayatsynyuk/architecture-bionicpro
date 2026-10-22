using System.Globalization;
using System.Security.Claims;
using System.Text;
using Microsoft.AspNetCore.Authentication.JwtBearer;
using Microsoft.IdentityModel.Tokens;
using Npgsql;
using NpgsqlTypes;

var builder = WebApplication.CreateBuilder(args);

var authority = builder.Configuration["Keycloak:Authority"]!;
var audience = builder.Configuration["Keycloak:Audience"]!;

var connectionString = builder.Configuration.GetConnectionString("ReportsDb")
    ?? throw new InvalidOperationException("Missing ReportsDb connection string.");

builder.Services.AddSingleton(NpgsqlDataSource.Create(connectionString));

// Add services to the container.
// Learn more about configuring OpenAPI at https://aka.ms/aspnet/openapi
builder.Services.AddOpenApi();
builder.Services.AddCors(options =>
{
    options.AddPolicy("Frontend", policy =>
        policy.WithOrigins("http://localhost:3000")
              .WithMethods("GET")
              .WithHeaders("Authorization", "Content-Type"));
});
builder.Services
    .AddAuthentication(JwtBearerDefaults.AuthenticationScheme)
    .AddJwtBearer(options =>
    {
        options.Authority = authority;
        options.Audience = audience;
        options.RequireHttpsMetadata = false;

        options.TokenValidationParameters = new TokenValidationParameters
        {
            ValidateIssuer = true,
            ValidIssuer = "http://localhost:8080/realms/reports-realm",
            ValidateAudience = true,
            ValidAudience = audience,
            ValidateLifetime = true
        };
    });

builder.Services.AddAuthorization();

var app = builder.Build();
app.UseCors("Frontend");
app.UseAuthentication();
app.UseAuthorization();
// Configure the HTTP request pipeline.
if (app.Environment.IsDevelopment())
{
    app.MapOpenApi();
}


var summaries = new[]
{
    "Freezing", "Bracing", "Chilly", "Cool", "Mild", "Warm", "Balmy", "Hot", "Sweltering", "Scorching"
};

app.MapGet("/ping", () => "API is responding");

app.MapGet("/reports", async (
    HttpContext context,
    NpgsqlDataSource db,
    DateOnly? from,
    DateOnly? to,
    CancellationToken ct) =>
{
    var userId = context.User.FindFirst("preferred_username")?.Value
        ?? context.User.FindFirst(ClaimTypes.NameIdentifier)?.Value;

    Console.WriteLine($"{userId}, {context.User}"); 

    if (string.IsNullOrEmpty(userId))
        return Results.Unauthorized();

    if (from is not null && to is not null && from > to)
        return Results.BadRequest("from must be on or before to.");

    const string sql = """
        SELECT user_id, event_date, full_name, status,
               event_type, event_count
        FROM client_event_daily_aggregates
        WHERE user_id = @userId
          AND (@fromDate IS NULL OR event_date >= @fromDate)
          AND (@toDate IS NULL OR event_date <= @toDate)
        ORDER BY event_date, event_type;
        """;

    await using var cmd = db.CreateCommand(sql);
    cmd.Parameters.AddWithValue("userId", userId);
    cmd.Parameters.Add(new NpgsqlParameter("fromDate", NpgsqlDbType.Date)
    {
        Value = from is null ? DBNull.Value : from.Value
    });
    cmd.Parameters.Add(new NpgsqlParameter("toDate", NpgsqlDbType.Date)
    {
        Value = to is null ? DBNull.Value : to.Value
    });

    var csv = new StringBuilder();
    csv.AppendLine("user_id,event_date,full_name,status,event_type,event_count");

    await using var reader = await cmd.ExecuteReaderAsync(ct);
    while (await reader.ReadAsync(ct))
    {
        var eventType = reader.IsDBNull(4)
            ? ""
            : reader.GetInt32(4).ToString(CultureInfo.InvariantCulture);

        csv.Append(Csv(reader.GetString(0))).Append(',')
           .Append(reader.GetFieldValue<DateOnly>(1).ToString("yyyy-MM-dd", CultureInfo.InvariantCulture)).Append(',')
           .Append(Csv(reader.GetString(2))).Append(',')
           .Append(Csv(reader.GetString(3))).Append(',')
           .Append(Csv(eventType)).Append(',')
           .Append(reader.GetInt64(5).ToString(CultureInfo.InvariantCulture))
           .AppendLine();
    }

    return Results.File(
        Encoding.UTF8.GetBytes(csv.ToString()),
        contentType: "text/csv; charset=utf-8",
        fileDownloadName: "reports.csv");
})
.WithName("reports")
.RequireAuthorization();

app.Run();


static string Csv(string value) =>
    "\"" + value.Replace("\"", "\"\"") + "\"";
