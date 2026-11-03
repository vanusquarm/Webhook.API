using System.Text.Json;
using Microsoft.EntityFrameworkCore;
using RabbitMQ.Client;
using Webhooks.API.Events;
using Webhooks.API.Infrastructure;
using Webhooks.API.Model;
using Webhooks.API.Services;

var builder = WebApplication.CreateBuilder(args);

builder.Services.AddDbContext<WebhooksContext>(o =>
    o.UseSqlite(builder.Configuration.GetConnectionString("Webhooks") ?? "Data Source=webhooks.db"));
builder.Services.AddHttpClient("webhooks", c => c.Timeout = TimeSpan.FromSeconds(10))
    .ConfigurePrimaryHttpMessageHandler(() => new HttpClientHandler { AllowAutoRedirect = false });
builder.Services.AddScoped<EventIngestor>();

builder.Services.AddSingleton<IConnectionFactory>(sp =>
{
    var cfg = sp.GetRequiredService<IConfiguration>().GetSection("RabbitMQ");
    return new ConnectionFactory
    {
        HostName = cfg["Host"] ?? "localhost",
        Port = int.Parse(cfg["Port"] ?? "5672"),
        UserName = cfg["UserName"] ?? "guest",
        Password = cfg["Password"] ?? "guest"
    };
});
builder.Services.AddHostedService<PaymentEventConsumer>();
builder.Services.AddHostedService<DeliveryDispatcher>();

var app = builder.Build();
var allowPrivate = app.Configuration.GetValue<bool>("Webhooks:AllowPrivateTargets");

using (var scope = app.Services.CreateScope())
    await scope.ServiceProvider.GetRequiredService<WebhooksContext>().Database.EnsureCreatedAsync();

// Stand-in for real merchant authentication (API key / JWT).
static string? Merchant(HttpRequest r) => r.Headers["X-Merchant-Id"].FirstOrDefault() is { Length: > 0 } m ? m : null;

static object View(WebhookEndpoint e) => new { e.Id, e.Url, Events = e.EventTypes.Split(','), e.Enabled, e.CreatedAt };

var api = app.MapGroup("/api/endpoints");

api.MapPost("/", async (CreateEndpointRequest body, HttpRequest req, WebhooksContext db) =>
{
    if (Merchant(req) is not { } merchant) return Results.Unauthorized();

    var events = body.Events is { Length: > 0 } ? body.Events.Distinct().ToArray() : ["*"];
    var invalid = events.Where(t => t != "*" && !PaymentEventTypes.All.Contains(t)).ToArray();
    if (invalid.Length > 0)
        return Results.BadRequest($"Unknown events: {string.Join(", ", invalid)}. Valid: {string.Join(", ", PaymentEventTypes.All)}");

    if (!await WebhookSecurity.IsAllowedTargetAsync(body.Url, allowPrivate))
        return Results.BadRequest("URL must be a public HTTPS address");

    var endpoint = new WebhookEndpoint
    {
        MerchantId = merchant,
        Url = body.Url,
        EventTypes = string.Join(',', events),
        Secret = WebhookSecurity.GenerateSecret()
    };
    db.Endpoints.Add(endpoint);
    await db.SaveChangesAsync();

    // The secret is only ever returned here and on rotation.
    return Results.Created($"/api/endpoints/{endpoint.Id}", new { endpoint.Id, endpoint.Url, Events = events, endpoint.Secret });
});

api.MapGet("/", async (HttpRequest req, WebhooksContext db) =>
    Merchant(req) is { } m
        ? Results.Ok((await db.Endpoints.Where(e => e.MerchantId == m).ToListAsync()).Select(View))
        : Results.Unauthorized());

api.MapGet("/{id:guid}", async (Guid id, HttpRequest req, WebhooksContext db) =>
{
    if (Merchant(req) is not { } m) return Results.Unauthorized();
    var e = await db.Endpoints.SingleOrDefaultAsync(x => x.Id == id && x.MerchantId == m);
    return e is null ? Results.NotFound() : Results.Ok(View(e));
});

api.MapDelete("/{id:guid}", async (Guid id, HttpRequest req, WebhooksContext db) =>
{
    if (Merchant(req) is not { } m) return Results.Unauthorized();
    var e = await db.Endpoints.SingleOrDefaultAsync(x => x.Id == id && x.MerchantId == m);
    if (e is null) return Results.NotFound();
    db.Endpoints.Remove(e);
    await db.SaveChangesAsync();
    return Results.NoContent();
});

api.MapPost("/{id:guid}/rotate-secret", async (Guid id, HttpRequest req, WebhooksContext db) =>
{
    if (Merchant(req) is not { } m) return Results.Unauthorized();
    var e = await db.Endpoints.SingleOrDefaultAsync(x => x.Id == id && x.MerchantId == m);
    if (e is null) return Results.NotFound();
    e.Secret = WebhookSecurity.GenerateSecret();
    await db.SaveChangesAsync();
    return Results.Ok(new { e.Id, e.Secret });
});

api.MapGet("/{id:guid}/deliveries", async (Guid id, HttpRequest req, WebhooksContext db) =>
{
    if (Merchant(req) is not { } m) return Results.Unauthorized();
    if (!await db.Endpoints.AnyAsync(x => x.Id == id && x.MerchantId == m)) return Results.NotFound();

    var rows = (await db.Deliveries.Where(d => d.EndpointId == id)
        .Include(d => d.Event).ToListAsync())
        .OrderByDescending(d => d.Event.CreatedAt).Take(50);
    return Results.Ok(rows.Select(d => new
    {
        d.Id, d.EventId, EventType = d.Event.Type, Status = d.Status.ToString(),
        d.Attempts, d.NextAttemptAt, d.LastStatusCode, d.LastError, d.DeliveredAt
    }));
});

api.MapPost("/{id:guid}/deliveries/{deliveryId:guid}/retry", async (Guid id, Guid deliveryId, HttpRequest req, WebhooksContext db) =>
{
    if (Merchant(req) is not { } m) return Results.Unauthorized();
    var d = await db.Deliveries.Include(x => x.Endpoint)
        .SingleOrDefaultAsync(x => x.Id == deliveryId && x.EndpointId == id && x.Endpoint.MerchantId == m);
    if (d is null) return Results.NotFound();

    d.Status = DeliveryStatus.Pending;
    d.Attempts = 0;
    d.NextAttemptAt = DateTime.UtcNow;
    await db.SaveChangesAsync();
    return Results.Accepted();
});

if (app.Environment.IsDevelopment())
{
    // Publishes a payment event to the bus, e.g. POST /dev/events/payment.succeeded
    app.MapPost("/dev/events/{type}", async (string type, PaymentMessage body, IConnectionFactory factory) =>
    {
        if (!PaymentEventTypes.All.Contains(type)) return Results.BadRequest("Unknown event type");

        await using var conn = await factory.CreateConnectionAsync();
        await using var channel = await conn.CreateChannelAsync();
        await PaymentEventConsumer.DeclareTopologyAsync(channel, default);
        await channel.BasicPublishAsync(Topology.Exchange, type,
            JsonSerializer.SerializeToUtf8Bytes(body, new JsonSerializerOptions(JsonSerializerDefaults.Web)));
        return Results.Accepted();
    });

    // Local receiver that verifies the signature, for trying the flow end to end.
    app.MapPost("/dev/receiver", async (HttpRequest req, IConfiguration cfg, ILogger<Program> log) =>
    {
        using var reader = new StreamReader(req.Body);
        var body = await reader.ReadToEndAsync();
        // Set Webhooks:DevReceiverSecret to the endpoint's secret to enforce verification.
        var secret = cfg["Webhooks:DevReceiverSecret"];
        var valid = string.IsNullOrEmpty(secret)
            || req.Headers["Webhook-Signature"] == WebhookSecurity.Sign(secret, long.Parse(req.Headers["Webhook-Timestamp"]!), body);
        log.LogInformation("Webhook {Id} signature valid={Valid}: {Body}", req.Headers["Webhook-Id"], valid, body);
        return valid ? Results.Ok() : Results.Unauthorized();
    });
}

app.Run();
