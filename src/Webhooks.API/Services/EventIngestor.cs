using System.Text.Json;
using Microsoft.EntityFrameworkCore;
using Webhooks.API.Infrastructure;
using Webhooks.API.Model;

namespace Webhooks.API.Services;

public class EventIngestor(WebhooksContext db, ILogger<EventIngestor> logger)
{
    private static readonly JsonSerializerOptions Json = new(JsonSerializerDefaults.Web);

    // Stores the event and fans it out into one pending delivery per matching endpoint. Duplicate event ids are ignored.
    public async Task IngestAsync(string type, PaymentMessage msg)
    {
        if (await db.Events.AnyAsync(e => e.Id == msg.EventId))
        {
            logger.LogInformation("Duplicate event {Id} ignored", msg.EventId);
            return;
        }

        var envelope = new
        {
            id = msg.EventId,
            type,
            created = msg.OccurredAt,
            data = new { msg.PaymentId, msg.Amount, msg.Currency, msg.Status, msg.Metadata }
        };

        db.Events.Add(new PaymentEvent
        {
            Id = msg.EventId,
            MerchantId = msg.MerchantId,
            Type = type,
            Payload = JsonSerializer.Serialize(envelope, Json),
            CreatedAt = msg.OccurredAt.ToUniversalTime()
        });

        var endpoints = (await db.Endpoints
            .Where(e => e.MerchantId == msg.MerchantId && e.Enabled)
            .ToListAsync())
            .Where(e => e.Matches(type));

        foreach (var endpoint in endpoints)
            db.Deliveries.Add(new WebhookDelivery { EventId = msg.EventId, EndpointId = endpoint.Id });

        try
        {
            await db.SaveChangesAsync();
        }
        catch (DbUpdateException)
        {
            db.ChangeTracker.Clear();
            if (!await db.Events.AnyAsync(e => e.Id == msg.EventId)) throw;
            logger.LogInformation("Event {Id} ingested concurrently; ignored", msg.EventId);
        }
    }
}
