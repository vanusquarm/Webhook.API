namespace Webhooks.API.Model;

public static class PaymentEventTypes
{
    public const string Succeeded = "payment.succeeded";
    public const string Failed = "payment.failed";
    public const string Refunded = "payment.refunded";
    public const string DisputeCreated = "payment.dispute.created";

    public static readonly string[] All = [Succeeded, Failed, Refunded, DisputeCreated];
}

public class WebhookEndpoint
{
    public Guid Id { get; set; } = Guid.NewGuid();
    public string MerchantId { get; set; } = "";
    public string Url { get; set; } = "";
    // Needed in plaintext to sign payloads; protect the database accordingly.
    public string Secret { get; set; } = "";
    // Comma-separated event types, or "*" for all.
    public string EventTypes { get; set; } = "*";
    public bool Enabled { get; set; } = true;
    public DateTime CreatedAt { get; set; } = DateTime.UtcNow;

    public bool Matches(string type) =>
        EventTypes == "*" || EventTypes.Split(',').Contains(type);
}

public class PaymentEvent
{
    // Publisher-supplied id; the primary key makes ingestion idempotent.
    public string Id { get; set; } = "";
    public string MerchantId { get; set; } = "";
    public string Type { get; set; } = "";
    public string Payload { get; set; } = "";
    public DateTime CreatedAt { get; set; }
}

public enum DeliveryStatus { Pending, Succeeded, Failed }

public class WebhookDelivery
{
    public Guid Id { get; set; } = Guid.NewGuid();
    public string EventId { get; set; } = "";
    public PaymentEvent Event { get; set; } = null!;
    public Guid EndpointId { get; set; }
    public WebhookEndpoint Endpoint { get; set; } = null!;
    public DeliveryStatus Status { get; set; } = DeliveryStatus.Pending;
    public int Attempts { get; set; }
    public DateTime NextAttemptAt { get; set; } = DateTime.UtcNow;
    public int? LastStatusCode { get; set; }
    public string? LastError { get; set; }
    public DateTime? DeliveredAt { get; set; }
}

// Amounts are in minor units (e.g. cents).
public record PaymentMessage(
    string EventId,
    string MerchantId,
    string PaymentId,
    long Amount,
    string Currency,
    string Status,
    DateTime OccurredAt,
    Dictionary<string, string>? Metadata);

public record CreateEndpointRequest(string Url, string[]? Events);
