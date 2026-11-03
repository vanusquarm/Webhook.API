using System.Text.Json;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using Webhooks.API.Model;
using Webhooks.API.Services;

namespace Webhooks.API.Events;

public static class Topology
{
    public const string Exchange = "payments";
    public const string Queue = "webhooks.payments";
    public const string DeadExchange = "payments.dlx";
    public const string DeadQueue = "webhooks.payments.dead";
    public const string BindingKey = "payment.#";
}

public class PaymentEventConsumer(
    IConnectionFactory factory,
    IServiceScopeFactory scopeFactory,
    ILogger<PaymentEventConsumer> logger) : BackgroundService
{
    private static readonly JsonSerializerOptions Json = new(JsonSerializerDefaults.Web);

    protected override async Task ExecuteAsync(CancellationToken ct)
    {
        IConnection? connection = null;
        while (connection is null && !ct.IsCancellationRequested)
        {
            try
            {
                connection = await factory.CreateConnectionAsync(ct);
            }
            catch (Exception ex) when (ex is not OperationCanceledException)
            {
                logger.LogWarning("RabbitMQ unavailable ({Message}); retrying in 5s", ex.Message);
                await Task.Delay(TimeSpan.FromSeconds(5), ct);
            }
        }
        if (connection is null) return;

        await using var _ = connection;
        await using var channel = await connection.CreateChannelAsync(cancellationToken: ct);
        await DeclareTopologyAsync(channel, ct);
        await channel.BasicQosAsync(0, 20, false, ct);

        var consumer = new AsyncEventingBasicConsumer(channel);
        consumer.ReceivedAsync += async (_, ea) =>
        {
            try
            {
                await HandleAsync(ea.RoutingKey, ea.Body);
                await channel.BasicAckAsync(ea.DeliveryTag, false);
            }
            catch (Exception ex) when (ex is JsonException or ArgumentException)
            {
                // Malformed messages can never succeed; dead-letter instead of looping.
                logger.LogError(ex, "Rejecting invalid message {Key}", ea.RoutingKey);
                await channel.BasicNackAsync(ea.DeliveryTag, false, requeue: false);
            }
            catch (Exception ex)
            {
                logger.LogError(ex, "Transient failure on {Key}; requeueing", ea.RoutingKey);
                await channel.BasicNackAsync(ea.DeliveryTag, false, requeue: true);
            }
        };

        await channel.BasicConsumeAsync(Topology.Queue, autoAck: false, consumer, ct);
        logger.LogInformation("Consuming {Queue}", Topology.Queue);
        await Task.Delay(Timeout.Infinite, ct);
    }

    public static async Task DeclareTopologyAsync(IChannel channel, CancellationToken ct)
    {
        await channel.ExchangeDeclareAsync(Topology.DeadExchange, ExchangeType.Fanout, durable: true, cancellationToken: ct);
        await channel.QueueDeclareAsync(Topology.DeadQueue, durable: true, exclusive: false, autoDelete: false, cancellationToken: ct);
        await channel.QueueBindAsync(Topology.DeadQueue, Topology.DeadExchange, "", cancellationToken: ct);

        await channel.ExchangeDeclareAsync(Topology.Exchange, ExchangeType.Topic, durable: true, cancellationToken: ct);
        await channel.QueueDeclareAsync(Topology.Queue, durable: true, exclusive: false, autoDelete: false,
            arguments: new Dictionary<string, object?> { ["x-dead-letter-exchange"] = Topology.DeadExchange },
            cancellationToken: ct);
        await channel.QueueBindAsync(Topology.Queue, Topology.Exchange, Topology.BindingKey, cancellationToken: ct);
    }

    private async Task HandleAsync(string type, ReadOnlyMemory<byte> body)
    {
        if (!PaymentEventTypes.All.Contains(type))
            throw new ArgumentException($"Unknown event type {type}");

        var msg = JsonSerializer.Deserialize<PaymentMessage>(body.Span, Json)
            ?? throw new JsonException("Empty message");
        if (string.IsNullOrWhiteSpace(msg.EventId) || string.IsNullOrWhiteSpace(msg.MerchantId))
            throw new ArgumentException("EventId and MerchantId are required");

        using var scope = scopeFactory.CreateScope();
        await scope.ServiceProvider.GetRequiredService<EventIngestor>().IngestAsync(type, msg);
    }
}
