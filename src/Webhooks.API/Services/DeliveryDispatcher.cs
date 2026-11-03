using System.Text;
using Microsoft.EntityFrameworkCore;
using Webhooks.API.Infrastructure;
using Webhooks.API.Model;

namespace Webhooks.API.Services;

// Polls for due deliveries and sends them signed, retrying with backoff.
public class DeliveryDispatcher(
    IServiceScopeFactory scopeFactory,
    IHttpClientFactory httpClientFactory,
    IConfiguration config,
    ILogger<DeliveryDispatcher> logger) : BackgroundService
{
    private static readonly TimeSpan[] Backoff =
    [
        TimeSpan.FromSeconds(10), TimeSpan.FromMinutes(1), TimeSpan.FromMinutes(5),
        TimeSpan.FromMinutes(30), TimeSpan.FromHours(2), TimeSpan.FromHours(6), TimeSpan.FromHours(12)
    ];
    public static int MaxAttempts => Backoff.Length + 1;

    protected override async Task ExecuteAsync(CancellationToken ct)
    {
        using var timer = new PeriodicTimer(TimeSpan.FromSeconds(1));
        while (await timer.WaitForNextTickAsync(ct))
        {
            try
            {
                await DispatchBatchAsync(ct);
            }
            catch (Exception ex) when (ex is not OperationCanceledException)
            {
                logger.LogError(ex, "Dispatch loop error");
            }
        }
    }

    private async Task DispatchBatchAsync(CancellationToken ct)
    {
        using var scope = scopeFactory.CreateScope();
        var db = scope.ServiceProvider.GetRequiredService<WebhooksContext>();

        var now = DateTime.UtcNow;
        var due = await db.Deliveries
            .Include(d => d.Event).Include(d => d.Endpoint)
            .Where(d => d.Status == DeliveryStatus.Pending && d.NextAttemptAt <= now)
            .OrderBy(d => d.NextAttemptAt)
            .Take(50)
            .ToListAsync(ct);
        if (due.Count == 0) return;

        var allowPrivate = config.GetValue<bool>("Webhooks:AllowPrivateTargets");
        await Parallel.ForEachAsync(due, new ParallelOptions { MaxDegreeOfParallelism = 8, CancellationToken = ct },
            (d, token) => new ValueTask(AttemptAsync(d, allowPrivate, token)));

        await db.SaveChangesAsync(ct);
    }

    private async Task AttemptAsync(WebhookDelivery d, bool allowPrivate, CancellationToken ct)
    {
        d.Attempts++;
        d.LastStatusCode = null;
        d.LastError = null;

        if (!d.Endpoint.Enabled)
        {
            d.Status = DeliveryStatus.Failed;
            d.LastError = "Endpoint disabled";
            return;
        }

        try
        {
            if (!await WebhookSecurity.IsAllowedTargetAsync(d.Endpoint.Url, allowPrivate, ct))
            {
                d.Status = DeliveryStatus.Failed;
                d.LastError = "Target URL not allowed";
                return;
            }

            var ts = DateTimeOffset.UtcNow.ToUnixTimeSeconds();
            var request = new HttpRequestMessage(HttpMethod.Post, d.Endpoint.Url)
            {
                Content = new StringContent(d.Event.Payload, Encoding.UTF8, "application/json")
            };
            request.Headers.Add("Webhook-Id", d.EventId);
            request.Headers.Add("Webhook-Timestamp", ts.ToString());
            request.Headers.Add("Webhook-Signature", WebhookSecurity.Sign(d.Endpoint.Secret, ts, d.Event.Payload));

            var response = await httpClientFactory.CreateClient("webhooks").SendAsync(request, ct);
            d.LastStatusCode = (int)response.StatusCode;

            if (response.IsSuccessStatusCode)
            {
                d.Status = DeliveryStatus.Succeeded;
                d.DeliveredAt = DateTime.UtcNow;
                return;
            }
            d.LastError = $"HTTP {(int)response.StatusCode}";
        }
        catch (Exception ex) when (ex is HttpRequestException or TaskCanceledException)
        {
            d.LastError = ex.Message;
        }

        if (d.Attempts >= MaxAttempts)
        {
            d.Status = DeliveryStatus.Failed;
            logger.LogWarning("Delivery {Id} to {Url} permanently failed", d.Id, d.Endpoint.Url);
        }
        else
        {
            d.NextAttemptAt = DateTime.UtcNow + Backoff[d.Attempts - 1];
        }
    }
}
