using Microsoft.EntityFrameworkCore;
using Webhooks.API.Model;

namespace Webhooks.API.Infrastructure;

public class WebhooksContext(DbContextOptions<WebhooksContext> options) : DbContext(options)
{
    public DbSet<WebhookEndpoint> Endpoints => Set<WebhookEndpoint>();
    public DbSet<PaymentEvent> Events => Set<PaymentEvent>();
    public DbSet<WebhookDelivery> Deliveries => Set<WebhookDelivery>();

    protected override void OnModelCreating(ModelBuilder b)
    {
        b.Entity<WebhookEndpoint>(e =>
        {
            e.HasIndex(x => x.MerchantId);
            e.Property(x => x.Url).IsRequired();
            e.Property(x => x.Secret).IsRequired();
        });

        b.Entity<PaymentEvent>(e => e.HasIndex(x => x.MerchantId));

        b.Entity<WebhookDelivery>(e =>
        {
            e.HasOne(x => x.Event).WithMany().HasForeignKey(x => x.EventId);
            e.HasOne(x => x.Endpoint).WithMany().HasForeignKey(x => x.EndpointId).OnDelete(DeleteBehavior.Cascade);
            e.HasIndex(x => new { x.EventId, x.EndpointId }).IsUnique();
            e.HasIndex(x => new { x.Status, x.NextAttemptAt });
            e.Property(x => x.Status).HasConversion<string>();
        });
    }
}
