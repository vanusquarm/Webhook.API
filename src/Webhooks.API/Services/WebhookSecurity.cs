using System.Net;
using System.Net.Sockets;
using System.Security.Cryptography;
using System.Text;

namespace Webhooks.API.Services;

public static class WebhookSecurity
{
    public static string GenerateSecret() =>
        "whsec_" + Convert.ToBase64String(RandomNumberGenerator.GetBytes(32))
            .Replace('+', '-').Replace('/', '_').TrimEnd('=');

    // Receivers verify: HMAC-SHA256(secret, "{timestamp}.{body}") and reject stale timestamps.
    public static string Sign(string secret, long timestamp, string body)
    {
        var key = Encoding.UTF8.GetBytes(secret);
        var hash = HMACSHA256.HashData(key, Encoding.UTF8.GetBytes($"{timestamp}.{body}"));
        return "v1=" + Convert.ToHexStringLower(hash);
    }

    // Blocks non-HTTPS and internal addresses (SSRF) unless private targets are explicitly allowed.
    public static async Task<bool> IsAllowedTargetAsync(string url, bool allowPrivate, CancellationToken ct = default)
    {
        if (!Uri.TryCreate(url, UriKind.Absolute, out var uri)) return false;
        if (uri.Scheme != Uri.UriSchemeHttps && !(allowPrivate && uri.Scheme == Uri.UriSchemeHttp)) return false;
        if (uri.UserInfo.Length > 0) return false;
        if (allowPrivate) return true;

        try
        {
            var addresses = await Dns.GetHostAddressesAsync(uri.DnsSafeHost, ct);
            return addresses.Length > 0 && addresses.All(a => !IsInternal(a));
        }
        catch (SocketException)
        {
            return false;
        }
    }

    private static bool IsInternal(IPAddress ip)
    {
        if (ip.IsIPv4MappedToIPv6) ip = ip.MapToIPv4();
        if (IPAddress.IsLoopback(ip)) return true;

        if (ip.AddressFamily == AddressFamily.InterNetworkV6)
            return ip.IsIPv6LinkLocal || ip.IsIPv6SiteLocal || ip.IsIPv6UniqueLocal || ip.Equals(IPAddress.IPv6Any);

        var b = ip.GetAddressBytes();
        return b[0] == 0 || b[0] == 10 || b[0] == 127
            || (b[0] == 100 && b[1] is >= 64 and <= 127)
            || (b[0] == 169 && b[1] == 254)
            || (b[0] == 172 && b[1] is >= 16 and <= 31)
            || (b[0] == 192 && b[1] == 168);
    }
}
