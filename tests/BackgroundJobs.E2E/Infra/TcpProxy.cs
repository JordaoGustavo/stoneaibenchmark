using System.Net;
using System.Net.Sockets;
using Npgsql;

namespace BackgroundJobs.E2E.Infra;

/// Forwards one node's database connections, so a test can cut that node off from Postgres (a network partition or a
/// database outage as that node sees it) while other nodes keep their access.
public sealed class TcpProxy : IAsyncDisposable
{
    private readonly TcpListener _listener = new(IPAddress.Loopback, 0);
    private readonly string _host;
    private readonly int _port;
    private readonly List<TcpClient> _open = [];
    private readonly CancellationTokenSource _stop = new();
    private volatile bool _down;

    /// <paramref name="connectionString"/> rewritten to go through the proxy.
    public string ConnectionString { get; }

    public TcpProxy(string connectionString)
    {
        var target = new NpgsqlConnectionStringBuilder(connectionString);
        (_host, _port) = (target.Host!, target.Port);
        _listener.Start();
        ConnectionString = new NpgsqlConnectionStringBuilder(connectionString)
        {
            Host = "127.0.0.1", Port = ((IPEndPoint)_listener.LocalEndpoint).Port,
        }.ConnectionString;
        _ = AcceptAsync();
    }

    /// Drops every open connection and refuses new ones until <see cref="Restore"/>.
    public void Cut()
    {
        _down = true;
        lock (_open)
        {
            foreach (var client in _open) client.Dispose();
            _open.Clear();
        }
    }

    public void Restore() => _down = false;

    private async Task AcceptAsync()
    {
        while (!_stop.IsCancellationRequested)
        {
            TcpClient client;
            try { client = await _listener.AcceptTcpClientAsync(_stop.Token); }
            catch (Exception) { return; }
            if (_down)
            {
                client.Dispose();
                continue;
            }
            _ = ForwardAsync(client);
        }
    }

    private async Task ForwardAsync(TcpClient client)
    {
        using var upstream = new TcpClient();
        try
        {
            await upstream.ConnectAsync(_host, _port, _stop.Token);
            lock (_open) _open.AddRange([client, upstream]);
            var a = client.GetStream().CopyToAsync(upstream.GetStream(), _stop.Token);
            var b = upstream.GetStream().CopyToAsync(client.GetStream(), _stop.Token);
            await Task.WhenAny(a, b);
        }
        catch (Exception) { }
        finally
        {
            lock (_open) _open.RemoveAll(c => c == client || c == upstream);
            client.Dispose();
        }
    }

    public ValueTask DisposeAsync()
    {
        _stop.Cancel();
        _listener.Stop();
        Cut();
        return ValueTask.CompletedTask;
    }
}
