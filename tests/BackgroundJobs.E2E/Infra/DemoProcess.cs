using System.Diagnostics;
using System.Net;
using System.Net.Sockets;

namespace BackgroundJobs.E2E.Infra;

/// The demo app running as a real OS process, so tests can SIGTERM or kill -9 it.
public sealed class DemoProcess : IAsyncDisposable
{
    private readonly Process _process;
    private readonly List<string> _output = [];

    public ApiClient Api { get; }
    public string Output { get { lock (_output) return string.Join('\n', _output); } }

    private DemoProcess(Process process, int port)
    {
        _process = process;
        Api = new ApiClient(new Uri($"http://127.0.0.1:{port}"));
    }

    /// Recovery waits for a dead node's lease to expire; a short one keeps crash scenarios fast.
    public static readonly IReadOnlyDictionary<string, string> ShortLease = new Dictionary<string, string> { ["LEASE_MS"] = "1500" };

    /// A null <paramref name="connectionString"/> runs the demo on the in-memory store.
    public static async Task<DemoProcess> StartAsync(
        string? connectionString, int workers = 4, int? port = null, IReadOnlyDictionary<string, string>? env = null)
    {
        port ??= FreePort();
        var psi = new ProcessStartInfo("dotnet", DemoDll())
        {
            RedirectStandardOutput = true,
            RedirectStandardError = true,
            UseShellExecute = false,
        };
        psi.Environment["PORT"] = port.ToString();
        psi.Environment["WORKER_COUNT"] = workers.ToString();
        if (connectionString == null) psi.Environment["STORAGE"] = "memory";
        else psi.Environment["DATABASE_URL"] = connectionString;
        psi.Environment["ASPNETCORE_ENVIRONMENT"] = "Development";
        foreach (var (key, value) in env ?? new Dictionary<string, string>()) psi.Environment[key] = value;

        var demo = new DemoProcess(Process.Start(psi)!, port.Value);
        demo._process.OutputDataReceived += (_, e) => { if (e.Data != null) lock (demo._output) demo._output.Add(e.Data); };
        demo._process.ErrorDataReceived += (_, e) => { if (e.Data != null) lock (demo._output) demo._output.Add(e.Data); };
        demo._process.BeginOutputReadLine();
        demo._process.BeginErrorReadLine();
        await demo.WaitHealthyAsync();
        return demo;
    }

    public int Port => Api.BaseAddress.Port;

    private async Task WaitHealthyAsync()
    {
        var deadline = DateTime.UtcNow.AddSeconds(30);
        while (DateTime.UtcNow < deadline)
        {
            if (_process.HasExited) throw new InvalidOperationException("demo exited:\n" + Output);
            if (await Api.IsHealthyAsync()) return;
            await Task.Delay(100);
        }
        throw new TimeoutException("demo never became healthy:\n" + Output);
    }

    /// kill -9
    public async Task KillAsync()
    {
        _process.Kill(entireProcessTree: true);
        await _process.WaitForExitAsync();
    }

    /// SIGTERM: graceful stop.
    public void Terminate() => Process.Start("kill", ["-TERM", _process.Id.ToString()])!.WaitForExit();

    public bool HasExited => _process.HasExited;

    public Task WaitForExitAsync(TimeSpan timeout) => _process.WaitForExitAsync(new CancellationTokenSource(timeout).Token);

    public async ValueTask DisposeAsync()
    {
        if (!_process.HasExited) await KillAsync();
        _process.Dispose();
    }

    private static int FreePort()
    {
        var l = new TcpListener(IPAddress.Loopback, 0);
        l.Start();
        var port = ((IPEndPoint)l.LocalEndpoint).Port;
        l.Stop();
        return port;
    }

    private static string DemoDll()
    {
        var dir = new DirectoryInfo(AppContext.BaseDirectory);
        while (dir != null && !File.Exists(Path.Combine(dir.FullName, "BackgroundJobs.sln"))) dir = dir.Parent;
        if (dir == null) throw new InvalidOperationException("repo root not found");
        // Same configuration as the test build (bin/<Configuration>/net10.0).
        var configuration = new DirectoryInfo(AppContext.BaseDirectory).Parent!.Name;
        return Path.Combine(dir.FullName, "src", "BackgroundJobs.Demo", "bin", configuration, "net10.0", "BackgroundJobs.Demo.dll");
    }
}
