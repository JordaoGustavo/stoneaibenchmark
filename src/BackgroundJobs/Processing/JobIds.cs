using System.Security.Cryptography;

namespace BackgroundJobs.Processing;

/// <summary>
/// UUIDv7 job ids that sort in creation order within this process. <see cref="Guid.CreateVersion7(DateTimeOffset)"/>
/// fills everything after the millisecond with random bits, so two ids from the same millisecond sort at random; here the
/// 12-bit rand_a field is a counter (RFC 9562 §6.2, method 1) and the timestamp never goes backwards.
/// </summary>
internal static class JobIds
{
    private static readonly Lock Gate = new();
    private static long _lastMs;
    private static int _counter;

    public static Guid Next(DateTime now)
    {
        long ms;
        int counter;
        lock (Gate)
        {
            ms = new DateTimeOffset(now, TimeSpan.Zero).ToUnixTimeMilliseconds();
            if (ms > _lastMs) { _lastMs = ms; _counter = RandomNumberGenerator.GetInt32(0, 0x800); }
            else if (++_counter > 0xFFF) { _lastMs++; _counter = 0; } // counter exhausted: borrow the next millisecond
            ms = _lastMs;
            counter = _counter;
        }

        Span<byte> b = stackalloc byte[16];
        RandomNumberGenerator.Fill(b[8..]);
        for (var i = 0; i < 6; i++) b[i] = (byte)(ms >> (8 * (5 - i)));
        b[6] = (byte)(0x70 | (counter >> 8));
        b[7] = (byte)counter;
        b[8] = (byte)(0x80 | (b[8] & 0x3F));
        return new Guid(b, bigEndian: true);
    }
}
