namespace S3HttpRangeStream;

using System;
using System.IO;
using System.Net.Http;
using System.Net.Http.Headers;
using System.Threading;
using System.Threading.Tasks;

/// <summary>
/// A seekable stream that reads from a presigned S3 URL using HTTP Range requests.
/// </summary>
/// <remarks>
/// Optimized for minimal memory use and streaming large files.
/// </remarks>
public class HttpRangeSeekableStream : Stream
{
    private readonly HttpClient _httpClient;
    private readonly string _url;
    private readonly long _length;
    private long _position;

    private byte[] _buffer = [];
    private long _bufferStart;
    private int _bufferLength;

    private const int BUFFER_SIZE = 128 * 1024; // Tuned for performance: 128 KB

    private HttpRangeSeekableStream(string url, long length, HttpClient httpClient)
    {
        _url = url;
        _length = length;
        _httpClient = httpClient;
    }

    /// <summary>
    /// Factory method to create an instance of the stream and automatically determine content length.
    /// </summary>
    /// <param name="presignedUrl">The presigned S3 URL.</param>
    /// <param name="httpClient">Optional shared HttpClient instance.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>A new seekable stream instance.</returns>
    /// <exception cref="InvalidOperationException">Thrown if content length cannot be determined.</exception>
    public static async Task<HttpRangeSeekableStream> CreateAsync(string presignedUrl, HttpClient? httpClient = null, CancellationToken cancellationToken = default)
    {
        httpClient ??= new HttpClient();

        var request = new HttpRequestMessage(HttpMethod.Get, presignedUrl);
        request.Headers.Range = new RangeHeaderValue(0, 0); // Min size request to get content length

        var response = await httpClient.SendAsync(request, HttpCompletionOption.ResponseHeadersRead, cancellationToken);
        response.EnsureSuccessStatusCode();

        long length = response.Content.Headers.ContentRange?.Length
            ?? throw new InvalidOperationException("Content-Range header missing from S3 response");

        return new HttpRangeSeekableStream(presignedUrl, length, httpClient);
    }

    /// <inheritdoc />
    public override bool CanRead => true;

    /// <inheritdoc />
    public override bool CanSeek => true;

    /// <inheritdoc />
    public override bool CanWrite => false;

    /// <inheritdoc />
    public override long Length => _length;

    /// <inheritdoc />
    public override long Position
    {
        get => _position;
        set => Seek(value, SeekOrigin.Begin);
    }

    /// <inheritdoc />
    public override int Read(byte[] buffer, int offset, int count)
    {
        return ReadAsync(buffer, offset, count).GetAwaiter().GetResult();
    }

    /// <inheritdoc />
    public override async Task<int> ReadAsync(byte[] buffer, int offset, int count, CancellationToken cancellationToken)
    {
        if (_position >= _length)
            return 0;

        if (!IsInBuffer(_position))
        {
            await FillBufferAsync(_position, Math.Min(BUFFER_SIZE, _length - _position), cancellationToken);
        }

        int bufferOffset = (int)(_position - _bufferStart);
        int bytesToCopy = Math.Min(count, _bufferLength - bufferOffset);
        Array.Copy(_buffer, bufferOffset, buffer, offset, bytesToCopy);
        _position += bytesToCopy;

        return bytesToCopy;
    }

    private bool IsInBuffer(long pos)
    {
        return _buffer.Length > 0 &&
               pos >= _bufferStart &&
               pos < _bufferStart + _bufferLength;
    }

    private async Task FillBufferAsync(long start, long length, CancellationToken cancellationToken)
    {
        var request = new HttpRequestMessage(HttpMethod.Get, _url);
        request.Headers.Range = new RangeHeaderValue(start, start + length - 1);

        var response = await _httpClient.SendAsync(request, HttpCompletionOption.ResponseHeadersRead, cancellationToken);
        response.EnsureSuccessStatusCode();

        _buffer = await response.Content.ReadAsByteArrayAsync();
        _bufferStart = start;
        _bufferLength = _buffer.Length;
    }

    /// <inheritdoc />
    public override long Seek(long offset, SeekOrigin origin)
    {
        long newPos = origin switch
        {
            SeekOrigin.Begin => offset,
            SeekOrigin.Current => _position + offset,
            SeekOrigin.End => _length + offset,
            _ => throw new ArgumentOutOfRangeException(nameof(origin))
        };

        if (newPos < 0 || newPos > _length)
            throw new ArgumentOutOfRangeException(nameof(offset), "Seek position is out of bounds.");

        _position = newPos;
        return _position;
    }

    /// <inheritdoc />
    public override void Flush() { }

    /// <inheritdoc />
    public override void SetLength(long value) =>
        throw new NotSupportedException();

    /// <inheritdoc />
    public override void Write(byte[] buffer, int offset, int count) =>
        throw new NotSupportedException();
}
