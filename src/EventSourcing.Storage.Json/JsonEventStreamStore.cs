using System.Collections.ObjectModel;
using System.Text.Json;
using EventSourcing.Streams;
using EventSourcing.Transactions;

namespace EventSourcing.Storage.Json;

/// <summary>Deterministic publication observation points for diagnostics and fault-injection testing.</summary>
public enum JsonPublicationPoint
{
    /// <summary>Staging file is flushed; committed document has not been replaced.</summary>
    BeforeReplace,
    /// <summary>Complete document was replaced and the authoritative in-process view updated.</summary>
    AfterReplace
}

/// <summary>
/// Durable local atomic store. Holds an exclusive OS file lease throughout its lifetime; independent
/// processes/instances cannot open the same configured path simultaneously. Local filesystems supporting
/// atomic same-directory replacement are required. Process-crash recovery is supported; power-loss and
/// network-filesystem durability are not promised. Do not access the document through aliases or bypass this adapter.
/// </summary>
public sealed class JsonEventStreamStore : ITransactionSessionStore, IDisposable
{
    private readonly string _path;
    private readonly FileStream _lease;
    private readonly JsonEventCodec _codec;
    private readonly SemaphoreSlim _gate = new(1, 1);
    private readonly Action<JsonPublicationPoint>? _observe;
    private Dictionary<Guid, IReadOnlyList<IEvent>> _history;
    private bool _disposed;
    /// <inheritdoc />
    public Guid SessionId { get; } = Guid.NewGuid();
    /// <summary>
    /// Open under one lifetime lease; validate retained history before accepting work. Observer exceptions
    /// before replacement leave staging uncommitted; after replacement they model acknowledgement loss.
    /// The observer must not reenter the store. Secret-bearing payloads are never logged by this adapter.
    /// </summary>
    public JsonEventStreamStore(string path, JsonEventCodec codec, Action<JsonPublicationPoint>? observe = null)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(path); ArgumentNullException.ThrowIfNull(codec);
        Directory.CreateDirectory(Path.GetDirectoryName(Path.GetFullPath(path))!);
        _path = CanonicalPath(Path.GetFullPath(path)); _codec = codec.Freeze(); _observe = observe;
        if (new FileInfo(_path).LinkTarget is not null || new FileInfo(_path + ".lock").LinkTarget is not null)
            throw new IOException("Store documents and ownership leases cannot be symbolic links.");
        _lease = new FileStream(_path + ".lock", FileMode.OpenOrCreate, FileAccess.ReadWrite, FileShare.None);
        try
        {
            try { _history = Decode(File.ReadAllText(_path)); }
            catch (FileNotFoundException) { _history = new(); }
            // Only uncommitted staging files in this exact store's private name space may be removed.
            foreach (var file in Directory.EnumerateFiles(Path.GetDirectoryName(_path)!, Path.GetFileName(_path) + ".stage-*"))
                File.Delete(file);
        }
        catch { _lease.Dispose(); throw; }
    }
    /// <inheritdoc />
    public async Task<IReadOnlyList<IEvent>> ReadAsync(Guid streamId, CancellationToken cancellationToken = default) =>
        (await ReadBatchAsync(new[] { streamId }, cancellationToken).ConfigureAwait(false))[streamId];
    /// <inheritdoc />
    public async Task<StreamSlice> ReadFromAsync(Guid streamId, long afterVersion, CancellationToken cancellationToken = default) =>
        (await ReadBatchFromAsync(new[] { new StreamReadRequest(streamId, afterVersion) }, cancellationToken).ConfigureAwait(false))[streamId];
    /// <inheritdoc />
    public async Task<IReadOnlyDictionary<Guid, IReadOnlyList<IEvent>>> ReadBatchAsync(IReadOnlyList<Guid> streamIds,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(streamIds); var ids = streamIds.ToArray();
        if (ids.Any(id => id == Guid.Empty)) throw new ArgumentException("A stream ID is required.", nameof(streamIds));
        await _gate.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            ThrowIfDisposed();
            return new ReadOnlyDictionary<Guid, IReadOnlyList<IEvent>>(ids.Distinct().ToDictionary(id => id,
                id => _history.GetValueOrDefault(id) ?? Array.Empty<IEvent>()));
        }
        finally { _gate.Release(); }
    }
    /// <inheritdoc />
    public async Task<IReadOnlyDictionary<Guid, StreamSlice>> ReadBatchFromAsync(IReadOnlyList<StreamReadRequest> requests,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(requests); var batch = requests.ToArray();
        if (batch.Any(r => r is null) || batch.Select(r => r.StreamId).Distinct().Count() != batch.Length)
            throw new ArgumentException("Invalid or duplicate range read participant.", nameof(requests));
        var history = await ReadBatchAsync(batch.Select(r => r.StreamId).ToArray(), cancellationToken).ConfigureAwait(false);
        var result = new Dictionary<Guid, StreamSlice>();
        foreach (var request in batch)
        {
            var events = history[request.StreamId];
            if (request.AfterVersion > events.Count) throw new StreamConcurrencyException(request.AfterVersion, events.Count, request.StreamId);
            result.Add(request.StreamId, new(request.StreamId, request.AfterVersion, events.Count,
                events.Skip(checked((int)request.AfterVersion)), events.Take(checked((int)request.AfterVersion)).Select(e => e.Id)));
        }
        return new ReadOnlyDictionary<Guid, StreamSlice>(result);
    }
    /// <inheritdoc />
    public Task AppendAsync(Guid streamId, long expectedVersion, IReadOnlyList<IEvent> events,
        CancellationToken cancellationToken = default) => AppendBatchAsync(new[] { new StreamAppend(streamId, expectedVersion, events) }, cancellationToken);
    /// <inheritdoc />
    public async Task AppendBatchAsync(IReadOnlyList<StreamAppend> appends, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(appends); var batch = appends.ToArray();
        if (batch.Any(a => a is null) || batch.Select(a => a.StreamId).Distinct().Count() != batch.Length)
            throw new ArgumentException("Invalid or duplicate append participant.", nameof(appends));
        await _gate.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            ThrowIfDisposed(); cancellationToken.ThrowIfCancellationRequested();
            var next = new Dictionary<Guid, IReadOnlyList<IEvent>>(_history);
            foreach (var append in batch)
            {
                var history = _history.GetValueOrDefault(append.StreamId) ?? Array.Empty<IEvent>();
                if (history.Count != append.ExpectedVersion)
                    throw new StreamConcurrencyException(append.ExpectedVersion, history.Count, append.StreamId);
                var combined = history.Concat(append.Events).ToArray(); EventHistory.Validate(append.StreamId, combined);
                next[append.StreamId] = Array.AsReadOnly(combined);
            }
            ValidateEvidence(next);
            var streams = next.Select(pair => new StoredStream(pair.Key, pair.Value.Select(e =>
            { var (name, data) = _codec.Encode(e); return new StoredEvent(name, data); }).ToArray())).ToArray();
            var bytes = JsonSerializer.SerializeToUtf8Bytes(new StoredDocument(1, streams));
            var temporary = _path + ".stage-" + Guid.NewGuid().ToString("N");
            try
            {
                await using (var file = new FileStream(temporary, FileMode.CreateNew, FileAccess.Write, FileShare.None, 4096, FileOptions.Asynchronous))
                {
                    await file.WriteAsync(bytes, cancellationToken).ConfigureAwait(false);
                    await file.FlushAsync(cancellationToken).ConfigureAwait(false); file.Flush(flushToDisk: true);
                }
                _observe?.Invoke(JsonPublicationPoint.BeforeReplace);
                cancellationToken.ThrowIfCancellationRequested();
                File.Move(temporary, _path, overwrite: true);
                _history = next;
                _observe?.Invoke(JsonPublicationPoint.AfterReplace);
                // Cancellation after replacement does not retract publication or report a cancelled commit.
            }
            finally { if (File.Exists(temporary)) File.Delete(temporary); }
        }
        finally { _gate.Release(); }
    }
    private Dictionary<Guid, IReadOnlyList<IEvent>> Decode(string json)
    {
        var document = JsonSerializer.Deserialize<StoredDocument>(json) ?? throw new InvalidDataException("Null store document.");
        if (document.SchemaVersion != 1 || document.Streams is null) throw new InvalidDataException("Unsupported store format; explicit migration is required.");
        var result = new Dictionary<Guid, IReadOnlyList<IEvent>>();
        foreach (var stream in document.Streams)
        {
            if (stream is null || stream.Events is null) throw new InvalidDataException("Null stored stream.");
            var events = stream.Events.Select(e => e is null ? throw new InvalidDataException("Null stored event.") : _codec.Decode(e.Type, e.Payload)).ToArray();
            EventHistory.Validate(stream.Id, events);
            if (!result.TryAdd(stream.Id, Array.AsReadOnly(events))) throw new InvalidDataException("Duplicate stored stream.");
        }
        ValidateEvidence(result); return result;
    }
    private static void ValidateEvidence(Dictionary<Guid, IReadOnlyList<IEvent>> history)
    {
        foreach (var stream in history.Where(pair => pair.Value.Any(e => e is OperationJournalEvent)))
            OperationEvidence.Validate(stream.Key, stream.Value, history);
    }
    private static string CanonicalPath(string path)
    {
        // Resolve ancestor links too (for example macOS /var and /tmp), so directory aliases share a lease.
        for (var depth = 0; depth < 64; depth++)
        {
            var resolved = false;
            for (var directory = new DirectoryInfo(Path.GetDirectoryName(path)!); directory is not null; directory = directory.Parent)
            {
                if (directory.LinkTarget is null) continue;
                var target = directory.ResolveLinkTarget(returnFinalTarget: true)
                    ?? throw new IOException("Store directory link cannot be resolved.");
                path = Path.Combine(target.FullName, Path.GetRelativePath(directory.FullName, path));
                resolved = true; break;
            }
            if (!resolved) return Path.GetFullPath(path);
        }
        throw new IOException("Store directory has too many symbolic link aliases.");
    }
    private void ThrowIfDisposed() => ObjectDisposedException.ThrowIf(_disposed, this);
    /// <summary>Release exclusive process ownership after active operations finish. Old coordinators cannot use this session again.</summary>
    public void Dispose()
    {
        _gate.Wait();
        try { if (!_disposed) { _disposed = true; _lease.Dispose(); } }
        finally { _gate.Release(); }
    }
    private sealed record StoredDocument(int SchemaVersion, StoredStream[] Streams);
    private sealed record StoredStream(Guid Id, StoredEvent[] Events);
    private sealed record StoredEvent(string Type, JsonElement Payload);
}
