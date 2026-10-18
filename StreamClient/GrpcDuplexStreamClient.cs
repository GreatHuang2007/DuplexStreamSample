namespace StreamClient;

public class GrpcDuplexStreamClient<TReq, TRes> : IAsyncDisposable
    where TReq : IStreamRequest
    where TRes : IStreamResponse
{
    private readonly DuplexStreamDelegator<TReq, TRes> _streamingCall;

    private bool _disposed;

    public async ValueTask DisposeAsync()
    {
        if (_disposed) return;

        await _cts.CancelAsync();
        _cts.Dispose();

        await _streamingCall.DisposeAsync();

        lock (_idLock)
        {
            var cancellationException = new OperationCanceledException("Stream client has been disposed.");
            foreach (var (_, taskWrapper) in _pendingTasks)
            {
                taskWrapper.SetException(cancellationException);
            }

            _pendingTasks.Clear();
        }

        _disposed = true;
    }

    public GrpcDuplexStreamClient(DuplexStreamDelegator<TReq, TRes> streamingCall)
    {
        _streamingCall = streamingCall;
        _ = ListenResponse(_cts.Token);
    }

    private ulong _nextReqId = 0;
    private readonly object _idLock = new();
    private readonly Dictionary<ulong, ITaskWrapper> _pendingTasks = new();

    private ITaskWrapper? PopPendingTask(ulong reqId)
    {
        lock (_idLock)
        {
            _pendingTasks.Remove(reqId, out var wrapperObj);

            return wrapperObj;
        }
    }

    private interface ITaskWrapper
    {
        void SetResult(TRes response);
        void SetException(Exception ex);
    }

    private class TaskWrapper<TResult>(TaskCompletionSource<TResult> tcs, Func<TRes, TResult> valueGetter) : ITaskWrapper
    {
        public void SetResult(TRes response)
        {
            try
            {
                var result = valueGetter(response); // TODO 验证对应字段是否为 null
                tcs.TrySetResult(result);
            }
            catch (Exception ex)
            {
                tcs.TrySetException(ex);
            }
        }

        public void SetException(Exception ex)
        {
            tcs.TrySetException(ex);
        }
    }

    private readonly CancellationTokenSource _cts = new();

    protected virtual ValueTask HandleResponse(TRes response)
    {
        ulong reqId = response.ReqId;
        PopPendingTask(reqId)?.SetResult(response);
        return ValueTask.CompletedTask;
    }

    private async Task ListenResponse(CancellationToken ctx)
    {
        try
        {
            await foreach (var response in _streamingCall.ListenResponseAsync(ctx))
            {
                await HandleResponse(response);
            }
        }
        catch (Exception ex)
        {
            // 出现异常，将所有挂起的任务标记为故障
            lock (_idLock)
                foreach (var (_, wrapper) in _pendingTasks)
                {
                    wrapper.SetException(ex);
                }

            _pendingTasks.Clear();
        }
    }

    public Task<TResult> CallAsync<TResult>(TReq req, Func<TRes, TResult> valueGetter)
    {
        Task<TResult> resultTask;
        ulong reqId;

        lock (_idLock)
        {
            unchecked
            {
                reqId = _nextReqId++;
            }

            req.ReqId = reqId;

            var tcs = new TaskCompletionSource<TResult>();
            var wrapper = new TaskWrapper<TResult>(tcs, valueGetter);
            _pendingTasks[reqId] = wrapper;

            resultTask = tcs.Task;
        }

        async Task SendReqAsync()
        {
            try
            {
                await _streamingCall.SendRequestAsync(req, _cts.Token);
            }
            catch (Exception ex)
            {
                PopPendingTask(reqId)?.SetException(ex);
            }
        }

        _ = SendReqAsync();

        return resultTask;
    }
}

public class GrpcDuplexStreamClient<TReq, TRes, TNotify>(
    DuplexStreamDelegator<TReq, TRes> streamingCall,
    Func<TRes, TNotify?> notifyGetter)
    : GrpcDuplexStreamClient<TReq, TRes>(streamingCall)
    where TReq : IStreamRequest
    where TRes : IStreamResponse
    where TNotify : notnull
{
    private Task _lastNotificationTask = Task.CompletedTask;
    private readonly object _notifyLock = new();

    protected override async ValueTask HandleResponse(TRes response)
    {
        var notification = notifyGetter(response);
        if (notification is not null)
        {
            lock (_notifyLock)
            {
                // TODO 处理异常
                _lastNotificationTask = _lastNotificationTask.ContinueWith(_ =>
                    OnServerNotification.InvokeOneByOneAsync(notification));
                ;
            }

            return;
        }

        await base.HandleResponse(response);
    }

    public event AsyncEventHandler<TNotify>? OnServerNotification;
}