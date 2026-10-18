using System.Runtime.CompilerServices;
using System.Threading.Channels;

namespace StreamClient;

public sealed class DuplexStreamDelegator<TReq, TRes> : IAsyncDisposable
{
    private readonly Channel<TReq> _requestChannel;
    private readonly Channel<TRes> _responseChannel;

    private readonly Func<TReq, CancellationToken, Task> _sendRequest;
    private readonly Func<CancellationToken, IAsyncEnumerable<TRes>> _listenResponse;
    private readonly Func<ValueTask> _disposer;

    private readonly CancellationTokenSource _cts = new();
    private readonly Task _sendLoop;
    private readonly Task _receiveLoop;

    /// <summary>
    /// 创建一个双向流委托
    /// </summary>
    /// <param name="sendRequest">对接到原来的流对像的发送方法</param>
    /// <param name="listenResponse">对接原来的流对像的接收方法</param>
    /// <param name="disposer">如果你认为这个委托就是你的双向流服务的“化身”，那么你应该让这个委托来释放你的服务本体</param>
    public DuplexStreamDelegator(
        Func<TReq, CancellationToken, Task> sendRequest,
        Func<CancellationToken, IAsyncEnumerable<TRes>> listenResponse,
        Func<ValueTask> disposer)
    {
        _sendRequest = sendRequest;
        _listenResponse = listenResponse;
        _disposer = disposer;

        _requestChannel = Channel.CreateUnbounded<TReq>(new UnboundedChannelOptions()
        {
            SingleReader = true,
            SingleWriter = false,
            AllowSynchronousContinuations = false
        });

        _responseChannel = Channel.CreateUnbounded<TRes>(new UnboundedChannelOptions()
        {
            SingleReader = true,
            SingleWriter = true,
            AllowSynchronousContinuations = false
        });

        _sendLoop = RunSendLoopAsync();
        _receiveLoop = RunReceiveLoopAsync();
    }

    public async Task SendRequestAsync(TReq request, CancellationToken cancellationToken = default)
    {
        await _requestChannel.Writer.WriteAsync(request, cancellationToken)
            .ConfigureAwait(false);
    }

    public async IAsyncEnumerable<TRes> ListenResponseAsync(
        [EnumeratorCancellation] CancellationToken ctx = default)
    {
        await foreach (var item in _responseChannel.Reader.ReadAllAsync(ctx)
                           .ConfigureAwait(false))
        {
            yield return item;
        }
    }

    private async Task RunSendLoopAsync()
    {
        try
        {
            await foreach (var request in _requestChannel.Reader
                               .ReadAllAsync(_cts.Token)
                               .ConfigureAwait(false))
            {
                await _sendRequest(request, _cts.Token).ConfigureAwait(false);
            }
        }
        catch (OperationCanceledException oce) when (oce.CancellationToken == _cts.Token)
        {
            // 正常关闭
        }
        catch (Exception ex)
        {
            _requestChannel.Writer.TryComplete(ex);
            _responseChannel.Writer.TryComplete(ex);
        }
    }

    private async Task RunReceiveLoopAsync()
    {
        try
        {
            await foreach (var response in _listenResponse(_cts.Token)
                               .ConfigureAwait(false))
            {
                await _responseChannel.Writer.WriteAsync(response, _cts.Token)
                    .ConfigureAwait(false);
            }

            _responseChannel.Writer.TryComplete();
        }
        catch (OperationCanceledException oce) when (oce.CancellationToken == _cts.Token)
        {
            // 正常关闭
        }
        catch (Exception ex)
        {
            _requestChannel.Writer.TryComplete(ex);
            _responseChannel.Writer.TryComplete(ex);
        }
    }

    private Task? _disposeTask;
    private readonly object _disposeLock = new();

    public ValueTask DisposeAsync()
    {
        lock (_disposeLock)
        {
            _disposeTask ??= Dispose();
            return new ValueTask(_disposeTask);
        }

        async Task Dispose()
        {
            await _cts.CancelAsync();

            try
            {
                _requestChannel.Writer.TryComplete();
                _responseChannel.Writer.TryComplete();
            }
            catch
            {
                // 关闭阶段忽略异常
                // TODO log
            }

            try
            {
                await Task.WhenAll(_sendLoop, _receiveLoop).ConfigureAwait(false);
            }
            catch
            {
                //
            }

            try
            {
                await _disposer.Invoke();
            }
            catch
            {
                //
            }

            _cts.Dispose();
        }
    }
}