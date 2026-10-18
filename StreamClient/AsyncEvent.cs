namespace StreamClient;

public delegate Task AsyncEventHandler();

public delegate Task AsyncEventHandler<in TArg>(TArg e);

public static class AsyncEvent
{
    public static async Task InvokeOneByOneAsync(this AsyncEventHandler? e)
    {
        if (e is null) return;

        foreach (var handler in e.GetInvocationList().Cast<AsyncEventHandler>())
            await handler();
    }

    public static async Task InvokeOneByOneAsync<TArg>(this AsyncEventHandler<TArg>? e, TArg arg)
    {
        if (e is null) return;

        foreach (var handler in e.GetInvocationList().Cast<AsyncEventHandler<TArg>>())
            await handler(arg);
    }
}