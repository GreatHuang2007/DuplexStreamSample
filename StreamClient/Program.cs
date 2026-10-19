using Grpc.Core;
using Grpc.Net.Client;
using Streamcalls;

namespace Streamcalls
{
    using StreamClient;

    public partial class StreamRequest : IStreamRequest;

    public partial class StreamResponse : IStreamResponse;
}

namespace StreamClient
{
    internal static class Program
    {
        public static void Main(string[] args)
        {
            using var channel = GrpcChannel.ForAddress("http://[::1]:50051");

            var stub = new StreamService.StreamServiceClient(channel);

            Console.Write("Auth token> ");

            var authToken = Console.ReadLine() ?? string.Empty;

            var streamingCall = stub.DuplexStream(new CallOptions(
                headers: new Metadata { { "authorization", authToken } }));

            var streamingDelegate = new DuplexStreamDelegator<StreamRequest, StreamResponse>(
                sendRequest: streamingCall.RequestStream.WriteAsync,
                listenResponse: streamingCall.ResponseStream.ReadAllAsync,
                disposer: () =>
                {
                    streamingCall.Dispose();
                    return ValueTask.CompletedTask;
                });

            GrpcDuplexStreamClient<StreamRequest, StreamResponse, Notification> client =
                new(streamingDelegate, n => n.Notification);

            client.OnServerNotification += n =>
            {
                Console.WriteLine($"Notification: {n.Content}");
                return Task.CompletedTask;
            };

            for (ulong i = 0;; i++)
            {
                Console.Write($"{i}> ");
                var cmd = Console.ReadLine() ?? string.Empty;
                var arg = cmd.Split(" ");
                switch (arg[0])
                {
                    case "e":
                        _ = client.CallAsync(new StreamRequest()
                            {
                                Echo = new EchoRequest
                                {
                                    Message = arg[1]
                                }
                            }, res => res.Echo.Message)
                            .ContinueWith(res =>
                                Console.WriteLine(res.Result));
                        break;
                    case "c":
                        for (int j = 1; j < arg.Length; j++)
                        {
                            var n = uint.Parse(arg[j]);
                            Console.WriteLine($"Sending compute id: {j}");
                            // 同时发送多个耗时请求，同时等待返回
                            var id = j;
                            _ = client.CallAsync(new StreamRequest()
                                {
                                    SlowCompute = new SlowComputeRequest()
                                    {
                                        N = n
                                    }
                                }, res => res.SlowCompute.Result)
                                .ContinueWith(res =>
                                    Console.WriteLine($"Compute {id} finished with {res.Result}"));
                        }

                        Thread.Sleep(100);

                        break;
                }
            }
        }
    }
}