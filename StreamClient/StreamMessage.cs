namespace StreamClient;

public interface IStreamRequest
{
    ulong ReqId { get; set; }
}

public interface IStreamResponse
{
    ulong ReqId { get; }
}