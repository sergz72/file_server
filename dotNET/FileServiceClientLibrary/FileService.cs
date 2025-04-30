using NetworkServiceClientLibrary;

namespace FileServiceClientLibrary;

internal enum RequestId {
    Get,
    Set,
    GetLast,
    FetFileVersion
}

public record File(int Version, byte[] Data);

public record GetResponse(int DbVersion, Dictionary<int, File> Data);

public record GetLastResponse(int DbVersion, int Key, File Data);

public record FileServiceConfig(int UserId, byte[] Key, string HostName, short Port, int TimeoutMs, string DbName);

public class FileService(FileServiceConfig config): NetworkService(new NetworkServiceConfig(
    BitConverter.GetBytes(config.UserId),
    config.Key,
    config.HostName,
    config.Port,
    config.TimeoutMs))
{
    private readonly string _dbName = config.DbName;

    public GetResponse Get(int key1, int key2)
    {
        var request = BuildGetRequest(RequestId.Get, key1, key2);
        var response = Send(request);
        return response[0] switch
        {
            //no error
            0 => DecodeGetResponse(response[1..]),
            // error
            _ => throw new ResponseError(response)
        };
    }

    private byte[] BuildGetRequest(RequestId requestId, int key1, int key2)
    {
        var bytes = System.Text.Encoding.UTF8.GetBytes(_dbName);
        using var stream = new MemoryStream();
        using var bw = new BinaryWriter(stream);
        bw.Write((byte)requestId);
        bw.Write((byte)bytes.Length);
        bw.Write(bytes);
        bw.Write(key1);
        bw.Write(key2);
        return stream.ToArray();
    }

    private static GetResponse DecodeGetResponse(byte[] bytes)
    {
        using var reader = new BinaryReader(new MemoryStream(bytes));
        var dbVersion = reader.ReadInt32();
        var length = reader.ReadInt32();
        var result = new Dictionary<int, File>();
        while (length-- > 0)
        {
            var fileVersion = reader.ReadInt32();
            var key = reader.ReadInt32();
            var valueLength = reader.ReadInt32();
            var value = reader.ReadBytes(valueLength);
            result[key] = new File(fileVersion, value);
        }

        if (reader.BaseStream.Position != reader.BaseStream.Length)
            throw new IOException("Incorrect response length");
        
        return new GetResponse(dbVersion, result);
    }
}