using System.Net;
using System.Net.Sockets;
using System.Security.Cryptography;

namespace NetworkServiceClientLibrary;

public record NetworkServiceConfig(
    byte[] Prefix,
    byte[] Key,
    string HostName,
    short Port,
    int TimeoutMs = 1000);

public class NetworkService
{
    private readonly UdpClient _udpClient;
    private readonly NetworkServiceConfig _config;
    
    public NetworkService(NetworkServiceConfig config)
    {
        _config = config;
        _udpClient = new UdpClient(config.HostName, config.Port);
        _udpClient.Client.ReceiveTimeout = config.TimeoutMs;
    }

    public byte[] Send(byte[] request)
    {
        using var stream = new MemoryStream();
        using var bw = new BinaryWriter(stream);
        bw.Write(_config.Prefix);
        bw.Write(Encrypt(request));
        var data = stream.ToArray();
        var response = SendUdp(data);
        return Decrypt(response);
    }
    
    private byte[] SendUdp(byte[] data)
    {
        _udpClient.Send(data, data.Length);
        var ep = new IPEndPoint(IPAddress.Any, 0);
        return _udpClient.Receive(ref ep);
    }

    private byte[] Encrypt(byte[] data)
    {
        var iv = BuildIv();
        var transformed = TransformIv(iv);
        var cipher = new ChaCha20(_config.Key, iv);
        var encrypted = cipher.Encrypt(data);
        using var stream = new MemoryStream();
        using var bw = new BinaryWriter(stream);
        bw.Write(transformed);
        bw.Write(encrypted);
        return stream.ToArray();
    }

    private byte[] Decrypt(byte[] data)
    {
        if (data.Length < 13)
            throw new IOException("Invalid response.");
        var iv = TransformIv(data[..12]);
        var cipher = new ChaCha20(_config.Key, iv);
        return cipher.Encrypt(data[12..]);
    }
    
    private byte[] TransformIv(byte[] iv)
    {
        using var stream = new MemoryStream();
        using var bw = new BinaryWriter(stream);
        bw.Write(iv[..4]);
        bw.Write(iv[..4]);
        bw.Write(iv[..4]);
        var iv3 = stream.ToArray();
        var cipher = new ChaCha20(_config.Key, iv3);
        var transformed = cipher.Encrypt(iv[4..]);
        Array.Copy(transformed, 0, iv3, 4, 8);
        return iv3;
    }
    
    private static byte[] BuildIv()
    {
        using var stream = new MemoryStream();
        using var bw = new BinaryWriter(stream);
        bw.Write(RandomNumberGenerator.GetBytes(4));
        bw.Write(((DateTimeOffset)DateTime.UtcNow).ToUnixTimeSeconds());
        return stream.ToArray();
    }
}

public class ResponseError(byte[] decompressed): IOException(BuildMessage(decompressed))
{
    private static string BuildMessage(byte[] decompressed)
    {
        if (decompressed[0] == 2)
        {
            var message = System.Text.Encoding.UTF8.GetString(decompressed[1..]);
            return $"Error: {message}";
        }

        return $"Wrong response type {decompressed[0]}";
    }
}