using FileServiceClientLibrary;

if (args.Length != 8)
{
    Usage();
    return;
}

var userId = int.Parse(args[0]);
var keyBytes = System.IO.File.ReadAllBytes(args[1]);
var hostName = args[2];
var port = ushort.Parse(args[3]);
var dbName = args[4];
var command = args[5];
var key1 = int.Parse(args[6]);
var key2 = int.Parse(args[7]);

var config = new FileServiceConfig(
    userId,
    keyBytes,
    hostName,
    port,
    1000,
    dbName
);
var service = new FileService(config);

switch (command)
{
    case "get": RunGetCommand(); break;
    case "get_last": RunGetLastCommand(); break;
    default: Usage(); break;
}

return;

void RunGetCommand()
{
    var response = service.Get(key1, key2);
    Console.WriteLine("Db version: {0}, number of files: {1}.", response.DbVersion, response.Data.Count);
    foreach (var kv in response.Data)
    {
        var fileName = kv.Key.ToString();
        Console.WriteLine("File {0} version {1}.", fileName, kv.Value.Version);
        System.IO.File.WriteAllBytes(fileName, kv.Value.Data);
    }
}

void RunGetLastCommand()
{
    var response = service.GetLast(key1, key2);
    Console.WriteLine("Db version: {0}, number of files: {1}.", response.DbVersion, response.Key == null ? 0 : 1);
    if (response.Key == null || response.Data == null) return;
    var fileName = ((int)response.Key).ToString();
    Console.WriteLine("File {0} version {1}.", fileName, response.Data.Version);
    System.IO.File.WriteAllBytes(fileName, response.Data.Data);
}

void Usage()
{
    Console.WriteLine(
        "Usage: FileServiceClient userId keyFileName hostName port db_name [get key1 key2][get_last key1 key2]");
}
