using System.Diagnostics;
using System.Net.Http.Headers;
using System.Text.Json;
using Google.Apis.Auth.OAuth2;
using Google.Cloud.Storage.V1;
using StreamLargeFile;

class Program
{
    static async Task Main()
    {
        Stopwatch sw = Stopwatch.StartNew();

        string bucketName = "demo-large-file";
        string objectName = "people.jsonl";

        var credential = GoogleCredential.FromFile("./demos-470820-2d5792fb48ee.json");
        var storage = StorageClient.Create(credential);

        // foi chumbado o valor no código para simplificar, mas esse valor deve ser recuperado de alguma base para a aplicação ficar resiliente,
        // e em caso de falhas, ela continuar o processamento de onde parou.
        long position = 0; 

        const int chunkSize = 200 * 1024 * 1024; // 200 MB

        var jsonSerializerOptions = new JsonSerializerOptions { PropertyNameCaseInsensitive = true };
        var incompleteLine = "";
        long batch = 0;

        var obj = await storage.GetObjectAsync(bucketName, objectName);
        long objectSize = (long)(obj.Size ?? 0);


        while (position < objectSize)
        {
            using var stream = new MemoryStream();
            long end = Math.Min(position + chunkSize - 1, objectSize - 1);

            var options = new DownloadObjectOptions()
            {
                Range = new RangeHeaderValue(position, end)
            };

            await storage.DownloadObjectAsync(bucketName, objectName, stream, options);

            if (stream.Length is 0)
                break;

            stream.Position = 0;

            using var reader = new StreamReader(stream);
            string? line;
            long countLine = 0;
            while ((line = await reader.ReadLineAsync()) != null)
            {
                if(string.IsNullOrWhiteSpace(incompleteLine) is false) 
                {
                    incompleteLine += line;
                    ProcessLine(incompleteLine, jsonSerializerOptions);
                    incompleteLine = string.Empty;
                    continue;
                }

                if(isCompleteLine(line) is false)
                {
                    incompleteLine += line;
                    continue;
                }

                ProcessLine(line, jsonSerializerOptions);
                countLine++;
            }
            position += stream.Length;
            SavePositionProcessed(position);

            Console.WriteLine($"process {batch} batch with lines {countLine}");
            batch++;
        }

        sw.Stop();
        Console.WriteLine($"Tempo total: {sw.Elapsed.TotalSeconds:F2} s");
    }

    static void SavePositionProcessed(long position) =>
        Console.WriteLine($"Metodo para exemplificar o armazenamento da quantidade ({position}) de bytes processados.");

    static void ProcessLine(string content, JsonSerializerOptions jsonSerializerOptions)
    {
        try
        {
            var person = JsonSerializer.Deserialize<Person>(content, jsonSerializerOptions);
        }
        catch (Exception ex)
        {
            Console.WriteLine($"Erro ao processar linha: {ex.Message}");
        }
    }

    static bool isCompleteLine(string line) 
    {
        try
        {
            if(line.EndsWith('}') is false)
                return false;

            JsonDocument.Parse(line);
            return true;
        }
        catch
        {
            return false;
        }
    }
}