using System;
using System.IO;
using System.Linq;
using FhirLoader.QuwanLoader;
using Newtonsoft.Json;

namespace FhirLoader
{
    class Program
    {
        static void Main()
        {
            var config = JsonConvert.DeserializeObject<Configuration>(File.ReadAllText("config.json"));

            var uploader = new MyUploader(config.FhirServerUrl, config.AccessToken);

            var filePaths = Directory.GetFiles(config.DirectoryPath)
                    .Where(filePath => filePath.EndsWith(".ndjson", StringComparison.CurrentCultureIgnoreCase));

            foreach (string filePath in filePaths)
            {
                Console.WriteLine($"Process '{filePath}'");
                uploader.UploadAsync(File.ReadLines(filePath)).Wait();

                Console.WriteLine($"Finish upload resources for '{filePath}'");
            }
        }
    }
}
