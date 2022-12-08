using System;
using System.IO;
using System.Threading.Tasks;
using Azure.Identity;
using Azure.Storage.Blobs;

namespace BlobListGenerator
{
    public class Program
    {
        public static async Task Main()
        {
            string containerUri = "https://prefdatastore.blob.core.windows.net/synthea";
            string folderPath = "50g";
            string outputFileName = $"../../../data/fhir{folderPath}_bloblist.txt";
            var blobContainerClient = new BlobContainerClient(new Uri(containerUri), new DefaultAzureCredential());
            var pages = blobContainerClient.GetBlobsAsync(prefix: folderPath);
            using var filestream = File.OpenWrite(outputFileName);
            using var streamWriter = new StreamWriter(filestream);
            await foreach(var page in pages.AsPages())
            {
                foreach (var item in page.Values)
                {
                    if (item.Name.EndsWith("ndjson"))
                    {
                        streamWriter.WriteLine($"{containerUri}/{item.Name}");
                    }
                }
            }
        }
    }
    
}