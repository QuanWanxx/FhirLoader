using System;
using System.IO;
using System.Net.Http;
using System.Net.Http.Headers;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;
using Microsoft.IdentityModel.Clients.ActiveDirectory;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using Polly;

namespace FhirLoader
{
    class Program
    {

        static void Main(
            string fhirServer = "https://quwanossfhirserver0527.azurewebsites.net",
            string accessToken = null,
            string folderPath = @"C:\quwan\FHIR-examples\synathea1G\Small",
            int maxDegreeOfParallelism = 30,
            int refreshInterval = 20)
        {
            Uri fhirServerUrl = new Uri(fhirServer);
            string[] filePaths = Directory.GetFiles(folderPath);
            foreach (string filePath in filePaths)
            {
                Console.WriteLine($"Process {filePath}");
                Upload(fhirServerUrl, accessToken, filePath, maxDegreeOfParallelism, refreshInterval);
            }
        }

        static void Upload(
            Uri fhirServerUrl,
            string accessToken,
            string bufferFileName,
            int maxDegreeOfParallelism,
            int refreshInterval)
        {
            HttpClient httpClient = new HttpClient();
            MetricsCollector metrics = new MetricsCollector();

            var randomGenerator = new Random();

            var actionBlock = new ActionBlock<string>(async resourceString =>
            {
                var resource = JObject.Parse(resourceString);
                string resource_type = (string)resource["resourceType"];
                string id = (string)resource["id"];

                Thread.Sleep(TimeSpan.FromMilliseconds(randomGenerator.Next(50)));

                StringContent content = new StringContent(resourceString, Encoding.UTF8, "application/json");
                var pollyDelays =
                        new[]
                        {
                                TimeSpan.FromMilliseconds(2000 + randomGenerator.Next(50)),
                                TimeSpan.FromMilliseconds(3000 + randomGenerator.Next(50)),
                                TimeSpan.FromMilliseconds(5000 + randomGenerator.Next(50)),
                                TimeSpan.FromMilliseconds(8000 + randomGenerator.Next(50)),
                                TimeSpan.FromMilliseconds(12000 + randomGenerator.Next(50)),
                                TimeSpan.FromMilliseconds(16000 + randomGenerator.Next(50)),
                        };

                HttpResponseMessage uploadResult = await Policy
                    .HandleResult<HttpResponseMessage>(response => !response.IsSuccessStatusCode)
                    .WaitAndRetryAsync(pollyDelays, (result, timeSpan, retryCount, context) =>
                    {
                        if (retryCount > 3)
                        {
                            Console.WriteLine($"Request failed with {result.Result.StatusCode}. Waiting {timeSpan} before next retry. Retry attempt {retryCount}");
                        }
                    })
                    .ExecuteAsync(() =>
                    {
                        var message = new HttpRequestMessage(HttpMethod.Put, new Uri(fhirServerUrl, $"/{resource_type}/{id}"));

                        message.Content = content;
                        if (!string.IsNullOrEmpty(accessToken))
                        {
                            message.Headers.Authorization = new AuthenticationHeaderValue("Bearer", accessToken);
                        }

                        return httpClient.SendAsync(message);
                    });

                if (!uploadResult.IsSuccessStatusCode)
                {
                    string resultContent = await uploadResult.Content.ReadAsStringAsync();
                    Console.WriteLine(resultContent);
                    // throw new Exception($"Unable to upload to server. Error code {uploadResult.StatusCode}");
                }

                metrics.Collect(DateTime.Now);
            },
                new ExecutionDataflowBlockOptions
                {
                    MaxDegreeOfParallelism = maxDegreeOfParallelism
                }
            );

            // Start output on timer
            var t = new Task(() => {
                while (true)
                {
                    Thread.Sleep(1000 * refreshInterval);
                    Console.WriteLine($"Resources per second: {metrics.EventsPerSecond}");
                }
            });
            t.Start();

            // Read the ndjson file and feed it to the threads
            System.IO.StreamReader buffer = new System.IO.StreamReader(bufferFileName);
            string line;
            while ((line = buffer.ReadLine()) != null)
            {
                actionBlock.Post(line);
            }

            actionBlock.Complete();
            actionBlock.Completion.Wait();
        }
    }
}
