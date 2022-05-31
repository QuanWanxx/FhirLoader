using Newtonsoft.Json.Linq;
using Polly;
using System;
using System.Collections.Generic;
using System.Net.Http;
using System.Net.Http.Headers;
using System.Text;
using System.Threading.Tasks;

namespace FhirLoader.QuwanLoader
{
    public class MyUploader
    {
        public readonly Uri FhirServerUrl;
        public readonly string AccessToken;
        public readonly int MaxTaskCount;

        private readonly Random _randomGenerator;
        private readonly HttpClient _httpClient;

        public MyUploader(string fhirServerUrl, string accessToken = null, int maxTaskCount = 30)
        {
            FhirServerUrl = new Uri(fhirServerUrl);
            AccessToken = accessToken;
            MaxTaskCount = maxTaskCount;

            _randomGenerator = new Random();
            _httpClient = new HttpClient();
        }

        public async Task UploadAsync(IEnumerable<string> resources)
        {
            int processCount = 0;

            var tasks = new List<Task>();
            foreach (var resourceString in resources)
            {
                if (tasks.Count >= MaxTaskCount)
                {
                    var finishedTask = await Task.WhenAny(tasks);
                    if (finishedTask.IsFaulted)
                    {
                       Console.WriteLine("Process task failed: " + finishedTask.Exception.Message);
                        throw new Exception("Task execution failed", finishedTask.Exception);
                    }
                    
                    tasks.Remove(finishedTask);
                }

                var resourceContent = JObject.Parse(resourceString);
                string id = (string)resourceContent["id"];
                string resourceType = (string)resourceContent["resourceType"];
                // Create post job

                tasks.Add(Task.Run(async () => await UploadInternalAsync(resourceString, resourceType, id)));

                if (processCount % 100 == 0)
                {
                    Console.WriteLine($"{DateTime.Now} Start processing {processCount} resource for '{resourceType}'");
                }
                processCount += 1;
            }

            try
            {
                await Task.WhenAll(tasks);
            }
            catch (Exception ex)
            {
                Console.WriteLine("Process task failed: " + ex.Message);
            }
        }

        private async Task UploadInternalAsync(string resourceString, string resourceType, string id)
        {
            StringContent content = new StringContent(resourceString, Encoding.UTF8, "application/json");
            var pollyDelays =new[]
                {
                    TimeSpan.FromMilliseconds(2000 + _randomGenerator.Next(50)),
                    TimeSpan.FromMilliseconds(5000 + _randomGenerator.Next(50)),
                    TimeSpan.FromMilliseconds(8000 + _randomGenerator.Next(50)),
                    TimeSpan.FromMilliseconds(12000 + _randomGenerator.Next(50)),
                    TimeSpan.FromMilliseconds(16000 + _randomGenerator.Next(50)),
                };

            HttpResponseMessage uploadResult = await Policy
                    .HandleResult<HttpResponseMessage>(response => !response.IsSuccessStatusCode)
                    .WaitAndRetryAsync(pollyDelays, (result, timeSpan, retryCount, context) =>
                    {
                        if (retryCount > 2)
                        {
                            Console.WriteLine($"Request failed with {result.Result.StatusCode}. Waiting {timeSpan} before next retry. Retry attempt {retryCount}");
                        }
                    })
                    .ExecuteAsync(() =>
                    {
                        var message = new HttpRequestMessage(HttpMethod.Put, new Uri(FhirServerUrl, $"/{resourceType}/{id}"));

                        message.Content = content;
                        if (!string.IsNullOrEmpty(AccessToken))
                        {
                            message.Headers.Authorization = new AuthenticationHeaderValue("Bearer", AccessToken);
                        }

                        return _httpClient.SendAsync(message);
                    });

            if (!uploadResult.IsSuccessStatusCode)
            {
                string resultContent = await uploadResult.Content.ReadAsStringAsync();
                throw new Exception($"Unable to upload to server. Error code: {uploadResult.StatusCode}, resource content: {resourceString}");
            }
        }
    }
}
