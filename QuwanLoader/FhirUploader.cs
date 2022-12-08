using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net.Http;
using System.Net.Http.Headers;
using System.Runtime.CompilerServices;
using System.Text;
using System.Threading;
using System.Threading.Channels;
using System.Threading.Tasks;
using Azure.Core;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Microsoft.Identity.Client;
using Newtonsoft.Json.Linq;
using Polly;
using Polly.Retry;
using QuwanLoader;
using System.Timers;
using Timer = System.Timers.Timer;

namespace FhirLoader.QuwanLoader
{
    public class FhirUploader
    {
        private readonly Uri _fhirServerUrl;
        private readonly bool _needAuth;
        private readonly FhirAccessTokenProvider _accessTokenProvider;
        private readonly int _maxTaskCount;
        private readonly IHttpClientFactory _httpClientFactory;
        private readonly HttpClient _httpClient;

        private readonly Random _randomGenerator;
        private readonly AsyncRetryPolicy<HttpResponseMessage> _retryPolicy;
        private readonly ILogger<FhirUploader> _logger;

        public FhirUploader(
            FhirAccessTokenProvider tokenProvider,
            IHttpClientFactory httpClientFactory,
            IOptions<UploadConfiguration> config,
            ILogger<FhirUploader> logger)
        {
            _httpClientFactory = httpClientFactory;
            _fhirServerUrl = new Uri(config.Value.FhirServerUrl);
            _maxTaskCount = config.Value.PutFhirConcurrency;
            _needAuth = config.Value.UseFhirAuthentication;
            _logger = logger;

            _httpClient = _httpClientFactory.CreateClient();
            _randomGenerator = new Random();
            _accessTokenProvider = tokenProvider;

            var pollyDelays = new[]
            {
                    TimeSpan.FromMilliseconds(2000 + _randomGenerator.Next(50)),
                    TimeSpan.FromMilliseconds(5000 + _randomGenerator.Next(50)),
                    TimeSpan.FromMilliseconds(8000 + _randomGenerator.Next(50)),
                    TimeSpan.FromMilliseconds(12000 + _randomGenerator.Next(50)),
                    TimeSpan.FromMilliseconds(16000 + _randomGenerator.Next(50)),
            };
            _retryPolicy = Policy
                        .Handle<HttpRequestException>()
                        .Or<TaskCanceledException>()
                        .OrResult<HttpResponseMessage>(response => !response.IsSuccessStatusCode)
                        .WaitAndRetryAsync(pollyDelays, (result, timeSpan, retryCount, context) =>
                        {
                            string error = result.Exception?.ToString();
                            _logger.LogWarning($"Request failed with {result?.Result?.StatusCode}. {result.Result?.RequestMessage?.RequestUri}: {error}. Waiting {timeSpan} before next retry. Retry attempt {retryCount}");
                        });
        }

        public async Task UploadAsync(ChannelReader<ResourceItem> channelReader, CancellationTokenSource cancellationTokenSource)
        {
            var tasks = new List<Task<Tuple<string, int>>>();
            for(int i = 0; i < _maxTaskCount; i ++)
            {
                string workerId = $"Worker {i}";
                tasks.Add(Task.Run(() => UploadInternalAsync(workerId, channelReader, cancellationTokenSource.Token)));
            }
            _logger.LogInformation($"Initialized {tasks.Count()} FHIR uploaders.");

            var workerCounts = new Dictionary<string, int>();

            while (tasks.Any())
            {
                var completed = await Task.WhenAny(tasks);
                try
                {
                    var result = await completed;
                    workerCounts[result.Item1] = result.Item2;
                    tasks.Remove(completed);
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, $"upload failed. {ex}");
                    cancellationTokenSource.Cancel();
                    return;
                }
            }

            foreach(var item in workerCounts)
            {
                _logger.LogInformation($"Process summary >> {item.Key}: {item.Value}");
            }

            _logger.LogInformation($"Upload finished: {workerCounts.Values.Sum()} resources loaded.");
        }

        private async Task<Tuple<string, int>> UploadInternalAsync(string id, ChannelReader<ResourceItem> channelReader, CancellationToken cancellationToken = default)
        {
            int processedCount = 0;
            DateTime current = DateTime.Now;

            try
            {
                while (await channelReader.WaitToReadAsync(cancellationToken))
                {
                    bool shouldHeartBeat = false;
                    if (current.AddMinutes(2) < DateTime.Now)
                    {
                        current = DateTime.Now;
                        _logger.LogInformation($"{current} {id}, starts to read.");

                        shouldHeartBeat = true;
                    }

                    var resourceItem = await channelReader.ReadAsync(cancellationToken);
                    StringContent content = new StringContent(resourceItem.Resource, Encoding.UTF8, "application/json");

                    string accessToken = _needAuth ? _accessTokenProvider.GetAccessTokenAsync(_fhirServerUrl.AbsoluteUri, cancellationToken) : string.Empty;
                    var message = new HttpRequestMessage(HttpMethod.Put, new Uri(_fhirServerUrl, $"/{resourceItem.ResourceType}/{resourceItem.Id}"));

                    message.Content = content;
                    if (_needAuth)
                    {
                        message.Headers.Authorization = new AuthenticationHeaderValue("Bearer", accessToken);
                    }

                    HttpResponseMessage uploadResult = await _retryPolicy
                            .ExecuteAsync(() =>
                            {
                                return _httpClient.SendAsync(message.Clone(), cancellationToken);
                            });

                    if (!uploadResult.IsSuccessStatusCode)
                    {
                        string resultContent = await uploadResult.Content.ReadAsStringAsync(cancellationToken);
                        _logger.LogError($"{id} Unable to upload to server. Error code: {uploadResult.StatusCode}, resource content: {resourceItem.Resource}");
                    }

                    processedCount++;

                    if (processedCount % 10000 == 0)
                    {
                        _logger.LogInformation($"{id}, processed {processedCount} resources.");
                    }

                    if (shouldHeartBeat)
                    {
                        _logger.LogInformation($"{current} {id}, waits to read next.");
                    }
                }

            }
            catch (Exception ex)
            {
                _logger.LogError($"{id} execution failed! {ex}");
                throw;
            }

            _logger.LogInformation($"{id} completed with {processedCount}");

            return Tuple.Create(id, processedCount);
        }
    }
}
