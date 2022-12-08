using System.Net;
using System.Net.Http.Headers;
using System.Threading.Channels;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Polly;
using Polly.Retry;
using Microsoft.Health.Dicom.Client;
using FellowOakDicom;

namespace Dicom.Loader.Tool
{
    public class DicomUploader
    {
        private const string DicomManagedIdentityUrl = "https://dicom.healthcareapis.azure.com";

        private readonly Uri _dicomServerUrl;
        private readonly bool _needAuth;
        private readonly ServerAccessTokenProvider _accessTokenProvider;
        private readonly int _maxTaskCount;
        private readonly IHttpClientFactory _httpClientFactory;

        private readonly Random _randomGenerator;
        private readonly AsyncRetryPolicy<DicomWebResponse<DicomDataset>> _retryPolicy;
        private readonly ILogger<DicomUploader> _logger;

        public DicomUploader(
            ServerAccessTokenProvider tokenProvider,
            IHttpClientFactory httpClientFactory,
            IOptions<UploadConfiguration> config,
            ILogger<DicomUploader> logger)
        {
            _dicomServerUrl = new Uri(config.Value.DicomServerUrl);
            _maxTaskCount = config.Value.PutFhirConcurrency;
            _needAuth = config.Value.UseFhirAuthentication;
            _logger = logger;
            _httpClientFactory = httpClientFactory;

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
                        .OrResult<DicomWebResponse<DicomDataset>>(response => !response.IsSuccessStatusCode)
                        .WaitAndRetryAsync(pollyDelays, (result, timeSpan, retryCount, context) =>
                        {
                            string? error = result.Exception?.ToString();
                            _logger.LogWarning($"Request failed with {result?.Result?.StatusCode}: {error}. Waiting {timeSpan} before next retry. Retry attempt {retryCount}");
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
                    DicomFile dicomFile = resourceItem.DicomFile;
                    string accessToken = _needAuth ? _accessTokenProvider.GetAccessTokenAsync(DicomManagedIdentityUrl, cancellationToken) : string.Empty;

                    DicomWebResponse uploadResult;
                    try
                    {
                        uploadResult = await _retryPolicy
                            .ExecuteAsync(() =>
                            {
                                var httpClient = _httpClientFactory.CreateClient();
                                httpClient.BaseAddress = _dicomServerUrl;
                                IDicomWebClient client = new DicomWebClient(httpClient);
                                client.HttpClient.DefaultRequestHeaders.Authorization = new AuthenticationHeaderValue("Bearer", accessToken);
                                return client.StoreAsync(new List<DicomFile>() { dicomFile });
                            });
                        if (!uploadResult.IsSuccessStatusCode)
                        {
                            string resultContent = await uploadResult.Content.ReadAsStringAsync(cancellationToken);
                            _logger.LogError($"{id} Unable to upload to server. Error code: {uploadResult.StatusCode}, File Path: {resourceItem.FilePath}");
                        }
                    }
                    catch (DicomWebException dicomWebException)
                    {
                        if (dicomWebException.StatusCode == HttpStatusCode.Conflict)
                        {
                            _logger.LogInformation($"Conflict! {id}, processed {processedCount} resources.");
                        }
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
