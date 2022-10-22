using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Channels;
using System.Threading.Tasks;
using FhirLoader.QuwanLoader;
using Microsoft.ApplicationInsights;
using Microsoft.ApplicationInsights.DataContracts;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace QuwanLoader
{
    public class FhirUploadService : IHostedService
    {
        private BlobStreamReader _blobReader;
        private FhirUploader _fhirUploader;
        private FhirAccessTokenProvider _tokenProvider;
        private ILogger<FhirUploadService> _logger;
        private TelemetryClient _telemetryClient;
        private readonly IHostApplicationLifetime _hostApplicationLifetime;

        public FhirUploadService(
            IHostApplicationLifetime hostApplicationLifetime,
            BlobStreamReader blobReader,
            FhirUploader fhirUploader,
            FhirAccessTokenProvider tokenProvider,
            TelemetryClient tc,
            ILogger<FhirUploadService> logger)
        {
            _hostApplicationLifetime = hostApplicationLifetime;
            _blobReader = blobReader;
            _fhirUploader = fhirUploader;
            _tokenProvider = tokenProvider;

            _telemetryClient = tc;
            _logger = logger;
        }

        public async Task StartAsync(CancellationToken cancellationToken)
        {
            _tokenProvider.EnsureInitialized(cancellationToken);

            using (_telemetryClient.StartOperation<RequestTelemetry>("UploadFhir"))
            {
                _logger.LogInformation("Start upload service.");

                var channel = Channel.CreateBounded<ResourceItem>(200000);
                var cancellationTokenSource = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
                var readTask = _blobReader.ReadAsync(channel.Writer, cancellationTokenSource);
                await _fhirUploader.UploadAsync(channel.Reader, cancellationTokenSource.Token);
                await readTask;

                _logger.LogInformation("Stop upload service.");
            }

            _hostApplicationLifetime.StopApplication();
        }

        public Task StopAsync(CancellationToken cancellationToken)
        {
            _logger.LogInformation("Service stopped.");
            return Task.CompletedTask;
        }

    }
}
