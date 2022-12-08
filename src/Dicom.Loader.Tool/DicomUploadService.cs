using System.Threading.Channels;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace Dicom.Loader.Tool
{
    public class DicomUploadService : IHostedService
    {
        private DicomDataReader _dicomDataReader;
        private DicomUploader _dicomUploader;
        private ServerAccessTokenProvider _tokenProvider;
        private ILogger<DicomUploadService> _logger;
        private readonly IHostApplicationLifetime _hostApplicationLifetime;

        public DicomUploadService(
            IHostApplicationLifetime hostApplicationLifetime,
            DicomDataReader dicomDataReader,
            DicomUploader dicomUploader,
            ServerAccessTokenProvider tokenProvider,
            ILogger<DicomUploadService> logger)
        {
            _hostApplicationLifetime = hostApplicationLifetime;
            _dicomDataReader = dicomDataReader;
            _dicomUploader = dicomUploader;
            _tokenProvider = tokenProvider;

            _logger = logger;
        }

        public async Task StartAsync(CancellationToken cancellationToken)
        {
            _tokenProvider.EnsureInitialized(cancellationToken);

            _logger.LogInformation("Start upload service.");

            var channel = Channel.CreateBounded<ResourceItem>(5000);
            var cancellationTokenSource = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
            var readTask = _dicomDataReader.ReadAsync(channel.Writer, cancellationTokenSource);
            await _dicomUploader.UploadAsync(channel.Reader, cancellationTokenSource);
            await readTask;

            _logger.LogInformation("Stop upload service.");

            _hostApplicationLifetime.StopApplication();
        }

        public Task StopAsync(CancellationToken cancellationToken)
        {
            _logger.LogInformation("Service stopped.");
            return Task.CompletedTask;
        }

    }
}
