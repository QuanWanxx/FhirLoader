using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Channels;
using System.Threading.Tasks;
using FhirLoader.QuwanLoader;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace QuwanLoader
{
    public class FhirUploadService : IHostedService
    {
        private BlobStreamReader _blobReader;
        private FhirUploader _fhirUploader;
        private ILogger<FhirUploadService> _logger;

        public FhirUploadService(
            BlobStreamReader blobReader,
            FhirUploader fhirUploader,
            ILogger<FhirUploadService> logger)
        {
            _blobReader = blobReader;
            _fhirUploader = fhirUploader;
            _logger = logger;
        }

        public async Task StartAsync(CancellationToken cancellationToken)
        {
            var channel = Channel.CreateBounded<ResourceItem>(200000);
            var readTask = _blobReader.ReadAsync(channel.Writer);
            await _fhirUploader.UploadAsync(channel.Reader);

            await readTask;
        }

        public Task StopAsync(CancellationToken cancellationToken)
        {
            _logger.LogInformation("Service stopped.");
            return Task.CompletedTask;
        }

    }
}
