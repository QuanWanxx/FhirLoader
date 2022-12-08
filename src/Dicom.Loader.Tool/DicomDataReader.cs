using System.Threading.Channels;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace Dicom.Loader.Tool
{
    public class DicomDataReader
    {
        private readonly string _directoryPath;
        private readonly int _repeatCount;
        private readonly int _readBlobConcurrency;
        private readonly ILogger<DicomDataReader> _logger;

        public DicomDataReader(
            IOptions<UploadConfiguration> config,
            ILogger<DicomDataReader> logger
            )
        {
            _directoryPath = config.Value.DirectoryPath;
            _readBlobConcurrency = config.Value.ReadBlobConcurrency;
            _repeatCount = config.Value.RepeatCount;
            _logger = logger;
        }
        public async Task ReadAsync(ChannelWriter<ResourceItem> writer, CancellationTokenSource cancellationTokenSource)
        {
            _logger.LogInformation($"Begin to read {_repeatCount} repeats dicom files from {_directoryPath}.");

            List<Task> tasks = new List<Task>();
            for (int i = 0; i < _repeatCount; i++)
            {
                if (tasks.Count >= _readBlobConcurrency)
                {
                    var finishedTask = await Task.WhenAny(tasks);
                    await finishedTask;
                    tasks.Remove(finishedTask);
                }

                string progress = $"{i}/{_directoryPath}";
                tasks.Add(Task.Run(() => ReadSingleBlobAsync(writer, progress, i, cancellationTokenSource)));

                _logger.LogInformation($"Read {i} repeat from {progress}");
            }

            await Task.WhenAll(tasks);

            writer.Complete();
        }

        private async Task ReadSingleBlobAsync(ChannelWriter<ResourceItem> writer, string progress, int index, CancellationTokenSource cancellationTokenSource = default)
        {
            // _logger.LogInformation($"Read blob {progress} from {filePath}");
            if (cancellationTokenSource.IsCancellationRequested)
            {
                throw new OperationCanceledException($"Blob reader {progress} canceled.");
            };

            try
            {
                var dicomResults = DicomDataGenerator.Generate(_directoryPath);
                foreach (var dicomResult in dicomResults)
                {
                    var item = new ResourceItem { DicomFile = dicomResult.DicomFile, FilePath = dicomResult.FilePath, Index = index, ResourceType = "Dicom" };
                    await writer.WriteAsync(item);
                }
            }
            catch (Exception ex)
            {
                _logger.LogError($"Read Dicom directory {progress} failed: {ex}");
                cancellationTokenSource.Cancel();
                throw;
            }

            // _logger.LogInformation($"Completed reading {index - 1} resources from {progress} {filePath}");
        }
    }
}
