using System.IO;
using System.Threading.Tasks;
using Microsoft.ApplicationInsights.WorkerService;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace Fhir.Loader.Tool
{
    public class Program
    {
        public static async Task Main(string[] args)
        {
            using IHost host = Host.CreateDefaultBuilder(args)
                .ConfigureHostConfiguration(configHost =>
                {
                    configHost.SetBasePath(Directory.GetCurrentDirectory());
                    configHost.AddJsonFile("appsettings.json", optional: true);
                    configHost.AddEnvironmentVariables(prefix: "PREFIX_");
                    configHost.AddCommandLine(args);
                })
               .ConfigureServices((context, services) =>
               {
                   var configurationRoot = context.Configuration;
                   services.Configure<UploadConfiguration>(configurationRoot);
                   services.AddHttpClient();
                   services
                    .AddSingleton<FhirAccessTokenProvider>()
                    .AddSingleton<FhirUploader>()
                    .AddSingleton<BlobStreamReader>()
                    .AddHostedService<FhirUploadService>()
                    .AddApplicationInsightsTelemetryWorkerService(new ApplicationInsightsServiceOptions {  EnableDependencyTrackingTelemetryModule = false} );
    
                   var logger = services.BuildServiceProvider().GetRequiredService<ILogger<Program>>();
                   logger.LogInformation("Logging setup.");
               }).Build();

            // Application code should start here.

            await host.RunAsync();

        }
    }
}