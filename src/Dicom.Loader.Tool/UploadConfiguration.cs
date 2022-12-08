using Newtonsoft.Json;

namespace Dicom.Loader.Tool
{
    public class UploadConfiguration
    {
        [JsonProperty("dicomServerUrl")]
        public string DicomServerUrl { get; set; }

        [JsonProperty("useFhirAuthentication")]
        public bool UseFhirAuthentication { get; set; }

        [JsonProperty("directoryPath")]
        public string DirectoryPath { get; set; }

        [JsonProperty("repeatCount")]
        public int RepeatCount { get; set; }

        [JsonProperty("readBlobConcurrency")]
        public int ReadBlobConcurrency { get; set; }

        [JsonProperty("putFhirConcurrency")]
        public int PutFhirConcurrency { get; set; }

        [JsonProperty("tenantId")]
        public string TenantId { get; set; }

        [JsonProperty("clientId")]
        public string ClientId { get; set; }

        [JsonProperty("clientSecret")]
        public string ClientSecret { get; set; }
    }
}
