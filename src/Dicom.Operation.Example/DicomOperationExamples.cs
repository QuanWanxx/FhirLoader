using System;
using System.Collections.Generic;
using System.Linq;
using System.Net.Http.Headers;
using System.Text;
using System.Threading.Tasks;
using FellowOakDicom;
using Microsoft.Health.Dicom.Client;

namespace Dicom.Operation.Example
{
    public class DicomOperationExample
    {
        private static IDicomWebClient _client;

        public static void RunExample()
        {
            string webServerUrl = "";
            string bearerToken = "";

            var httpClient = new HttpClient();
            httpClient.BaseAddress = new Uri(webServerUrl);
            _client = new DicomWebClient(httpClient);
            if (!string.IsNullOrWhiteSpace(bearerToken))
            {
                _client.HttpClient.DefaultRequestHeaders.Authorization = new AuthenticationHeaderValue("Bearer", bearerToken);
            }

            string dicomDirectoryPath = @"D:\Data\Dicom\generated_300";

            // Upload_Dicom_Sample().GetAwaiter().GetResult();

            Upload_Dicom_Directory(dicomDirectoryPath).GetAwaiter().GetResult();

            // _client.DeleteStudyAsync("").GetAwaiter().GetResult();

            // _client.DeleteStudyAsync("").GetAwaiter().GetResult();

            Console.WriteLine("Hello, World!");
        }

        private static async Task Upload_Dicom_Sample()
        {
            var dicomFile = await DicomFile.OpenAsync(@"C:\quwan\dicom-server\docs\dcms\blue-circle.dcm");

            List<DicomFile> dicomFiles = new List<DicomFile>()
            {
                await DicomFile.OpenAsync(@"C:\quwan\dicom-server\docs\dcms\blue-circle.dcm"),
                await DicomFile.OpenAsync(@"C:\quwan\dicom-server\docs\dcms\green-square.dcm"),
                await DicomFile.OpenAsync(@"C:\quwan\dicom-server\docs\dcms\red-triangle.dcm"),
            };

            DicomWebResponse response = await _client.StoreAsync(dicomFiles);
            Console.WriteLine(response.StatusCode);
        }

        private static async Task Upload_Dicom_Directory(string directoryPath)
        {
            var dicomFiles = Directory.GetFiles(directoryPath)
                .Select(filePath => DicomFile.Open(filePath))
                .ToList();

            DicomWebResponse response;
            int index = 0;
            int count = 20;
            while (index + count <= dicomFiles.Count)
            {
                try
                {
                    response = await _client.StoreAsync(dicomFiles.GetRange(index, count));
                }
                catch (Exception ex)
                {
                    Console.WriteLine(ex.Message);
                }
                index += count;

                Console.WriteLine($"Uploaded {index} ~ {index + count}");
            }

            try
            {
                response = await _client.StoreAsync(dicomFiles.GetRange(index, dicomFiles.Count - index));
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.Message);
            }

            Console.WriteLine($"Uploaded {index} ~ {dicomFiles.Count - 1}");
        }
    }
}
