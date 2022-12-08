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
            string webServerUrl = "https://testfhirservices-test-replace.dicom.azurehealthcareapis.com";
            string bearerToken = "eyJ0eXAiOiJKV1QiLCJhbGciOiJSUzI1NiIsIng1dCI6IjJaUXBKM1VwYmpBWVhZR2FYRUpsOGxWMFRPSSIsImtpZCI6IjJaUXBKM1VwYmpBWVhZR2FYRUpsOGxWMFRPSSJ9.eyJhdWQiOiJodHRwczovL2RpY29tLmhlYWx0aGNhcmVhcGlzLmF6dXJlLmNvbSIsImlzcyI6Imh0dHBzOi8vc3RzLndpbmRvd3MubmV0LzcyZjk4OGJmLTg2ZjEtNDFhZi05MWFiLTJkN2NkMDExZGI0Ny8iLCJpYXQiOjE2Njk5ODA5MjIsIm5iZiI6MTY2OTk4MDkyMiwiZXhwIjoxNjY5OTg2MTAwLCJhY3IiOiIxIiwiYWlvIjoiQVZRQXEvOFRBQUFBcmhLakw3R0hvZ1Bza2JSZGRaOGNMUFhWQ2toS2ZscXhiaGJTaXlhd293aVB3cko2cHN4ODFoYUZXQU85YWs4QnAxUVhDVVd3b3p1SmlZdDgvUU5WTTU5M1dGWFo4aDMwWm1hNmVTVW0vMXc9IiwiYW1yIjpbInJzYSIsIm1mYSJdLCJhcHBpZCI6IjA0YjA3Nzk1LThkZGItNDYxYS1iYmVlLTAyZjllMWJmN2I0NiIsImFwcGlkYWNyIjoiMCIsImRldmljZWlkIjoiYjI3ZWQzNTktOWE5MC00YTE4LWFhODEtMjU1YmJlYzAwNjI0IiwiZmFtaWx5X25hbWUiOiJXYW4iLCJnaXZlbl9uYW1lIjoiUXVhbiIsImlwYWRkciI6IjE2Ny4yMjAuMjU1LjciLCJuYW1lIjoiUXVhbiBXYW4iLCJvaWQiOiJkNDA0NjZhZi01ZThjLTQwMTEtOWM1NS0xMGE5ZDI0NGU2NjUiLCJvbnByZW1fc2lkIjoiUy0xLTUtMjEtMjE0Njc3MzA4NS05MDMzNjMyODUtNzE5MzQ0NzA3LTI2Mjc4MDUiLCJwdWlkIjoiMTAwMzIwMDBDMzIwMjlEQSIsInJoIjoiMC5BUm9BdjRqNWN2R0dyMEdScXkxODBCSGJSNzhsNTNYT1p1cE1tNXBjVEtybGZ6TWFBS2cuIiwic2NwIjoidXNlcl9pbXBlcnNvbmF0aW9uIiwic3ViIjoic09pQldvdzZJWVFONmh3MkJmRXV1NndWRWc1UV9YaGNFMmNqWkRIU1FsZyIsInRpZCI6IjcyZjk4OGJmLTg2ZjEtNDFhZi05MWFiLTJkN2NkMDExZGI0NyIsInVuaXF1ZV9uYW1lIjoicXV3YW5AbWljcm9zb2Z0LmNvbSIsInVwbiI6InF1d2FuQG1pY3Jvc29mdC5jb20iLCJ1dGkiOiJPTWRjOC14VU9FcVAxZkhMRENGWkFnIiwidmVyIjoiMS4wIn0.QJSroxas9SoBtVbzYjWJko4C3XhHNq5GTKh2BGxgNJaGsvj3f9qM7ub0BddzNWC_l2jfup9RI-d7BaOJdVU992OJ3cF3Q5Xoc10AGsupfBu_VO3c2ttxdhHQgrsw48_Olk4zhj3YWioGzyaR14myv_x9unUGWooFL3bPuygPlo8lZfJxjFXtsscBTFt0PGZ4KC2_jhRZqZt1fKhEBB-mR9WeYvgImf8fogb_c9PL1mEXL6PQudXo8qDuN1LX3ALNvVcjsu5Vr64jWHpsvpt0O_HV4utDpA613u72WxW1DvF-_a7915c6BYUaz29ans0ZusF1RhZEIh9VQNEvrc87BQ";

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

            // _client.DeleteStudyAsync("1.2.826.0.1.3680043.8.498.13230779778012324449356534479549187420").GetAwaiter().GetResult();

            // _client.DeleteStudyAsync("1.3.6.1.4.1.14519.5.2.1.1706.8374.164750580271137946982420100377").GetAwaiter().GetResult();

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
