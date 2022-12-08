using FellowOakDicom;
using FellowOakDicom.Imaging;

namespace Dicom.Loader.Tool
{
    public class DicomDataGenerator
    {
        public static List<DicomResult> Generate(string directoryPath)
        {
            var files = Directory.EnumerateFiles(directoryPath, "*.dcm", SearchOption.AllDirectories);

            var dicomFiles = new List<DicomResult>();

            foreach (var file in files)
            {
                var dicomFile = DicomFile.Open(file);

                // Update SOPInstanceUID
                var sopInstanceUid = DicomUIDGenerator.GenerateDerivedFromUUID();
                dicomFile.Dataset.AddOrUpdate(DicomTag.SOPInstanceUID, sopInstanceUid);

                DicomDataset dataset = new DicomDataset(dicomFile.Dataset);
                dataset.AddOrUpdate(DicomTag.PhotometricInterpretation, PhotometricInterpretation.Rgb.Value);
                dataset.AddOrUpdate(DicomTag.Rows, (ushort)1);
                dataset.AddOrUpdate(DicomTag.Columns, (ushort)1);
                dataset.AddOrUpdate(DicomTag.BitsAllocated, (ushort)8);

                DicomPixelData pixelData = DicomPixelData.Create(dataset, true);

                pixelData.BitsStored = 8;
                pixelData.SamplesPerPixel = 3;
                pixelData.HighBit = 7;
                pixelData.PhotometricInterpretation = PhotometricInterpretation.Rgb;
                pixelData.PixelRepresentation = 0;
                pixelData.PlanarConfiguration = 0;
                pixelData.Height = (ushort)1;
                pixelData.Width = (ushort)1;

                dicomFiles.Add(new DicomResult()
                {
                    DicomFile = new DicomFile(dataset.Clone()),
                    FilePath = file,
                }); 
            }

            return dicomFiles;
        }
    }
}
