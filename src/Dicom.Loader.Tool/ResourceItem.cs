using FellowOakDicom;

public class ResourceItem
{
    public DicomFile DicomFile { get; set; }

    public string FilePath { get; set; }

    public int Index { get; set; }

    public string ResourceType { get; set; }

    public override string ToString()
    {
        return $"{{\"BlobName\": {FilePath}, \"Index\": {Index}}}";
    }
}