public class ResourceItem
{
    public string Resource { get; set; }
    public string BlobName { get; set; }
    public int Index { get; set; }

    public string Id { get; set; }

    public string ResourceType { get; set; }

    public override string ToString()
    {
        return $"{{\"Resource\": {Resource}, \"BlobName\": {BlobName}, \"Index\": {Index}}}";
    }
}