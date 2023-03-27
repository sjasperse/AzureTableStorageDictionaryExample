using Azure;
using Azure.Data.Tables;
using System.Text.Json;

var tableClient = new TableClient(connectionString: "UseDevelopmentStorage=true", tableName: "schemalesspoc");
await tableClient.CreateIfNotExistsAsync();

var jsonOpts = new JsonSerializerOptions 
{
    PropertyNameCaseInsensitive = true,
    WriteIndented = true
};

var eventTime = new DateTimeOffset(new DateTime(2023, 3, 1), TimeSpan.Zero);

var asset1Json = JsonSerializer.Serialize(new {
    assetId = Guid.Parse("00000000-0000-0000-0000-0000000000A1"),
    assetName = "TEST000001",
    accountId = 1,
    properties = new {
        fleet = "A",
        homebase = "A",
        udef_1 = "Aardvark"
    }
}, jsonOpts);

var asset2Json = JsonSerializer.Serialize(new {
    assetId = Guid.Parse("00000000-0000-0000-0000-0000000000B1"),
    assetName = "TEST000002",
    accountId = 2,
    properties = new {
        fleet = "B",
        homebase = "B",
        udef_2 = "Butterfly",
        udef_CustomProp = "Custom"
    }

}, jsonOpts);


var outOfDateEventTime = new DateTimeOffset(new DateTime(2023, 2, 1), TimeSpan.Zero);
var asset2OutOfDateJson = JsonSerializer.Serialize(new {
    assetId = Guid.Parse("00000000-0000-0000-0000-0000000000B1"),
    assetName = "TEST000002",
    accountId = 2,
    properties = new {
        fleet = "B",
        homebase = "B",
        udef_OldProp = "Old"
    }

}, jsonOpts);

await ProcessAsset(asset1Json, eventTime, tableClient, jsonOpts);
await ProcessAsset(asset2Json, eventTime, tableClient, jsonOpts);
await ProcessAsset(asset2OutOfDateJson, outOfDateEventTime, tableClient, jsonOpts);

async Task ProcessAsset(string assetJson, DateTimeOffset eventTime, TableClient tableClient, JsonSerializerOptions jsonOptions)
{
    var assetObj = JsonSerializer.Deserialize<Asset>(assetJson, jsonOpts)!;
    assetObj.Time = eventTime;
    var assetTE = assetObj.ToTableEntity();

    var (existingAsset, etag) = await tableClient.GetAssetIfExists(assetObj.AssetId, assetObj.AccountId);
    if (existingAsset != null)
    {
        if (eventTime > existingAsset.Time)
        {
            await tableClient.UpdateEntityAsync(assetTE, etag!.Value);
        }
    }
    else
    {
        await tableClient.AddEntityAsync(assetTE);
    }

    // re-retrieve it so we can show that it saved and deserialized properly
    var (asset, _) = await tableClient.GetAssetIfExists(assetObj.AssetId, assetObj.AccountId);
 
    Console.WriteLine(JsonSerializer.Serialize(asset, jsonOptions));
}


class Asset
{
    public Guid AssetId { get; set; }
    public string AssetName { get; set; }
    public int AccountId { get; set; }
    public DateTimeOffset? Time { get; set; }
    public Dictionary<string, object> Properties { get; set; } = new ();
}

static class Extensions
{
    public static TableEntity ToTableEntity(this Asset asset)
    {
        var te = new TableEntity(asset.AccountId.ToString(), asset.AssetId.ToString());

        te[nameof(asset.AssetId)] = asset.AssetId;
        te[nameof(asset.AssetName)] = asset.AssetName;
        te[nameof(asset.AccountId)] = asset.AccountId;
        te[nameof(asset.Time)] = asset.Time;

        foreach (var kvp in asset.Properties)
        {
            te[$"prop_{kvp.Key}"] = kvp.Value;
        }

        return te;
    }

    public static Asset ToAsset(this TableEntity te)
    {
        var asset = new Asset()
        {
            AssetId = te.GetGuid("AssetId").Value,
            AssetName = te.GetString("AssetName"),
            AccountId = te.GetInt32("AccountId").Value
        };

        foreach (var kvp in te)
        {
            if (kvp.Key.StartsWith("prop_"))
            {
                var propName = kvp.Key.Substring(5);
                asset.Properties[propName] = kvp.Value;
            }
        }

        return asset;
    }

    public static async Task<(Asset?, Azure.ETag? etag)> GetAssetIfExists(this TableClient tableClient, Guid assetId, int accountId)
    {
        var partitionKey = accountId.ToString();
        var rowKey = assetId.ToString();

        var assetResponse = await tableClient.GetEntityIfExistsAsync<TableEntity>(partitionKey, rowKey);
        var asset = (Asset?)null;
        var etag = (ETag?)null;
        if (assetResponse.HasValue)
        {
            asset = assetResponse.Value.ToAsset();
            etag = assetResponse.Value.ETag;
        }

        return (asset, etag);
    }
}



