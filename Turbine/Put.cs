using Amazon.DynamoDBv2;
using Amazon.DynamoDBv2.Model;

namespace Turbine;

public struct PreparedSk<T>
{
    public Schema Schema { get; }
    public AmazonDynamoDBClient Client { get; }
    public Func<T, string> PkFunc { get; }
    public Func<T, string> SkFunc { get; }

    public PreparedSk(Schema schema, AmazonDynamoDBClient client, Func<T, string> pkFunc, Func<T, string> skFunc)
    {
        Schema = schema;
        Client = client;
        PkFunc = pkFunc;
        SkFunc = skFunc;
    }
}

public class Put<T> : IPut<T>
{
    private readonly PreparedSk<T> preparedSk;

    public Put(PreparedSk<T> preparedSk)
    {
        this.preparedSk = preparedSk;
    }

    public async Task<bool> PutIfNotExists(T item)
    {
        var schema = preparedSk.Schema;
        var client = preparedSk.Client;
        var putRequest = PrepareRequest(item);

        putRequest.ConditionExpression = $"attribute_not_exists({schema.Pk}) AND attribute_not_exists({schema.Sk})";

        try
        {
            await client.PutItemAsync(putRequest);
            return true;
        }
        catch (ConditionalCheckFailedException)
        {
            return false;
        }
    }

    public async Task UpsertAsync(T item)
    {
        var client = preparedSk.Client;
        var putRequest = PrepareRequest(item);

        await client.PutItemAsync(putRequest);
    }

    public async Task UpsertAsync(IEnumerable<T> items)
    {
        var schema = preparedSk.Schema;
        var client = preparedSk.Client;

        var putRequests = items
            .Select(i => new WriteRequest { PutRequest = new PutRequest(ConvertToAttributes(i)) })
            .Chunk(25)
            .Select(chunk => chunk.ToList())
            .ToArray();

        var batchWriteRequest = new BatchWriteItemRequest();
        batchWriteRequest.RequestItems = new Dictionary<string, List<WriteRequest>>();

        foreach (var batchRequest in putRequests)
        {
            batchWriteRequest.RequestItems[schema.TableName] = batchRequest;
            await client.BatchWriteItemAsync(batchWriteRequest);
        }
    }

    private Dictionary<string, AttributeValue> ConvertToAttributes(T item)
    {
        var schema = preparedSk.Schema;

        var attributes = new List<KeyValuePair<string, AttributeValue>>
        {
            new(schema.Pk, new AttributeValue(preparedSk.PkFunc(item))),
            new(schema.Sk, new AttributeValue(preparedSk.SkFunc(item)))
        };

        var entityType = typeof(T);
        var props = entityType.GetProperties();

        foreach (var prop in props)
        {
            var shouldSkip = schema.PartitionMap.TryGetValue(entityType, out var p) && p == prop.Name;

            if (!shouldSkip) shouldSkip = schema.SortMap.TryGetValue(entityType, out var s) && s == prop.Name;

            if (shouldSkip) continue;

            var value = prop.GetValue(item);

            if (value is not null)
                attributes.Add(new KeyValuePair<string, AttributeValue>(prop.Name, Reflection.ToAttributeValue(value)));
        }

        return attributes.ToDictionary(kv => kv.Key, kv => kv.Value);
    }

    private PutItemRequest PrepareRequest(T item)
    {
        var schema = preparedSk.Schema;
        var putRequest = new PutItemRequest { TableName = schema.TableName };

        var attributes = ConvertToAttributes(item);

        putRequest.Item = attributes;

        return putRequest;
    }
}

public struct PreparedPk<T>
{
    public Schema Schema { get; }
    public AmazonDynamoDBClient Client { get; }
    public Func<T, string> PkFunc { get; }

    public PreparedPk(Schema schema, AmazonDynamoDBClient client, Func<T, string> pkFunc)
    {
        Schema = schema;
        Client = client;
        PkFunc = pkFunc;
    }
}

public class PutBuilderSk<T> : IPutBuilderSk<T>
{
    private readonly PreparedPk<T> preparedPk;

    public PutBuilderSk(PreparedPk<T> preparedPk)
    {
        this.preparedPk = preparedPk;
    }

    public IPut<T> WithSk(string value)
    {
        return new Put<T>(new PreparedSk<T>(
            preparedPk.Schema,
            preparedPk.Client,
            preparedPk.PkFunc,
            _ => value));
    }

    public IPut<T> WithSk(Func<T, string> skFunc)
    {
        return new Put<T>(new PreparedSk<T>(
            preparedPk.Schema,
            preparedPk.Client,
            preparedPk.PkFunc,
            skFunc));
    }
}

public class PutBuilderPk<T> : IPutBuilderPk<T>
{
    private readonly AmazonDynamoDBClient client;
    private readonly Schema schema;

    public PutBuilderPk(Schema schema, AmazonDynamoDBClient client)
    {
        this.schema = schema;
        this.client = client;
    }

    public IPutBuilderSk<T> WithPk(string value)
    {
        return new PutBuilderSk<T>(
            new PreparedPk<T>(
                schema,
                client,
                _ => value
            )
        );
    }

    public IPutBuilderSk<T> WithPk(Func<T, string> pkFunc)
    {
        return new PutBuilderSk<T>(
            new PreparedPk<T>(
                schema,
                client,
                pkFunc));
    }
}