using Amazon.DynamoDBv2;
using Amazon.DynamoDBv2.Model;

namespace Turbine;

internal class Put<T> : IPut<T>
{
    private readonly AmazonDynamoDBClient client;
    private readonly EntitySchema schema;

    public Put(EntitySchema schema, AmazonDynamoDBClient client)
    {
        this.schema = schema;
        this.client = client;
    }

    public async Task<bool> PutIfNotExistsAsync(T item)
    {
        ArgumentNullException.ThrowIfNull(item);

        var putRequest = PrepareRequest(item);

        putRequest.ConditionExpression =
            $"attribute_not_exists({schema.TableSchema.Pk}) AND attribute_not_exists({schema.TableSchema.Sk})";

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
        var putRequest = PrepareRequest(item);

        await client.PutItemAsync(putRequest);
    }

    public async Task UpsertAsync(IEnumerable<T> items)
    {
        var putRequests = items
            .Select(i => new WriteRequest { PutRequest = new PutRequest(ConvertToAttributes(i)) })
            .Chunk(25)
            .Select(chunk => chunk.ToList())
            .ToArray();

        var batchWriteRequest = new BatchWriteItemRequest
        {
            RequestItems = new Dictionary<string, List<WriteRequest>>()
        };

        foreach (var batchRequest in putRequests)
        {
            batchWriteRequest.RequestItems[schema.TableSchema.TableName] = batchRequest;
            await client.BatchWriteItemAsync(batchWriteRequest);
        }
    }

    private Dictionary<string, AttributeValue> ConvertToAttributes(T item)
    {
        ArgumentNullException.ThrowIfNull(item);

        var attributes = new Dictionary<string, AttributeValue>
        {
            { schema.TableSchema.Pk, new AttributeValue(schema.GetPk(item)) },
            { schema.TableSchema.Sk, new AttributeValue(schema.GetSk(item)) }
        };

        var entityType = typeof(T);
        var props = entityType.GetProperties();

        foreach (var prop in props)
        {
            var shouldSkip = schema.IsPkProperty(prop.Name) || schema.IsSkProperty(prop.Name);

            if (shouldSkip)
            {
                continue;
            }

            var value = prop.GetValue(item);

            if (value is not null)
            {
                attributes.Add(prop.Name, Reflection.ToAttributeValue(value));
            }
        }

        return attributes;
    }

    private PutItemRequest PrepareRequest(T item)
    {
        var putRequest = new PutItemRequest { TableName = schema.TableSchema.TableName };

        var attributes = ConvertToAttributes(item);

        putRequest.Item = attributes;

        return putRequest;
    }
}