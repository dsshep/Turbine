using System.Text.Json;
using Amazon.DynamoDBv2;
using Amazon.DynamoDBv2.Model;

namespace Turbine;

internal class Put<T> : IPut<T>
{
    private readonly IAmazonDynamoDB client;
    private readonly ItemSchema schema;

    public Put(ItemSchema schema, IAmazonDynamoDB client)
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
            .Select(i => new WriteRequest { PutRequest = new PutRequest(AttributeConverter.Convert(schema, i)) })
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

    private PutItemRequest PrepareRequest(T item)
    {
        var putRequest = new PutItemRequest { TableName = schema.TableSchema.TableName };

        var attributes = AttributeConverter.Convert(schema, item);

        putRequest.Item = attributes;

        return putRequest;
    }
}

internal class TransactPut<T> : ITransactPut<T>
{
    private readonly ItemSchema<T> itemSchema;
    private readonly ITurbineTransact turbineTransact;
    private readonly Action<TransactWriteItem> writeItem;

    public TransactPut(
        ItemSchema<T> itemSchema,
        ITurbineTransact turbineTransact,
        Action<TransactWriteItem> writeItem)
    {
        this.itemSchema = itemSchema;
        this.turbineTransact = turbineTransact;
        this.writeItem = writeItem;
    }

    public ITurbineTransact Upsert(T entity)
    {
        return Upsert(entity, Condition.None);
    }

    public ITurbineTransact Upsert(T entity, Condition condition)
    {
        var item = new TransactWriteItem
        {
            Put = new Put
            {
                TableName = itemSchema.TableSchema.TableName,
                Item = AttributeConverter.Convert(itemSchema, entity)
            }
        };

        if (condition != Condition.None)
        {
            item.ConditionCheck = condition.ToConditionCheck(itemSchema.TableSchema.TableName);
        }

        writeItem(new TransactWriteItem
        {
            Put = new Put
            {
                TableName = itemSchema.TableSchema.TableName,
                Item = AttributeConverter.Convert(itemSchema, entity)
            }
        });

        return turbineTransact;
    }

    public ITurbineTransact Upsert(IEnumerable<T> entities)
    {
        return Upsert(entities, Condition.None);
    }

    public ITurbineTransact Upsert(IEnumerable<T> entities, Condition condition)
    {
        var enumerable = entities as T[] ?? entities.ToArray();

        if (enumerable.Length > 25)
        {
            throw new TurbineException("Cannot insert more than 25 items in a single transaction.");
        }

        foreach (var item in enumerable)
        {
            Upsert(item, condition);
        }

        return turbineTransact;
    }
}

internal static class AttributeConverter
{
    public static Dictionary<string, AttributeValue> Convert<T>(ItemSchema schema, T item)
    {
        ArgumentNullException.ThrowIfNull(item);

        var attributes = new Dictionary<string, AttributeValue>
        {
            { schema.TableSchema.Pk, new AttributeValue(schema.GetPk(item)) },
            { schema.TableSchema.Sk, new AttributeValue(schema.GetSk(item)) }
        };

        if (schema.IsJsonItem(out var jsonAttribute))
        {
            attributes[jsonAttribute] = new AttributeValue
            {
                S = JsonSerializer.Serialize(item)
            };

            return attributes;
        }

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
}