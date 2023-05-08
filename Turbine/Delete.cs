using Amazon.DynamoDBv2;
using Amazon.DynamoDBv2.Model;

namespace Turbine;

internal class Delete<T> : IDelete<T>
{
    private readonly IAmazonDynamoDB client;
    private readonly ItemSchema<T> itemSchema;
    private readonly IQueryBuilderPk<T> query;

    public Delete(ItemSchema<T> itemSchema, IQueryBuilderPk<T> query, IAmazonDynamoDB client)
    {
        this.itemSchema = itemSchema;
        this.query = query;
        this.client = client;
    }

    public async Task DeleteAsync(string pk, string sk)
    {
        var key = new Dictionary<string, AttributeValue>
        {
            { itemSchema.TableSchema.Pk, new AttributeValue(pk) },
            { itemSchema.TableSchema.Sk, new AttributeValue(sk) }
        };

        var deleteRequest = new DeleteItemRequest
        {
            TableName = itemSchema.TableSchema.TableName,
            Key = key
        };

        _ = await client.DeleteItemAsync(deleteRequest);
    }

    public async Task DeleteAsync(string pk, SortKey sk)
    {
        if (sk.IsExactly)
        {
            await DeleteAsync(pk, sk.AttributeValue1.S);
            return;
        }

        var items = await query
            .WithPk(pk)
            .WithSk(sk)
            .ToListAsync(25);

        await BatchDelete(items);
    }

    public async Task DeleteAsync(T item)
    {
        ArgumentNullException.ThrowIfNull(item);

        var pk = itemSchema.GetPk(item);
        var sk = itemSchema.GetSk(item);

        await DeleteAsync(pk, sk);
    }

    private async Task BatchDelete(QueryList<T> items)
    {
        while (true)
        {
            if (!items.Any())
            {
                break;
            }

            var writeRequests = items.Select(i => new WriteRequest
                {
                    DeleteRequest = new DeleteRequest
                    {
                        Key = new Dictionary<string, AttributeValue>
                        {
                            { itemSchema.TableSchema.Pk, new AttributeValue(itemSchema.GetPk(i!)) },
                            { itemSchema.TableSchema.Sk, new AttributeValue(itemSchema.GetSk(i!)) }
                        }
                    }
                })
                .ToList();

            var requestItems = new Dictionary<string, List<WriteRequest>>
                { { itemSchema.TableSchema.TableName, writeRequests } };

            _ = await client.BatchWriteItemAsync(requestItems);

            if (items.HasNextPage)
            {
                var nextPage = await items.NextPageAsync();
                items = nextPage;
                continue;
            }

            break;
        }
    }
}

internal class TransactDelete<T> : ITransactDelete<T>
{
    private readonly ItemSchema<T> itemSchema;
    private readonly ITurbineTransact turbineTransact;
    private readonly Action<TransactWriteItem> writeItem;

    public TransactDelete(
        ItemSchema<T> itemSchema,
        ITurbineTransact turbineTransact,
        Action<TransactWriteItem> writeItem)
    {
        this.itemSchema = itemSchema;
        this.turbineTransact = turbineTransact;
        this.writeItem = writeItem;
    }

    public ITurbineTransact Delete(string pk, string sk)
    {
        writeItem(new TransactWriteItem
        {
            Delete = new Delete
            {
                TableName = itemSchema.TableSchema.TableName,
                Key = new Dictionary<string, AttributeValue>
                {
                    { itemSchema.TableSchema.Pk, new AttributeValue(pk) },
                    { itemSchema.TableSchema.Sk, new AttributeValue(sk) }
                }
            }
        });

        return turbineTransact;
    }

    public ITurbineTransact Delete(string pk, string sk, Condition condition)
    {
        var conditionCheck = condition.ToConditionCheck(itemSchema.TableSchema.TableName);

        var transactionWrite = new TransactWriteItem
        {
            Delete = new Delete
            {
                TableName = itemSchema.TableSchema.TableName,
                Key = new Dictionary<string, AttributeValue>
                {
                    { itemSchema.TableSchema.Pk, new AttributeValue(pk) },
                    { itemSchema.TableSchema.Sk, new AttributeValue(sk) }
                }
            }
        };

        if (conditionCheck is not null)
        {
            transactionWrite.Delete.ConditionExpression = conditionCheck.ConditionExpression;
            transactionWrite.Delete.ExpressionAttributeValues = conditionCheck.ExpressionAttributeValues;
        }

        writeItem(transactionWrite);

        return turbineTransact;
    }

    public ITurbineTransact Delete(T item)
    {
        ArgumentNullException.ThrowIfNull(item);

        return Delete(itemSchema.GetPk(item), itemSchema.GetSk(item));
    }

    public ITurbineTransact Delete(T item, Condition condition)
    {
        ArgumentNullException.ThrowIfNull(item);

        return Delete(itemSchema.GetPk(item), itemSchema.GetSk(item), condition);
    }
}