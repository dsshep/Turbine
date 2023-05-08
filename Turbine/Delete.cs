using Amazon.DynamoDBv2;
using Amazon.DynamoDBv2.Model;

namespace Turbine;

internal class Delete<T> : IDelete<T>
{
    private readonly IAmazonDynamoDB client;
    private readonly EntitySchema<T> entitySchema;
    private readonly IQueryBuilderPk<T> query;

    public Delete(EntitySchema<T> entitySchema, IQueryBuilderPk<T> query, IAmazonDynamoDB client)
    {
        this.entitySchema = entitySchema;
        this.query = query;
        this.client = client;
    }

    public async Task DeleteAsync(string pk, string sk)
    {
        var key = new Dictionary<string, AttributeValue>
        {
            { entitySchema.TableSchema.Pk, new AttributeValue(pk) },
            { entitySchema.TableSchema.Sk, new AttributeValue(sk) }
        };

        var deleteRequest = new DeleteItemRequest
        {
            TableName = entitySchema.TableSchema.TableName,
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

        var pk = entitySchema.GetPk(item);
        var sk = entitySchema.GetSk(item);

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
                            { entitySchema.TableSchema.Pk, new AttributeValue(entitySchema.GetPk(i!)) },
                            { entitySchema.TableSchema.Sk, new AttributeValue(entitySchema.GetSk(i!)) }
                        }
                    }
                })
                .ToList();

            var requestItems = new Dictionary<string, List<WriteRequest>>
                { { entitySchema.TableSchema.TableName, writeRequests } };

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