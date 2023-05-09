using Amazon.DynamoDBv2;
using Amazon.DynamoDBv2.Model;

namespace Turbine;

internal struct PreparedQuery<T>
{
    public ItemSchema<T> ItemSchema { get; }
    public string Pk { get; }
    public SortKey Sk { get; }

    public PreparedQuery(ItemSchema<T> itemSchema, string pk, SortKey sk)
    {
        ItemSchema = itemSchema;
        Pk = pk;
        Sk = sk;
    }
}

internal class Query<T> : IPageableQuery, IQuery<T>
{
    private readonly IAmazonDynamoDB client;
    private readonly IItemBuilder<T> itemBuilder;
    private readonly PreparedQuery<T> query;

    public Query(PreparedQuery<T> query, IAmazonDynamoDB client)
    {
        itemBuilder = new ItemBuilderHelper<T>(query.ItemSchema).GetBuilder();
        this.query = query;
        this.client = client;
    }

    public async Task<QueryResponse> DoQuery(int? itemLimit, Dictionary<string, AttributeValue>? lastEvalKey = null)
    {
        var (pk, sk, schema) = (query.Pk, query.Sk, query.ItemSchema);
        var sortKeyExpr = sk.KeyExpr.Replace("<SORT_KEY>", schema.TableSchema.Sk);

        var queryRequest = new QueryRequest
        {
            TableName = schema.TableSchema.TableName,
            KeyConditionExpression = $"{schema.TableSchema.Pk} = :pkVal AND {sortKeyExpr}"
        };

        var expressionAttributes = sk.AttributeValue2 is not null
            ? new List<KeyValuePair<string, AttributeValue>>
            {
                new(":pkVal", new AttributeValue(pk)),
                new(":skVal1", sk.AttributeValue1),
                new(":skVal2", sk.AttributeValue2)
            }
            : new List<KeyValuePair<string, AttributeValue>>
            {
                new(":pkVal", new AttributeValue(pk)),
                new(":skVal", sk.AttributeValue1)
            };

        queryRequest.ExpressionAttributeValues = new Dictionary<string, AttributeValue>(expressionAttributes);

        if (itemLimit.HasValue)
        {
            queryRequest.Limit = itemLimit.Value;
        }

        queryRequest.ExclusiveStartKey = lastEvalKey;

        var result = await client.QueryAsync(queryRequest);
        return result;
    }

    public async Task<T?> FirstOrDefaultAsync()
    {
        var result = await DoQuery(1);

        return result.Items.Count == 1
            ? itemBuilder.HydrateEntity(result.Items[0])
            : default;
    }

    public Task<QueryList<T>> ToListAsync()
    {
        return ToListAsync(new int?());
    }

    public async Task<QueryList<T>> ToListAsync(int? limit)
    {
        var result = await DoQuery(limit);

        var entities = result.Items
            .Select(item => itemBuilder.HydrateEntity(item))
            .ToList();

        return new QueryList<T>(entities, limit, result, this, itemBuilder);
    }
}

internal struct PreparedPk<T>
{
    public ItemSchema<T> ItemSchema { get; }
    public string Pk { get; }

    public PreparedPk(ItemSchema<T> itemSchema, string pk)
    {
        ItemSchema = itemSchema;
        Pk = pk;
    }
}

internal class QueryBuilderSk<T> : IQueryBuilderSk<T>
{
    private readonly IAmazonDynamoDB client;
    private readonly PreparedPk<T> pk;

    public QueryBuilderSk(PreparedPk<T> pk, IAmazonDynamoDB client)
    {
        this.pk = pk;
        this.client = client;
    }

    public IQuery<T> WithSk(SortKey sortKey)
    {
        return new Query<T>(
            new PreparedQuery<T>(pk.ItemSchema, pk.Pk, sortKey),
            client);
    }
}

internal class QueryBuilderPk<T> : IQueryBuilderPk<T>
{
    private readonly IAmazonDynamoDB client;
    private readonly ItemSchema<T> itemSchema;

    public QueryBuilderPk(ItemSchema<T> itemSchema, IAmazonDynamoDB client)
    {
        this.itemSchema = itemSchema;
        this.client = client;
    }

    public IQueryBuilderSk<T> WithPk(string value)
    {
        return new QueryBuilderSk<T>(new PreparedPk<T>(itemSchema, value), client);
    }
}