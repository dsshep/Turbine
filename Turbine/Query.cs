using Amazon.DynamoDBv2;
using Amazon.DynamoDBv2.Model;

namespace Turbine;

internal struct PreparedQuery<T>
{
    public ItemSchema<T> ItemSchema { get; }
    public string Pk { get; }
    public SortKey Sk { get; }
    public string? Index { get; }

    public PreparedQuery(ItemSchema<T> itemSchema, string pk, SortKey sk, string? index)
    {
        ItemSchema = itemSchema;
        Pk = pk;
        Sk = sk;
        Index = index;
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
        var queryRequest = BuildQuery();

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

    private QueryRequest BuildQuery()
    {
        static (string, string) GetKeys(PreparedQuery<T> query)
        {
            if (query.Index is null)
            {
                return (query.ItemSchema.TableSchema.Pk, query.ItemSchema.TableSchema.Sk);
            }

            var gsi = query.ItemSchema.TableSchema.GlobalSecondaryIndexes[query.Index];

            return (gsi.PkName, gsi.SkName);
        }

        var (pkVal, skVal, schema, index) = (query.Pk, query.Sk, query.ItemSchema, query.Index);
        var (pk, sk) = GetKeys(query);
        var sortKeyExpr = skVal.KeyExpr.Replace("<SORT_KEY>", sk);

        var queryRequest = new QueryRequest
        {
            IndexName = query.Index,
            TableName = schema.TableSchema.TableName,
            KeyConditionExpression = $"{pk} = :pkVal AND {sortKeyExpr}"
        };

        var expressionAttributes = skVal.AttributeValue2 is not null
            ? new List<KeyValuePair<string, AttributeValue>>
            {
                new(":pkVal", new AttributeValue(pkVal)),
                new(":skVal1", skVal.AttributeValue1),
                new(":skVal2", skVal.AttributeValue2)
            }
            : new List<KeyValuePair<string, AttributeValue>>
            {
                new(":pkVal", new AttributeValue(pkVal)),
                new(":skVal", skVal.AttributeValue1)
            };

        queryRequest.ExpressionAttributeValues = new Dictionary<string, AttributeValue>(expressionAttributes);

        return queryRequest;
    }
}

internal struct PreparedPk<T>
{
    public ItemSchema<T> ItemSchema { get; }
    public string Pk { get; }
    public string? IndexName { get; }

    public PreparedPk(ItemSchema<T> itemSchema, string pk, string? indexName)
    {
        ItemSchema = itemSchema;
        Pk = pk;
        IndexName = indexName;
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
            new PreparedQuery<T>(pk.ItemSchema, pk.Pk, sortKey, pk.IndexName),
            client);
    }
}

internal class QueryBuilderPk<T> : IQueryBuilderPk<T>
{
    private readonly IAmazonDynamoDB client;
    private readonly string? index;
    private readonly ItemSchema<T> itemSchema;

    public QueryBuilderPk(ItemSchema<T> itemSchema, IAmazonDynamoDB client, string? index)
    {
        this.itemSchema = itemSchema;
        this.client = client;
        this.index = index;
    }

    public IQueryBuilderPk<T> Gsi(string indexName)
    {
        if (itemSchema.TableSchema.GlobalSecondaryIndexes.ContainsKey(indexName))
        {
            return new QueryBuilderPk<T>(itemSchema, client, indexName);
        }

        var registeredGsis = string.Join(", ", itemSchema.TableSchema.GlobalSecondaryIndexes.Keys);

        throw new TurbineException($"Cannot find GSI index with name '{indexName}'. " +
                                   $"The following have been registered: [{registeredGsis}]");
    }

    public IQueryBuilderSk<T> WithPk(string value)
    {
        return new QueryBuilderSk<T>(new PreparedPk<T>(itemSchema, value, index), client);
    }
}