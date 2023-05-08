using Amazon.DynamoDBv2;
using Amazon.DynamoDBv2.Model;

namespace Turbine;

internal struct PreparedQuery<T>
{
    public EntitySchema<T> EntitySchema { get; }
    public string Pk { get; }
    public SortKey Sk { get; }

    public PreparedQuery(EntitySchema<T> entitySchema, string pk, SortKey sk)
    {
        EntitySchema = entitySchema;
        Pk = pk;
        Sk = sk;
    }
}

internal class Query<T> : IPageableQuery, IQuery<T>
{
    private readonly AmazonDynamoDBClient client;
    private readonly PreparedQuery<T> query;

    public Query(PreparedQuery<T> query, AmazonDynamoDBClient client)
    {
        this.query = query;
        this.client = client;
    }

    public async Task<QueryResponse> DoQuery(int? itemLimit, Dictionary<string, AttributeValue>? lastEvalKey = null)
    {
        var (pk, sk, schema) = (query.Pk, query.Sk, query.EntitySchema);
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
            ? EntityBuilder.HydrateEntity<T>(query.EntitySchema, result.Items[0])
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
            .Select(item => EntityBuilder.HydrateEntity<T>(query.EntitySchema, item))
            .ToList();

        return new QueryList<T>(entities, limit, result, this, query.EntitySchema);
    }
}

internal struct PreparedPk<T>
{
    public EntitySchema<T> EntitySchema { get; }
    public string Pk { get; }

    public PreparedPk(EntitySchema<T> entitySchema, string pk)
    {
        EntitySchema = entitySchema;
        Pk = pk;
    }
}

internal class QueryBuilderSk<T> : IQueryBuilderSk<T>
{
    private readonly AmazonDynamoDBClient client;
    private readonly PreparedPk<T> pk;

    public QueryBuilderSk(PreparedPk<T> pk, AmazonDynamoDBClient client)
    {
        this.pk = pk;
        this.client = client;
    }

    public IQuery<T> WithSk(SortKey sortKey)
    {
        return new Query<T>(
            new PreparedQuery<T>(pk.EntitySchema, pk.Pk, sortKey),
            client);
    }
}

internal class QueryBuilderPk<T> : IQueryBuilderPk<T>
{
    private readonly AmazonDynamoDBClient client;
    private readonly EntitySchema<T> entitySchema;

    public QueryBuilderPk(EntitySchema<T> entitySchema, AmazonDynamoDBClient client)
    {
        this.entitySchema = entitySchema;
        this.client = client;
    }

    public IQueryBuilderSk<T> WithPk(string value)
    {
        return new QueryBuilderSk<T>(new PreparedPk<T>(entitySchema, value), client);
    }
}