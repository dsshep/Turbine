using Amazon.DynamoDBv2;
using Amazon.DynamoDBv2.Model;

namespace Turbine;

internal struct PreparedQuery
{
    public Schema Schema { get; }
    public string Pk { get; }
    public SortKey Sk { get; }

    public PreparedQuery(Schema schema, string pk, SortKey sk)
    {
        Schema = schema;
        Pk = pk;
        Sk = sk;
    }
}

internal class Query<T> : IPageableQuery, IQuery<T>
{
    private readonly AmazonDynamoDBClient client;
    private readonly PreparedQuery query;

    public Query(PreparedQuery query, AmazonDynamoDBClient client)
    {
        this.query = query;
        this.client = client;
    }

    public async Task<QueryResponse> DoQuery(int? itemLimit, Dictionary<string, AttributeValue>? lastEvalKey = null)
    {
        var (pk, sk, schema) = (query.Pk, query.Sk, query.Schema);
        var sortKeyExpr = sk.KeyExpr.Replace("<SORT_KEY>", schema.Sk);

        var queryRequest = new QueryRequest
        {
            TableName = schema.TableName,
            KeyConditionExpression = $"{schema.Pk} = :pkVal AND {sortKeyExpr}"
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
            ? EntityBuilder.HydrateEntity<T>(query.Schema, result.Items[0])
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
            .Select(item => EntityBuilder.HydrateEntity<T>(query.Schema, item))
            .ToList();

        return new QueryList<T>(entities, limit, result, this, query.Schema);
    }
}

internal struct PreparedPk
{
    public Schema Schema { get; }
    public string Pk { get; }

    public PreparedPk(Schema schema, string pk)
    {
        Schema = schema;
        Pk = pk;
    }
}

internal class QueryBuilderSk<T> : IQueryBuilderSk<T>
{
    private readonly AmazonDynamoDBClient client;
    private readonly PreparedPk pk;

    public QueryBuilderSk(PreparedPk pk, AmazonDynamoDBClient client)
    {
        this.pk = pk;
        this.client = client;
    }

    public IQuery<T> WithSk(SortKey sortKey)
    {
        return new Query<T>(
            new PreparedQuery(pk.Schema, pk.Pk, sortKey),
            client);
    }
}

internal class QueryBuilderPk<T> : IQueryBuilderPk<T>
{
    private readonly AmazonDynamoDBClient client;
    private readonly Schema schema;

    public QueryBuilderPk(Schema schema, AmazonDynamoDBClient client)
    {
        this.schema = schema;
        this.client = client;
    }

    public IQueryBuilderSk<T> WithPk(string value)
    {
        return new QueryBuilderSk<T>(new PreparedPk(schema, value), client);
    }
}