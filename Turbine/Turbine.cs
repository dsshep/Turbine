using Amazon.DynamoDBv2;
using Amazon.DynamoDBv2.Model;

namespace Turbine;

public sealed class Turbine : IDisposable
{
    public static readonly IDictionary<Type, Func<AttributeValue, object?>> FromDynamoConverters =
        new Dictionary<Type, Func<AttributeValue, object?>>();

    public static readonly IDictionary<Type, Func<object?, AttributeValue>> ToDynamoConverters =
        new Dictionary<Type, Func<object?, AttributeValue>>();

    private readonly AmazonDynamoDBClient client;

    public Turbine(AmazonDynamoDBClient client)
    {
        this.client = client;
    }

    public void Dispose()
    {
        client.Dispose();
    }

    public IQueryBuilderPk<T> Query<T>(Schema schema)
    {
        return new QueryBuilderPk<T>(schema, client);
    }

    public IPutBuilderPk<T> Put<T>(Schema schema)
    {
        return new PutBuilderPk<T>(schema, client);
    }
}