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

    public IQueryBuilderPk<T> Query<T>(EntitySchema<T> entitySchema)
    {
        return new QueryBuilderPk<T>(entitySchema, client);
    }

    public IPut<T> Put<T>(EntitySchema<T> schema)
    {
        return new Put<T>(schema, client);
    }
}