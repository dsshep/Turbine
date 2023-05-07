using Amazon.DynamoDBv2;

namespace Turbine;

public sealed class Turbine : IDisposable
{
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