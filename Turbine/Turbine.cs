using Amazon.DynamoDBv2;
using Amazon.DynamoDBv2.Model;

namespace Turbine;

public sealed class Turbine : IDisposable
{
    public static readonly IDictionary<Type, Func<AttributeValue, object?>> FromDynamoConverters =
        new Dictionary<Type, Func<AttributeValue, object?>>();

    public static readonly IDictionary<Type, Func<object?, AttributeValue>> ToDynamoConverters =
        new Dictionary<Type, Func<object?, AttributeValue>>();

    private readonly IAmazonDynamoDB client;

    public Turbine(IAmazonDynamoDB client)
    {
        ArgumentNullException.ThrowIfNull(client);
        this.client = client;
    }

    public void Dispose()
    {
        client.Dispose();
    }

    public IQueryBuilderPk<T> Query<T>(EntitySchema<T> entitySchema)
    {
        ArgumentNullException.ThrowIfNull(entitySchema);
        return new QueryBuilderPk<T>(entitySchema, client);
    }

    public ITurbineEntitySchema<T> WithSchema<T>(EntitySchema<T> entitySchema)
    {
        ArgumentNullException.ThrowIfNull(entitySchema);

        return new TurbineEntitySchema<T>(entitySchema, client);
    }

    private sealed class TurbineEntitySchema<T> : ITurbineEntitySchema<T>
    {
        private readonly IAmazonDynamoDB client;
        private readonly EntitySchema<T> entitySchema;

        public TurbineEntitySchema(EntitySchema<T> entitySchema, IAmazonDynamoDB client)
        {
            this.entitySchema = entitySchema;
            this.client = client;
        }

        public Task UpsertAsync(T entity)
        {
            return new Put<T>(entitySchema, client).UpsertAsync(entity);
        }

        public Task UpsertAsync(IEnumerable<T> entities)
        {
            return new Put<T>(entitySchema, client).UpsertAsync(entities);
        }

        public Task<bool> PutIfNotExistsAsync(T entity)
        {
            return new Put<T>(entitySchema, client).PutIfNotExistsAsync(entity);
        }

        public Task DeleteAsync(string pk, string sk)
        {
            return new Delete<T>(entitySchema, new QueryBuilderPk<T>(entitySchema, client), client).DeleteAsync(pk, sk);
        }

        public Task DeleteAsync(string pk, SortKey sk)
        {
            return new Delete<T>(entitySchema, new QueryBuilderPk<T>(entitySchema, client), client).DeleteAsync(pk, sk);
        }

        public Task DeleteAsync(T item)
        {
            return new Delete<T>(entitySchema, new QueryBuilderPk<T>(entitySchema, client), client).DeleteAsync(item);
        }
    }
}