using Amazon.DynamoDBv2.Model;

namespace Turbine;

internal interface IPageableQuery
{
    Task<QueryResponse> DoQuery(int? itemLimit, Dictionary<string, AttributeValue>? lastEvalKey = null);
}

public sealed class QueryList<T> : List<T>
{
    private readonly ItemSchema<T> itemSchema;
    private readonly int? pageSize;
    private readonly QueryResponse previousResponse;
    private readonly IPageableQuery query;

    internal QueryList(
        IEnumerable<T> items,
        int? pageSize,
        QueryResponse previousResponse,
        IPageableQuery query,
        ItemSchema<T> itemSchema)
    {
        AddRange(items);
        HasNextPage = previousResponse.LastEvaluatedKey != null;
        this.pageSize = pageSize;
        this.previousResponse = previousResponse;
        this.query = query;
        this.itemSchema = itemSchema;
    }

    public bool HasNextPage { get; }

    public async Task<QueryList<T>> NextPageAsync()
    {
        var response = await query.DoQuery(pageSize, previousResponse.LastEvaluatedKey);

        var entities = response.Items
            .Select(item => EntityBuilder.HydrateEntity<T>(itemSchema, item))
            .ToArray();

        return new QueryList<T>(entities, pageSize, response, query, itemSchema);
    }
}

public interface IQuery<T>
{
    Task<T?> FirstOrDefaultAsync();
    Task<QueryList<T>> ToListAsync();
    Task<QueryList<T>> ToListAsync(int? limit);
}

public interface IQueryBuilderSk<T>
{
    IQuery<T> WithSk(SortKey sortKey);
}

public interface IQueryBuilderPk<T>
{
    IQueryBuilderSk<T> WithPk(string value);
}

public interface ITransactPut<in T>
{
    ITurbineTransact Upsert(T entity);
    ITurbineTransact Upsert(T entity, Condition condition);
    ITurbineTransact Upsert(IEnumerable<T> entities);
    ITurbineTransact Upsert(IEnumerable<T> entities, Condition condition);
}

public interface IPut<in T>
{
    Task UpsertAsync(T entity);
    Task UpsertAsync(IEnumerable<T> entities);
    Task<bool> PutIfNotExistsAsync(T entity);
}

public interface ITransactDelete<in T>
{
    ITurbineTransact Delete(string pk, string sk);

    ITurbineTransact Delete(string pk, string sk, Condition condition);

    ITurbineTransact Delete(T item);

    ITurbineTransact Delete(T item, Condition condition);
}

public interface IDelete<in T>
{
    Task DeleteAsync(string pk, string sk);

    Task DeleteAsync(string pk, SortKey sk);

    Task DeleteAsync(T item);
}

public interface ITurbineEntitySchema<in T> : IPut<T>, IDelete<T>
{
}

public interface ITurbineTransactEntitySchema<in T> : ITransactPut<T>, ITransactDelete<T>
{
    ITurbineTransactEntitySchema<T> Condition(T item, Condition condition);
}

public interface ITurbineTransact : IAsyncDisposable
{
    ITurbineTransactEntitySchema<T> WithSchema<T>(ItemSchema<T> itemSchema);

    Task<bool> Commit();
}