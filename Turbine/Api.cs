using Amazon.DynamoDBv2.Model;

namespace Turbine;

internal interface IPageableQuery
{
    Task<QueryResponse> DoQuery(int? itemLimit, Dictionary<string, AttributeValue>? lastEvalKey = null);
}

public class QueryList<T> : List<T>
{
    private readonly EntitySchema<T> entitySchema;
    private readonly int? pageSize;
    private readonly QueryResponse previousResponse;
    private readonly IPageableQuery query;

    internal QueryList(
        IEnumerable<T> items,
        int? pageSize,
        QueryResponse previousResponse,
        IPageableQuery query,
        EntitySchema<T> entitySchema)
    {
        AddRange(items);
        HasNextPage = previousResponse.LastEvaluatedKey != null;
        this.pageSize = pageSize;
        this.previousResponse = previousResponse;
        this.query = query;
        this.entitySchema = entitySchema;
    }

    public bool HasNextPage { get; }

    public async Task<QueryList<T>> NextPageAsync()
    {
        var response = await query.DoQuery(pageSize, previousResponse.LastEvaluatedKey);

        var entities = response.Items
            .Select(item => EntityBuilder.HydrateEntity<T>(entitySchema, item))
            .ToArray();

        return new QueryList<T>(entities, pageSize, response, query, entitySchema);
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

public interface IPut<in T>
{
    Task UpsertAsync(T entity);
    Task UpsertAsync(IEnumerable<T> entities);
    Task<bool> PutIfNotExistsAsync(T entity);
}