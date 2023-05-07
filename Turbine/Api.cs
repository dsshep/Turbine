using Amazon.DynamoDBv2.Model;

namespace Turbine;

internal interface IPageableQuery
{
    Task<QueryResponse> DoQuery(int? itemLimit, Dictionary<string, AttributeValue>? lastEvalKey = null);
}

public class QueryList<T> : List<T>
{
    private readonly int? pageSize;
    private readonly QueryResponse previousResponse;
    private readonly IPageableQuery query;
    private readonly Schema schema;

    internal QueryList(IEnumerable<T> items, int? pageSize, QueryResponse previousResponse, IPageableQuery query,
        Schema schema)
    {
        AddRange(items);
        HasNextPage = previousResponse.LastEvaluatedKey != null;
        this.pageSize = pageSize;
        this.previousResponse = previousResponse;
        this.query = query;
        this.schema = schema;
    }

    public bool HasNextPage { get; }

    public async Task<QueryList<T>> NextPageAsync()
    {
        var response = await query.DoQuery(pageSize, previousResponse.LastEvaluatedKey);

        var entities = response.Items
            .Select(item => EntityBuilder.HydrateEntity<T>(schema, item))
            .ToArray();

        return new QueryList<T>(entities!, pageSize, response, query, schema);
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

public interface IPut<T>
{
    Task UpsertAsync(T entity);
    Task UpsertAsync(IEnumerable<T> entities);
    Task<bool> PutIfNotExists(T entity);
}

public interface IPutBuilderSk<T>
{
    IPut<T> WithSk(string value);
    IPut<T> WithSk(Func<T, string> valueFunc);
}

public interface IPutBuilderPk<T>
{
    IPutBuilderSk<T> WithPk(string value);
    IPutBuilderSk<T> WithPk(Func<T, string> valueFunc);
}