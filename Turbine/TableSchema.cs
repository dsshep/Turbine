namespace Turbine;

public sealed record GsiOptions
{
    internal GsiOptions(string index, int num)
    {
        Index = index;
        PkName = $"gsi{num}pk";
        SkName = $"gsi{num}sk";
    }

    public string Index { get; }

    public string PkName { get; set; }

    public string SkName { get; set; }
}

public sealed class TableSchema
{
    public TableSchema(string tableName)
    {
        TableName = tableName;
        Pk = "pk";
        Sk = "sk";
    }

    public TableSchema(string tableName, string pk, string sk)
    {
        TableName = tableName;
        Pk = pk;
        Sk = sk;
    }

    internal Dictionary<string, GsiOptions> GlobalSecondaryIndexes { get; } = new();

    public string TableName { get; }
    public string Pk { get; }
    public string Sk { get; }

    public ItemSchema<T> AddEntity<T>()
    {
        var schema = new ItemSchema<T>(this, StringComparison.OrdinalIgnoreCase);
        return schema;
    }

    public TableSchema AddGsi(string indexName, Action<GsiOptions>? options)
    {
        var o = new GsiOptions(indexName, GlobalSecondaryIndexes.Count + 1);

        options?.Invoke(o);

        GlobalSecondaryIndexes.Add(o.Index, o);

        return this;
    }
}