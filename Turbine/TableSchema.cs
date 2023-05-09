namespace Turbine;

public sealed record GsiOptions
{
    internal GsiOptions(int num)
    {
        PkName = $"GSI{num}pk";
        SkName = $"GSI{num}sk";
    }

    public string? PkName { get; set; }

    public string? SkName { get; set; }
}

public sealed class TableSchema
{
    private readonly List<GsiOptions> globalSecondaryIndexes = new();

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

    public string TableName { get; }
    public string Pk { get; }
    public string Sk { get; }

    public ItemSchema<T> AddEntity<T>()
    {
        var schema = new ItemSchema<T>(this, StringComparison.OrdinalIgnoreCase);
        return schema;
    }

    public TableSchema AddGsi(Action<GsiOptions>? options)
    {
        var o = new GsiOptions(globalSecondaryIndexes.Count + 1);

        options?.Invoke(o);

        return this;
    }
}