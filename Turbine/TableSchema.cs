namespace Turbine;

public sealed class TableSchema
{
    internal readonly List<EntitySchema> EntitySchemas = new();

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

    public EntitySchema<T> AddEntity<T>()
    {
        var schema = new EntitySchema<T>(this, StringComparison.OrdinalIgnoreCase);
        EntitySchemas.Add(schema);
        return schema;
    }
}