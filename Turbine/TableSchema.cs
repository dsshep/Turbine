namespace Turbine;

public sealed class TableSchema
{
    internal readonly Dictionary<Type, ItemSchema> EntitySchemas = new();

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
        EntitySchemas.Add(typeof(T), schema);
        return schema;
    }

    internal ItemSchema GetSchemaForType(Type t)
    {
        if (EntitySchemas.TryGetValue(t, out var s))
        {
            return s;
        }

        throw new TurbineException($@"Cannot find schema for type '{t.Name}'.");
    }
}