using System.Linq.Expressions;

namespace Turbine;

public class EntitySchema<T>
{
    private readonly Dictionary<Type, string> partitionMap;
    private readonly Dictionary<Type, string> sortMap;

    public EntitySchema(Schema schema, Dictionary<Type, string> partitionMap, Dictionary<Type, string> sortMap)
    {
        Schema = schema;
        this.partitionMap = partitionMap;
        this.sortMap = sortMap;
    }

    public Schema Schema { get; }

    public EntitySchema<T> PkMapping<TProperty>(Expression<Func<T, TProperty>> partitionKey)
    {
        partitionMap[typeof(T)] = Reflection.GetPropertyName(partitionKey);
        return this;
    }

    public EntitySchema<T> SkMapping<TProperty>(Expression<Func<T, TProperty>> partitionKey)
    {
        sortMap[typeof(T)] = Reflection.GetPropertyName(partitionKey);
        return this;
    }

    public EntitySchema<T> AddEntity()
    {
        return new EntitySchema<T>(Schema, partitionMap, sortMap);
    }
}

public sealed class Schema
{
    public Schema(string tableName)
    {
        TableName = tableName;
        Pk = "pk";
        Sk = "sk";
        PartitionMap = new Dictionary<Type, string>();
        SortMap = new Dictionary<Type, string>();
    }

    public Schema(string tableName, string pk, string sk)
    {
        TableName = tableName;
        Pk = pk;
        Sk = sk;
        PartitionMap = new Dictionary<Type, string>();
        SortMap = new Dictionary<Type, string>();
    }

    public string TableName { get; }
    public string Pk { get; }
    public string Sk { get; }
    public Dictionary<Type, string> PartitionMap { get; }
    public Dictionary<Type, string> SortMap { get; }

    public EntitySchema<T> AddEntity<T>()
    {
        return new EntitySchema<T>(this, PartitionMap, SortMap);
    }
}