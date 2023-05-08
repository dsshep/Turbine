using System.Linq.Expressions;
using System.Reflection;
using Amazon.DynamoDBv2.DataModel;

namespace Turbine;

public abstract class EntitySchema
{
    public abstract string GetPk(object entity);

    public abstract string GetSk(object entity);
}

public sealed class EntitySchema2<T> : EntitySchema
{
    private readonly (PropertyInfo, DynamoDBHashKeyAttribute)? hashKeyAttribute;
    private readonly PropertyInfo? pkProperty;
    private readonly (PropertyInfo, DynamoDBRangeKeyAttribute)? rangeKeyAttribute;
    private readonly Schema schema;
    private readonly PropertyInfo? skProperty;
    private readonly StringComparison stringComparison;

    private Dictionary<int, Func<T, string>?> gsiPk = new()
    {
        { 1, null },
        { 2, null },
        { 3, null },
        { 4, null },
        { 5, null },
        { 6, null }
    };

    private Dictionary<int, Func<T, string>?> gsiSk = new()
    {
        { 1, null },
        { 2, null },
        { 3, null },
        { 4, null },
        { 5, null },
        { 6, null }
    };

    private Func<T, string>? pkNameBuilder;
    private Func<T, string>? skNameBuilder;

    internal EntitySchema2(Schema schema, StringComparison stringComparison)
    {
        this.schema = schema;
        this.stringComparison = stringComparison;
        var properties = typeof(T).GetProperties();

        hashKeyAttribute = properties
            .Select(p => (p, (p.GetCustomAttribute(typeof(DynamoDBHashKeyAttribute)) as DynamoDBHashKeyAttribute)!))
            .SingleOrDefault(tup => tup.Item2 is not null);

        rangeKeyAttribute = properties
            .Select(p => (p, (p.GetCustomAttribute(typeof(DynamoDBRangeKeyAttribute)) as DynamoDBRangeKeyAttribute)!))
            .SingleOrDefault(tup => tup.Item2 is not null);

        pkProperty = properties.SingleOrDefault(p =>
            p.Name.Equals(schema.Pk, stringComparison) && p.PropertyType == typeof(string));

        skProperty = properties.SingleOrDefault(p =>
            p.Name.Equals(schema.Sk, stringComparison) && p.PropertyType == typeof(string));
    }

    public EntitySchema2<T> PartitionKey(Func<T, string> pkBuilder)
    {
        pkNameBuilder = pkBuilder;
        return this;
    }

    public EntitySchema2<T> SortKey(Func<T, string> skBuilder)
    {
        skNameBuilder = skBuilder;
        return this;
    }

    public override string GetPk(object entity)
    {
        return Get("Partition Key", entity, pkNameBuilder, hashKeyAttribute, pkProperty);
    }

    public override string GetSk(object entity)
    {
        return Get("Sort Key", entity, skNameBuilder, rangeKeyAttribute, skProperty);
    }

    private static string Get<TAttribute>(
        string fieldName,
        object entity,
        Func<T, string>? builder,
        (PropertyInfo, TAttribute)? attribute,
        PropertyInfo? property)
    {
        if (entity is not T typedEntity)
        {
            throw new TurbineException(
                $"Cannot convert type '{entity.GetType().Name}' to '{typeof(T).Name}'.");
        }

        if (builder is not null)
        {
            return builder(typedEntity);
        }

        if (attribute is not null)
        {
            var (prop, _) = attribute.Value;

            if (prop.GetValue(typedEntity) is not string pk)
            {
                throw new TurbineException(
                    $"Property '{prop.Name}' is annotated with '{typeof(TAttribute).Name}' but is not a string.");
            }

            return pk;
        }

        if (property is null)
        {
            throw new TurbineException(
                $"Cannot determine {fieldName} field for '{typeof(T).Name}'.");
        }

        return (property.GetValue(typedEntity) as string)!;
    }
}

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