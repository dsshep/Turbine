using System.Linq.Expressions;
using System.Reflection;
using Amazon.DynamoDBv2.DataModel;

namespace Turbine;

internal class CompoundKeySchema<T> : IKeySchema<T>
{
    private readonly string attributeName;
    private readonly Func<T, string> builder;

    public CompoundKeySchema(Func<T, string> builder, string attributeName)
    {
        this.builder = builder;
        this.attributeName = attributeName;
    }

    public string GetKey(T item)
    {
        return builder(item);
    }

    public bool IsNamedProperty(string name)
    {
        return name.Equals(attributeName, StringComparison.OrdinalIgnoreCase);
    }
}

internal class PropertyKeySchema<T> : IKeySchema<T>
{
    private readonly PropertyInfo property;

    public PropertyKeySchema(PropertyInfo property)
    {
        this.property = property;
    }

    public string GetKey(T item)
    {
        return property.GetValue(item)?.ToString()!;
    }

    public bool IsNamedProperty(string name)
    {
        return property.Name.Equals(name, StringComparison.OrdinalIgnoreCase);
    }
}

internal class AttributeKeySchema<T> : IKeySchema<T>
{
    private readonly Attribute attribute;
    private readonly PropertyInfo property;

    public AttributeKeySchema(PropertyInfo property, Attribute attribute)
    {
        this.property = property;
        this.attribute = attribute;
    }

    public string GetKey(T item)
    {
        return property.GetValue(item)?.ToString()!;
    }

    public bool IsNamedProperty(string name)
    {
        return property.Name.Equals(name, StringComparison.OrdinalIgnoreCase);
    }
}

internal interface IKeySchema<in T>
{
    string GetKey(T item);

    bool IsNamedProperty(string name);

    public static bool TryCreate(
        string schemaKey,
        Type attributeType,
        PropertyInfo[] properties,
        out IKeySchema<T> keySchema)
    {
        keySchema = default!;

        var (property, attribute) = properties
            .Select(p => (p, p.GetCustomAttribute(attributeType)))
            .SingleOrDefault(tup => tup.Item2 is not null);

        if ((property, attribute) != (default, default))
        {
            keySchema = new AttributeKeySchema<T>(property, attribute!);
            return true;
        }

        var keyProperty = properties.SingleOrDefault(p =>
            p.Name.Equals(schemaKey, StringComparison.OrdinalIgnoreCase) && p.PropertyType == typeof(string));

        if (keyProperty is not null)
        {
            keySchema = new PropertyKeySchema<T>(keyProperty);
            return true;
        }

        return false;
    }
}

public sealed class ItemSchema<T>
{
    private readonly Dictionary<string, (IKeySchema<T>, IKeySchema<T>)> gsis = new();
    private readonly StringComparison stringComparison;
    private string? jsonAttribute;
    private IKeySchema<T>? pkSchema;
    private IKeySchema<T>? skSchema;

    internal ItemSchema(TableSchema tableSchema, StringComparison stringComparison)
    {
        TableSchema = tableSchema;
        this.stringComparison = stringComparison;
        var properties = typeof(T).GetProperties();

        IKeySchema<T>.TryCreate(
            tableSchema.Pk,
            typeof(DynamoDBHashKeyAttribute),
            properties,
            out pkSchema);

        IKeySchema<T>.TryCreate(
            tableSchema.Sk,
            typeof(DynamoDBRangeKeyAttribute),
            properties,
            out skSchema);
    }

    internal TableSchema TableSchema { get; }

    public ItemSchema<T> PartitionKey(Func<T, string> pkBuilder)
    {
        pkSchema = new CompoundKeySchema<T>(pkBuilder, TableSchema.Sk);
        return this;
    }

    public ItemSchema<T> SortKey(Func<T, string> skBuilder)
    {
        skSchema = new CompoundKeySchema<T>(skBuilder, TableSchema.Sk);
        return this;
    }

    public ItemSchema<T> MapPk<TProperty>(Expression<Func<T, TProperty>> mapping)
    {
        var pkMappedProperty = Reflection.GetPropertyInfo(mapping);

        pkSchema = new PropertyKeySchema<T>(pkMappedProperty);

        return this;
    }

    public ItemSchema<T> MapSk<TProperty>(Expression<Func<T, TProperty>> mapping)
    {
        var skMappedProperty = Reflection.GetPropertyInfo(mapping);

        skSchema = new PropertyKeySchema<T>(skMappedProperty);

        return this;
    }

    public ItemSchema<T> MapGsi<TProperty1, TProperty2>(
        string index,
        Expression<Func<T, TProperty1>> gsiPkMapping,
        Expression<Func<T, TProperty2>> gsiSkMapping)
    {
        if (gsis.Count == 6)
        {
            throw new TurbineException("Can only map 6 GSIs.");
        }

        var propertyName = Reflection.GetPropertyInfo(gsiPkMapping);

        var pkKeySchema = new PropertyKeySchema<T>(propertyName);
        var skKeySchema = new PropertyKeySchema<T>(Reflection.GetPropertyInfo(gsiSkMapping));

        gsis.Add(index, (pkKeySchema, skKeySchema));

        return this;
    }

    public ItemSchema<T> ToJsonAttribute(string jsonAttributeName)
    {
        jsonAttribute = jsonAttributeName;
        return this;
    }

    internal string? GetPk(T entity)
    {
        return pkSchema?.GetKey(entity);
    }

    internal string? GetSk(T entity)
    {
        return skSchema?.GetKey(entity);
    }

    internal bool IsPkProperty(string name)
    {
        return pkSchema?.IsNamedProperty(name) ?? name == TableSchema.Pk;
    }

    internal bool IsSkProperty(string name)
    {
        return skSchema?.IsNamedProperty(name) ?? name == TableSchema.Sk;
    }

    internal string? GetGsiAttributeName(string name)
    {
        var matchingGsi =
            gsis.FirstOrDefault(kvp => kvp.Value.Item1.IsNamedProperty(name) || kvp.Value.Item2.IsNamedProperty(name));

        if (matchingGsi.Equals(default(KeyValuePair<string, (IKeySchema<T>, IKeySchema<T>)>)))
        {
            return default;
        }

        var gsi = TableSchema.GlobalSecondaryIndexes[matchingGsi.Key];

        var (pk, _) = matchingGsi.Value;

        return pk.IsNamedProperty(name) ? gsi.PkName : gsi.SkName;
    }

    internal bool IsJsonItem(out string jAttribute)
    {
        jAttribute = jsonAttribute!;
        return jsonAttribute is not null;
    }

    public ItemSchema<T> AddEntity()
    {
        var entitySchema = new ItemSchema<T>(TableSchema, stringComparison);
        return entitySchema;
    }
}