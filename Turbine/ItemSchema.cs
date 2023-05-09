using System.Linq.Expressions;
using System.Reflection;
using Amazon.DynamoDBv2.DataModel;

namespace Turbine;

public abstract class ItemSchema
{
    internal abstract TableSchema TableSchema { get; }

    internal abstract string GetPk(object entity);

    internal abstract string GetSk(object entity);

    internal abstract PropertyInfo? GetPkProperty();

    internal abstract PropertyInfo? GetSkProperty();

    internal abstract bool IsPkProperty(string name);

    internal abstract bool IsSkProperty(string name);

    internal abstract bool IsJsonItem(out string jsonAttribute);
}

public sealed class ItemSchema<T> : ItemSchema
{
    private readonly (PropertyInfo, DynamoDBHashKeyAttribute)? hashKeyAttribute;
    private readonly PropertyInfo? pkProperty;
    private readonly PropertyInfo[] properties;
    private readonly (PropertyInfo, DynamoDBRangeKeyAttribute)? rangeKeyAttribute;

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

    private string? jsonAttribute;

    private PropertyInfo? pkMappedProperty;

    private Func<T, string>? pkNameBuilder;
    private PropertyInfo? skMappedProperty;
    private Func<T, string>? skNameBuilder;

    internal ItemSchema(TableSchema tableSchema, StringComparison stringComparison)
    {
        TableSchema = tableSchema;
        this.stringComparison = stringComparison;
        properties = typeof(T).GetProperties();

        hashKeyAttribute = properties
            .Select(p => (p, p.GetCustomAttribute(typeof(DynamoDBHashKeyAttribute)) as DynamoDBHashKeyAttribute))
            .SingleOrDefault(tup => tup.Item2 is not null)!;

        if (hashKeyAttribute == (default, default))
        {
            hashKeyAttribute = null;
        }

        rangeKeyAttribute = properties
            .Select(p => (p, p.GetCustomAttribute(typeof(DynamoDBRangeKeyAttribute)) as DynamoDBRangeKeyAttribute))
            .SingleOrDefault(tup => tup.Item2 is not null)!;

        if (rangeKeyAttribute == (default, default))
        {
            rangeKeyAttribute = null;
        }

        pkProperty = properties.SingleOrDefault(p =>
            p.Name.Equals(tableSchema.Pk, stringComparison) && p.PropertyType == typeof(string));

        skProperty = properties.SingleOrDefault(p =>
            p.Name.Equals(tableSchema.Sk, stringComparison) && p.PropertyType == typeof(string));
    }

    internal override TableSchema TableSchema { get; }

    public ItemSchema<T> PartitionKey(Func<T, string> pkBuilder)
    {
        pkNameBuilder = pkBuilder;
        return this;
    }

    public ItemSchema<T> SortKey(Func<T, string> skBuilder)
    {
        skNameBuilder = skBuilder;
        return this;
    }

    public ItemSchema<T> MapPk<TProperty>(Expression<Func<T, TProperty>> mapping)
    {
        var property = Reflection.GetPropertyName(mapping);

        pkMappedProperty = properties.Single(p => p.Name.Equals(property));

        return this;
    }

    public ItemSchema<T> MapSk<TProperty>(Expression<Func<T, TProperty>> mapping)
    {
        var property = Reflection.GetPropertyName(mapping);

        skMappedProperty = properties.Single(p => p.Name.Equals(property));

        return this;
    }

    public ItemSchema<T> ToJsonAttribute(string jsonAttributeName)
    {
        jsonAttribute = jsonAttributeName;
        return this;
    }

    internal override string GetPk(object entity)
    {
        return Get(
            "Partition Key",
            entity,
            pkNameBuilder,
            hashKeyAttribute,
            pkProperty,
            pkMappedProperty);
    }

    internal override string GetSk(object entity)
    {
        return Get(
            "Sort Key",
            entity,
            skNameBuilder,
            rangeKeyAttribute,
            skProperty,
            skMappedProperty);
    }

    internal override PropertyInfo? GetPkProperty()
    {
        if (pkProperty is not null)
        {
            return pkProperty;
        }

        if (hashKeyAttribute.HasValue)
        {
            return hashKeyAttribute.Value.Item1;
        }

        return pkMappedProperty ?? null;
    }

    internal override PropertyInfo? GetSkProperty()
    {
        if (skProperty is not null)
        {
            return skProperty;
        }

        if (rangeKeyAttribute.HasValue)
        {
            return rangeKeyAttribute.Value.Item1;
        }

        return skMappedProperty ?? null;
    }

    internal override bool IsPkProperty(string name)
    {
        if (pkProperty?.Name.Equals(name, stringComparison) ?? false)
        {
            return true;
        }

        if (hashKeyAttribute.HasValue && hashKeyAttribute.Value.Item1.Name.Equals(name, stringComparison))
        {
            return true;
        }

        return pkMappedProperty?.Name.Equals(name, stringComparison) ?? false;
    }

    internal override bool IsSkProperty(string name)
    {
        if (skProperty?.Name.Equals(name, stringComparison) ?? false)
        {
            return true;
        }

        if (rangeKeyAttribute.HasValue && rangeKeyAttribute.Value.Item1.Name.Equals(name, stringComparison))
        {
            return true;
        }

        return skMappedProperty?.Name.Equals(name, stringComparison) ?? false;
    }

    internal override bool IsJsonItem(out string jAttribute)
    {
        jAttribute = jsonAttribute!;
        return jsonAttribute is not null;
    }

    private static string Get<TAttribute>(
        string fieldName,
        object entity,
        Func<T, string>? builder,
        (PropertyInfo, TAttribute)? attribute,
        PropertyInfo? property,
        PropertyInfo? mappedProperty)
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

        if (attribute.HasValue)
        {
            var (prop, _) = attribute.Value;
            var value = prop.GetValue(typedEntity)?.ToString();

            if (value is null)
            {
                throw new TurbineException($"{fieldName} property cannot be null.");
            }

            return value;
        }

        if (property is not null)
        {
            return property.GetValue(typedEntity)?.ToString()!;
        }

        if (mappedProperty is not null)
        {
            return mappedProperty.GetValue(typedEntity)?.ToString()!;
        }

        throw new TurbineException(
            $"Cannot determine {fieldName} field for '{typeof(T).Name}'.");
    }

    public ItemSchema<T> AddEntity()
    {
        var entitySchema = new ItemSchema<T>(TableSchema, stringComparison);
        TableSchema.EntitySchemas.Add(typeof(T), entitySchema);
        return entitySchema;
    }
}