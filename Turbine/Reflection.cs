using System.Collections;
using System.Linq.Expressions;
using Amazon.DynamoDBv2.Model;

namespace Turbine;

internal static class Reflection
{
    private static readonly Dictionary<Type, Func<AttributeValue, object>> DynamoToNetLookup = new()
    {
        { typeof(string), static av => av.S },
        { typeof(Guid), static av => Guid.Parse(av.S) },
        { typeof(int), static av => int.Parse(av.N) },
        { typeof(long), static av => long.Parse(av.N) },
        { typeof(float), static av => float.Parse(av.N) },
        { typeof(double), static av => double.Parse(av.N) },
        { typeof(decimal), static av => decimal.Parse(av.N) },
        { typeof(bool), static av => bool.Parse(av.BOOL.ToString()) },
        { typeof(DateTime), static av => DateTime.Parse(av.S) },
        { typeof(DateTimeOffset), static av => DateTimeOffset.Parse(av.S) },
        { typeof(DateOnly), static av => DateOnly.Parse(av.S) },
        { typeof(TimeOnly), static av => TimeOnly.Parse(av.S) },
        { typeof(TimeSpan), static av => TimeSpan.FromTicks(long.Parse(av.N)) },
        { typeof(byte[]), static av => av.B.ToArray() }
    };

    private static readonly Dictionary<Type, Func<object, AttributeValue>> NetToDynamoLookup = new()
    {
        { typeof(string), static value => new AttributeValue { S = value.ToString() } },
        { typeof(Guid), static value => new AttributeValue { S = value.ToString() } },
        { typeof(int), static value => new AttributeValue { N = value.ToString() } },
        { typeof(long), static value => new AttributeValue { N = value.ToString() } },
        { typeof(float), static value => new AttributeValue { N = value.ToString() } },
        { typeof(double), static value => new AttributeValue { N = value.ToString() } },
        { typeof(decimal), static value => new AttributeValue { N = value.ToString() } },
        { typeof(bool), static value => new AttributeValue { BOOL = (bool)value } },
        { typeof(DateTime), static value => new AttributeValue { S = ((DateTime)value).ToString("o") } },
        { typeof(DateTimeOffset), static value => new AttributeValue { S = ((DateTimeOffset)value).ToString("o") } },
        { typeof(DateOnly), static value => new AttributeValue { S = ((DateOnly)value).ToString("o") } },
        { typeof(TimeOnly), static value => new AttributeValue { S = ((TimeOnly)value).ToString("o") } },
        { typeof(TimeSpan), static value => new AttributeValue { N = ((TimeSpan)value).Ticks.ToString() } },
        { typeof(byte[]), static value => new AttributeValue { B = new MemoryStream((byte[])value) } }
    };

    private static bool IsEnumerable(Type t)
    {
        foreach (var interfaceType in t.GetInterfaces())
        {
            if (interfaceType.IsGenericType && interfaceType.GetGenericTypeDefinition() == typeof(IEnumerable<>))
            {
                return true;
            }

            if (interfaceType == typeof(IEnumerable))
            {
                return true;
            }
        }

        return false;
    }

    private static bool IsDictionary(Type t)
    {
        var genericDef = t.GetGenericTypeDefinition();

        if (genericDef == typeof(IDictionary<,>)
            || genericDef == typeof(IReadOnlyDictionary<,>)
            || t == typeof(IDictionary))
        {
            return true;
        }

        foreach (var interfaceType in t.GetInterfaces())
        {
            if (interfaceType.IsGenericType
                && (interfaceType.GetGenericTypeDefinition() == typeof(IDictionary<,>)
                    || interfaceType.GetGenericTypeDefinition() == typeof(IReadOnlyDictionary<,>)))
            {
                return true;
            }

            if (interfaceType == typeof(IDictionary))
            {
                return true;
            }
        }

        return false;
    }

    public static string GetPropertyName<T, TProperty>(Expression<Func<T, TProperty>> expr)
    {
        if (expr.Body is MemberExpression memberExpr)
        {
            return memberExpr.Member.Name;
        }

        throw new TurbineException("Invalid expression: must be a property access expression.");
    }

    public static object? FromAttributeValue(Type t, AttributeValue av)
    {
        if (DynamoToNetLookup.TryGetValue(t, out var converter))
        {
            return converter(av);
        }

        if (Turbine.FromDynamoConverters.TryGetValue(t, out var customConverter))
        {
            return customConverter(av);
        }

        if (t.IsGenericType && t.GetGenericTypeDefinition() == typeof(Nullable<>))
        {
            var underlyingType = Nullable.GetUnderlyingType(t);
            return av.NULL || underlyingType is null ? null : FromAttributeValue(underlyingType, av);
        }

        if (IsDictionary(t))
        {
            var valueType = t.GetGenericArguments()[1];
            var dictType = typeof(Dictionary<,>).MakeGenericType(typeof(string), valueType);

            if (Activator.CreateInstance(dictType) is IDictionary returnDict)
            {
                foreach (var (key, value) in av.M)
                {
                    returnDict[key] = FromAttributeValue(valueType, value)!;
                }

                return returnDict;
            }
        }

        if (IsEnumerable(t))
        {
            var listItemType = t.GetGenericArguments()[0];
            var listType = typeof(List<>).MakeGenericType(listItemType);

            if (Activator.CreateInstance(listType) is IList returnList)
            {
                foreach (var attribute in av.L)
                {
                    returnList.Add(FromAttributeValue(listItemType, attribute)!);
                }

                return returnList;
            }
        }

        throw new TurbineException(
            $"Type '{t.Name}' not supported. If this is a custom type, use Turbine.FromDynamoConverters and Turbine.FromDynamoConverters to define the type conversion.");
    }

    public static AttributeValue ToAttributeValue(object value)
    {
        var t = value.GetType();

        if (NetToDynamoLookup.TryGetValue(t, out var converter))
        {
            return converter(value);
        }

        if (t.IsGenericType && t.GetGenericTypeDefinition() == typeof(Nullable<>))
        {
            return new AttributeValue { NULL = true };
        }

        if (Turbine.ToDynamoConverters.TryGetValue(t, out var customConverter))
        {
            return customConverter(value);
        }

        if (IsDictionary(t))
        {
            var dictionary = (IDictionary)value;
            var attributeValues = new Dictionary<string, AttributeValue>();

            foreach (DictionaryEntry entry in dictionary)
            {
                attributeValues[entry.Key.ToString()!] = ToAttributeValue(entry.Value!);
            }

            return new AttributeValue
            {
                M = attributeValues
            };
        }

        if (IsEnumerable(t))
        {
            var enumerable = (IEnumerable)value;

            var attributeValues = enumerable.Cast<object>().Select(ToAttributeValue).ToList();

            return new AttributeValue
            {
                L = attributeValues
            };
        }

        throw new TurbineException(
            $"Type '{t.Name}' not supported. If this is a custom type, use Turbine.FromDynamoConverters and Turbine.FromDynamoConverters to define the type conversion.");
    }
}