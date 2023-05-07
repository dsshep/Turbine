using System.Linq.Expressions;
using Amazon.DynamoDBv2.Model;

namespace Turbine;

internal static class Reflection
{
    private static readonly Dictionary<Type, Func<AttributeValue, object>> NetToDynamoLookup = new()
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

    private static readonly Dictionary<Type, Func<object, AttributeValue>> DynamoToNetLookup = new()
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

    public static string GetPropertyName<T, TProperty>(Expression<Func<T, TProperty>> expr)
    {
        if (expr.Body is MemberExpression memberExpr)
        {
            return memberExpr.Member.Name;
        }

        throw new TurbineException("Invalid expression: must be a property access expression");
    }

    public static object? ToNetType(Type t, AttributeValue av)
    {
        if (NetToDynamoLookup.TryGetValue(t, out var converter))
        {
            return converter(av);
        }

        if (Turbine.FromDynamoConverters.TryGetValue(t, out var customConverter))
        {
            return customConverter(av);
        }

        if (!t.IsGenericType || t.GetGenericTypeDefinition() != typeof(Nullable<>))
        {
            throw new TurbineException(
                $"Type '{t.Name}' not supported. If this is a custom type, use Turbine.FromDynamoConverters and Turbine.FromDynamoConverters to define the type conversion.");
        }

        var underlyingType = Nullable.GetUnderlyingType(t);
        return av.NULL || underlyingType is null ? null : ToNetType(underlyingType, av);
    }

    public static AttributeValue ToAttributeValue(object value)
    {
        var t = value.GetType();

        if (DynamoToNetLookup.TryGetValue(t, out var converter))
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

        throw new TurbineException(
            $"Type '{t.Name}' not supported. If this is a custom type, use Turbine.FromDynamoConverters and Turbine.FromDynamoConverters to define the type conversion.");
    }
}