using System.Linq.Expressions;
using Amazon.DynamoDBv2.Model;

namespace Turbine;

public static class Reflection
{
    public static string GetPropertyName<T, TProperty>(Expression<Func<T, TProperty>> expr)
    {
        if (expr.Body is MemberExpression memberExpr) return memberExpr.Member.Name;

        throw new TurbineException("Invalid expression: must be a property access expression");
    }

    public static object? ToNetType(Type t, AttributeValue av)
    {
        if (t == typeof(string)) return av.S;
        if (t == typeof(Guid)) return Guid.Parse(av.S);
        if (t == typeof(int)) return int.Parse(av.N);
        if (t == typeof(long)) return long.Parse(av.N);
        if (t == typeof(float)) return float.Parse(av.N);
        if (t == typeof(double)) return double.Parse(av.N);
        if (t == typeof(decimal)) return decimal.Parse(av.N);
        if (t == typeof(bool)) return bool.Parse(av.BOOL.ToString());
        if (t == typeof(DateTime)) return DateTime.Parse(av.S);
        if (t == typeof(TimeSpan)) return TimeSpan.Parse(av.S);
        if (t == typeof(byte[])) return av.B.ToArray();

        if (!t.IsGenericType || t.GetGenericTypeDefinition() != typeof(Nullable<>))
            throw new TurbineException($"Type '{t.Name}' not supported");

        var underlyingType = Nullable.GetUnderlyingType(t);
        return av.NULL || underlyingType is null ? null : ToNetType(underlyingType, av);
    }

    public static AttributeValue ToAttributeValue(object value)
    {
        var t = value.GetType();

        if (t == typeof(string)) return new AttributeValue { S = (string)value };
        if (t == typeof(Guid)) return new AttributeValue { S = value.ToString() };
        if (t == typeof(int)) return new AttributeValue { N = value.ToString() };
        if (t == typeof(long)) return new AttributeValue { N = value.ToString() };
        if (t == typeof(float)) return new AttributeValue { N = value.ToString() };
        if (t == typeof(double)) return new AttributeValue { N = value.ToString() };
        if (t == typeof(decimal)) return new AttributeValue { N = value.ToString() };
        if (t == typeof(bool)) return new AttributeValue { BOOL = (bool)value };
        if (t == typeof(DateTime)) return new AttributeValue { S = ((DateTime)value).ToString("o") };
        if (t == typeof(TimeSpan)) return new AttributeValue { S = ((TimeSpan)value).ToString() };
        if (t == typeof(byte[])) return new AttributeValue { B = new MemoryStream((byte[])value) };

        if (t.IsGenericType && t.GetGenericTypeDefinition() == typeof(Nullable<>))
            return new AttributeValue { NULL = true };

        throw new TurbineException($"Type '{t.Name}' not supported");
    }
}