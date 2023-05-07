using System.Reflection;
using Amazon.DynamoDBv2.Model;

namespace Turbine;

internal static class EntityBuilder
{
    private static object HydrateFromProps(
        Type t,
        object instance,
        IReadOnlyDictionary<string, AttributeValue> attributes,
        Schema schema)
    {
        var partitionKeyProp = schema.PartitionMap.TryGetValue(t, out var partitionValue) ? partitionValue : schema.Pk;
        var sortKeyProp = schema.SortMap.TryGetValue(t, out var sortValue) ? sortValue : schema.Sk;

        var props = Array.FindAll(t.GetProperties(), p => p.GetSetMethod(true) != null);

        foreach (var prop in props)
        {
            var propName = prop.Name;
            var propType = prop.PropertyType;

            if (attributes.TryGetValue(propName, out var av))
            {
                var value = Reflection.ToNetType(propType, av);
                prop.SetValue(instance, value);
            }
            else
            {
                if (string.Equals(propName, partitionKeyProp, StringComparison.OrdinalIgnoreCase))
                    if (attributes.TryGetValue(schema.Pk, out av))
                    {
                        var value = Reflection.ToNetType(propType, av);
                        prop.SetValue(instance, value);
                    }

                if (!string.Equals(propName, sortKeyProp, StringComparison.OrdinalIgnoreCase)) continue;
                {
                    if (!attributes.TryGetValue(schema.Sk, out av)) continue;
                    var value = Reflection.ToNetType(propType, av);
                    prop.SetValue(instance, value);
                }
            }
        }

        return instance;
    }

    public static T HydrateEntity<T>(Schema schema,
        IReadOnlyDictionary<string, AttributeValue> attributes)
    {
        var entityType = typeof(T);
        var ctors = entityType.GetConstructors();

        if (Array.Exists(ctors, c => c.GetParameters().Length == 0))
        {
            var instance = Activator.CreateInstance(entityType)!;
            return (T)HydrateFromProps(entityType, instance, attributes, schema);
        }

        var instanceOpt =
            ctors
                .Select(c => (c, c.GetParameters()))
                .OrderByDescending(x => x.Item2.Length)
                .Select<(ConstructorInfo, ParameterInfo[]), (T?, Exception?)>(x =>
                {
                    var ctor = x.Item1;
                    var parameters = x.Item2;

                    var args = parameters
                        .Select(p =>
                        {
                            if (p.Name is null) return null;

                            if (attributes.TryGetValue(p.Name, out var attribute))
                                return Reflection.ToNetType(p.ParameterType, attribute);

                            var partitionKeyProp = schema.PartitionMap.TryGetValue(entityType, out var partitionValue)
                                ? partitionValue
                                : schema.Pk;
                            var sortKeyProp = schema.SortMap.TryGetValue(entityType, out var sortValue)
                                ? sortValue
                                : schema.Sk;

                            if (string.Equals(p.Name, partitionKeyProp, StringComparison.OrdinalIgnoreCase))
                            {
                                if (attributes.TryGetValue(schema.Pk, out attribute))
                                    return Reflection.ToNetType(p.ParameterType, attribute);
                            }
                            else if (string.Equals(p.Name, sortKeyProp, StringComparison.OrdinalIgnoreCase))
                            {
                                if (attributes.TryGetValue(schema.Sk, out attribute))
                                    return Reflection.ToNetType(p.ParameterType, attribute);
                            }

                            return p.ParameterType.IsValueType ? Activator.CreateInstance(p.ParameterType) : null;
                        })
                        .ToArray();

                    try
                    {
                        return ((T)ctor.Invoke(args), null);
                    }
                    catch (Exception e)
                    {
                        return (default, e);
                    }
                })
                .FirstOrDefault();

        var (instanceOrNull, exception) = instanceOpt;

        if (instanceOrNull != null)
            return instanceOrNull;

        throw new TurbineException($"Could not create instance of {entityType.Name}.", exception);
    }
}