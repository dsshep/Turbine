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
            var matchingAttribute = attributes.KeyValueOrDefault(prop.Name);

            if (matchingAttribute is null &&
                prop.Name.Equals(partitionKeyProp, StringComparison.OrdinalIgnoreCase))
            {
                matchingAttribute = attributes.KeyValueOrDefault(schema.Pk);
            }

            if (matchingAttribute is null &&
                prop.Name.Equals(sortKeyProp, StringComparison.OrdinalIgnoreCase))
            {
                matchingAttribute = attributes.KeyValueOrDefault(schema.Sk);
            }

            if (matchingAttribute is not null)
            {
                prop.SetValue(instance, Reflection.ToNetType(prop.PropertyType, matchingAttribute.Value.Value));
            }
        }

        return instance;
    }

    public static T HydrateEntity<T>(Schema schema,
        IReadOnlyDictionary<string, AttributeValue> attributes)
    {
        var entityType = typeof(T);
        var constructors = entityType.GetConstructors();

        if (constructors.Any(c => c.GetParameters().Length == 0))
        {
            var instance = Activator.CreateInstance(entityType)!;
            return (T)HydrateFromProps(entityType, instance, attributes, schema);
        }

        var instanceOpt =
            constructors
                .Select(c => (c, c.GetParameters()))
                .OrderByDescending(x => x.Item2.Length)
                .Select<(ConstructorInfo, ParameterInfo[]), (T?, Exception?)>(x =>
                {
                    var ctor = x.Item1;
                    var parameters = x.Item2;

                    schema.PartitionMap.TryGetValue(entityType, out var partitionKeyProp);
                    schema.SortMap.TryGetValue(entityType, out var sortKeyProp);

                    var args = parameters
                        .Select(p =>
                        {
                            if (p.Name is null)
                            {
                                return null;
                            }

                            var matchingAttribute = attributes.KeyValueOrDefault(p.Name);

                            if (matchingAttribute is null &&
                                p.Name.Equals(partitionKeyProp, StringComparison.OrdinalIgnoreCase))
                            {
                                matchingAttribute = attributes.KeyValueOrDefault(schema.Pk);
                            }

                            if (matchingAttribute is null &&
                                p.Name.Equals(sortKeyProp, StringComparison.OrdinalIgnoreCase))
                            {
                                matchingAttribute = attributes.KeyValueOrDefault(schema.Sk);
                            }

                            if (matchingAttribute is not null)
                            {
                                return Reflection.ToNetType(p.ParameterType, matchingAttribute.Value.Value);
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
        {
            return instanceOrNull;
        }

        throw new TurbineException($"Could not create instance of '{entityType.Name}'.", exception);
    }
}