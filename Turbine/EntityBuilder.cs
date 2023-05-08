using System.Reflection;
using Amazon.DynamoDBv2.Model;

namespace Turbine;

internal static class EntityBuilder
{
    private static object HydrateFromProps(
        Type t,
        object instance,
        IReadOnlyDictionary<string, AttributeValue> attributes,
        EntitySchema entitySchema)
    {
        var partitionKeyProp = entitySchema.GetPkProperty();
        var sortKeyProp = entitySchema.GetSkProperty();

        var props = Array.FindAll(t.GetProperties(), p => p.GetSetMethod(true) != null);

        foreach (var prop in props)
        {
            var matchingAttribute = attributes.KeyValueOrDefault(prop.Name);

            if (matchingAttribute is null && entitySchema.IsPkProperty(prop.Name))
            {
                matchingAttribute = attributes.KeyValueOrDefault(entitySchema.TableSchema.Pk);
            }

            if (matchingAttribute is null && entitySchema.IsSkProperty(prop.Name))
            {
                matchingAttribute = attributes.KeyValueOrDefault(entitySchema.TableSchema.Sk);
            }

            if (matchingAttribute is not null)
            {
                prop.SetValue(instance, Reflection.ToNetType(prop.PropertyType, matchingAttribute.Value.Value));
            }
        }

        return instance;
    }

    public static T HydrateEntity<T>(
        EntitySchema entitySchema,
        IReadOnlyDictionary<string, AttributeValue> attributes)
    {
        var entityType = typeof(T);
        var constructors = entityType.GetConstructors();

        if (constructors.Any(c => c.GetParameters().Length == 0))
        {
            return (T)HydrateFromProps(
                entityType,
                Activator.CreateInstance(entityType)!,
                attributes,
                entitySchema);
        }

        var instanceOpt =
            constructors
                .Select(c => (c, c.GetParameters()))
                .OrderByDescending(x => x.Item2.Length)
                .Select<(ConstructorInfo, ParameterInfo[]), (T?, Exception?)>(x =>
                {
                    var ctor = x.Item1;
                    var parameters = x.Item2;

                    var args = parameters
                        .Select(p =>
                        {
                            if (p.Name is null)
                            {
                                return null;
                            }

                            var matchingAttribute = attributes.KeyValueOrDefault(p.Name);

                            if (matchingAttribute is null && entitySchema.IsPkProperty(p.Name))
                            {
                                matchingAttribute = attributes.KeyValueOrDefault(entitySchema.TableSchema.Pk);
                            }

                            if (matchingAttribute is null && entitySchema.IsSkProperty(p.Name))
                            {
                                matchingAttribute = attributes.KeyValueOrDefault(entitySchema.TableSchema.Sk);
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

        var (instance, exception) = instanceOpt;

        if (instance is not null)
        {
            return instance;
        }

        throw new TurbineException($"Could not create instance of '{entityType.Name}'.", exception);
    }
}