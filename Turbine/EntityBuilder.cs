using System.Reflection;
using Amazon.DynamoDBv2.Model;

namespace Turbine;

internal static class EntityBuilder
{
    private static object HydrateFromProps(
        Type t,
        object instance,
        IReadOnlyDictionary<string, AttributeValue> attributes,
        ItemSchema itemSchema)
    {
        var partitionKeyProp = itemSchema.GetPkProperty();
        var sortKeyProp = itemSchema.GetSkProperty();

        var props = Array.FindAll(t.GetProperties(), p => p.GetSetMethod(true) != null);

        foreach (var prop in props)
        {
            var matchingAttribute = attributes.KeyValueOrDefault(prop.Name);

            if (matchingAttribute is null && itemSchema.IsPkProperty(prop.Name))
            {
                matchingAttribute = attributes.KeyValueOrDefault(itemSchema.TableSchema.Pk);
            }

            if (matchingAttribute is null && itemSchema.IsSkProperty(prop.Name))
            {
                matchingAttribute = attributes.KeyValueOrDefault(itemSchema.TableSchema.Sk);
            }

            if (matchingAttribute is not null)
            {
                prop.SetValue(instance,
                    Reflection.FromAttributeValue(prop.PropertyType, matchingAttribute.Value.Value));
            }
        }

        return instance;
    }

    public static T HydrateEntity<T>(
        ItemSchema itemSchema,
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
                itemSchema);
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

                            if (matchingAttribute is null && itemSchema.IsPkProperty(p.Name))
                            {
                                matchingAttribute = attributes.KeyValueOrDefault(itemSchema.TableSchema.Pk);
                            }

                            if (matchingAttribute is null && itemSchema.IsSkProperty(p.Name))
                            {
                                matchingAttribute = attributes.KeyValueOrDefault(itemSchema.TableSchema.Sk);
                            }

                            if (matchingAttribute is not null)
                            {
                                return Reflection.FromAttributeValue(p.ParameterType, matchingAttribute.Value.Value);
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