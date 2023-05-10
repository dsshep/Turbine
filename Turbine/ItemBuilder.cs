using System.Reflection;
using System.Text.Json;
using Amazon.DynamoDBv2.Model;

namespace Turbine;

internal interface IItemBuilder<out T>
{
    T HydrateEntity(IReadOnlyDictionary<string, AttributeValue> attributes);
}

internal class ItemBuilderHelper<T>
{
    private readonly ItemSchema<T> itemSchema;
    private readonly PropertyInfo? pkProperty;
    private readonly PropertyInfo? skProperty;

    public ItemBuilderHelper(ItemSchema<T> itemSchema)
    {
        this.itemSchema = itemSchema;
        ItemType = typeof(T);
        ItemName = ItemType.Name;
        Properties = ItemType.GetProperties();
        Constructors = ItemType.GetConstructors();

        pkProperty = Properties.SingleOrDefault(p => itemSchema.IsPkProperty(p.Name));
        skProperty = Properties.SingleOrDefault(p => itemSchema.IsSkProperty(p.Name));
    }

    public string ItemName { get; }
    public Type ItemType { get; }
    public PropertyInfo[] Properties { get; }
    public ConstructorInfo[] Constructors { get; }

    public void ApplyKeys(T instance, IReadOnlyDictionary<string, AttributeValue> attributes)
    {
        SetProperty(instance, pkProperty, attributes, itemSchema.TableSchema.Pk);
        SetProperty(instance, skProperty, attributes, itemSchema.TableSchema.Sk);
    }

    private static void SetProperty(
        T instance,
        PropertyInfo? property,
        IReadOnlyDictionary<string, AttributeValue> attributes,
        string name)
    {
        if (property is null)
        {
            return;
        }

        var attribute = attributes.KeyValueOrDefault(name);

        if (attribute is not null && property.GetSetMethod() is not null)
        {
            property.SetValue(
                instance,
                Reflection.FromAttributeValue(property.PropertyType, attribute.Value.Value));
        }
    }

    public IItemBuilder<T> GetBuilder()
    {
        if (itemSchema.IsJsonItem(out var json))
        {
            return new JsonItemBuilder<T>(json, this);
        }

        return new PropConstructorItemBuilder<T>(this, itemSchema);
    }
}

internal class JsonItemBuilder<T> : IItemBuilder<T>
{
    private readonly ItemBuilderHelper<T> itemHelper;
    private readonly string jsonAttribute;

    public JsonItemBuilder(string jsonAttribute, ItemBuilderHelper<T> itemHelper)
    {
        this.jsonAttribute = jsonAttribute;
        this.itemHelper = itemHelper;
    }

    public T HydrateEntity(IReadOnlyDictionary<string, AttributeValue> attributes)
    {
        var attribute = attributes.KeyValueOrDefault(jsonAttribute);

        if (attribute is null)
        {
            throw new TurbineException(
                $"JSON column defined for type '{itemHelper.ItemName}' but not found in the items attributes.");
        }

        var instance = (T)JsonSerializer.Deserialize(attribute.Value.Value.S, itemHelper.ItemType)!;

        itemHelper.ApplyKeys(instance, attributes);

        return instance;
    }
}

internal class PropConstructorItemBuilder<T> : IItemBuilder<T>
{
    private readonly ItemBuilderHelper<T> itemHelper;
    private readonly ItemSchema<T> itemSchema;

    public PropConstructorItemBuilder(ItemBuilderHelper<T> itemHelper, ItemSchema<T> itemSchema)
    {
        this.itemHelper = itemHelper;
        this.itemSchema = itemSchema;
    }

    public T HydrateEntity(IReadOnlyDictionary<string, AttributeValue> attributes)
    {
        return (T)InnerHydrate(typeof(T), itemHelper.Properties, itemHelper.Constructors, attributes, 0);
    }

    private object InnerHydrate(
        Type t,
        PropertyInfo[] props,
        IEnumerable<ConstructorInfo> ctors,
        IReadOnlyDictionary<string, AttributeValue> attributes,
        int depth)
    {
        if (depth == 32)
        {
            throw new TurbineException("Maximum nested object depth reached (32).");
        }

        var isJsonItem = itemSchema.IsJsonItem(out _);

        if (ctors.Any(c => c.GetParameters().Length == 0) || isJsonItem)
        {
            return HydrateFromProps(t, props, attributes, 0);
        }

        return HydrateFromConstructor(itemHelper.Constructors, attributes, 0);
    }

    private object HydrateFromConstructor(
        IEnumerable<ConstructorInfo> constructors,
        IReadOnlyDictionary<string, AttributeValue> attributes,
        int depth)
    {
        var instanceOpt =
            constructors
                .Select(c => (c, c.GetParameters()))
                .OrderByDescending(x => x.Item2.Length)
                .Select<(ConstructorInfo, ParameterInfo[]), (object?, Exception?)>(x =>
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

                            var gsiName = itemSchema.GetGsiAttributeName(p.Name);

                            if (matchingAttribute is null && gsiName is not null)
                            {
                                matchingAttribute = attributes.KeyValueOrDefault(gsiName);
                            }

                            if (matchingAttribute is null)
                            {
                                return p.ParameterType.IsValueType ? Activator.CreateInstance(p.ParameterType) : null;
                            }

                            var value = Reflection.FromAttributeValue(
                                p.ParameterType,
                                matchingAttribute.Value.Value);

                            if (value is null && matchingAttribute.Value.Value.M is not null)
                            {
                                var ctors = p.ParameterType.GetConstructors();
                                var props = p.ParameterType.GetProperties();

                                value = InnerHydrate(p.ParameterType, props, ctors, matchingAttribute.Value.Value.M,
                                    depth + 1);
                            }

                            return value;
                        })
                        .ToArray();

                    try
                    {
                        return (ctor.Invoke(args), null);
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

        throw new TurbineException($"Could not create instance of '{itemHelper.ItemName}'.", exception);
    }

    private object HydrateFromProps(Type t, PropertyInfo[] properties,
        IReadOnlyDictionary<string, AttributeValue> attributes, int depth)
    {
        var props = Array.FindAll(properties, p => p.GetSetMethod(true) != null);

        var instance = Activator.CreateInstance(t)!;

        foreach (var prop in props)
        {
            var matchingAttribute = attributes.KeyValueOrDefault(prop.Name);

            if (matchingAttribute is null)
            {
                continue;
            }

            var value = Reflection.FromAttributeValue(prop.PropertyType, matchingAttribute.Value.Value);

            if (value is null && matchingAttribute.Value.Value.M is not null)
            {
                var ctors = prop.PropertyType.GetConstructors();
                var p = prop.PropertyType.GetProperties();

                value = InnerHydrate(prop.PropertyType, p, ctors, matchingAttribute.Value.Value.M, depth + 1);
            }

            prop.SetValue(instance, value);
        }

        if (depth == 0 && instance is T rootItem)
        {
            itemHelper.ApplyKeys(rootItem, attributes);
        }

        return instance;
    }
}