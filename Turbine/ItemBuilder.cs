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

        if (attribute is not null)
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
        var constructors = itemHelper.Constructors;

        var isJsonItem = itemSchema.IsJsonItem(out _);

        if (constructors.Any(c => c.GetParameters().Length == 0) || isJsonItem)
        {
            return (T)HydrateFromProps(attributes);
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

        throw new TurbineException($"Could not create instance of '{itemHelper.ItemName}'.", exception);
    }

    private object HydrateFromProps(IReadOnlyDictionary<string, AttributeValue> attributes)
    {
        var props = Array.FindAll(itemHelper.Properties, p => p.GetSetMethod(true) != null);

        var instance = (T)Activator.CreateInstance(itemHelper.ItemType)!;

        foreach (var prop in props)
        {
            var matchingAttribute = attributes.KeyValueOrDefault(prop.Name);

            if (matchingAttribute is not null)
            {
                prop.SetValue(instance,
                    Reflection.FromAttributeValue(prop.PropertyType, matchingAttribute.Value.Value));
            }
        }

        itemHelper.ApplyKeys(instance, attributes);

        return instance;
    }
}