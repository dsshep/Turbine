using Amazon.DynamoDBv2.Model;

namespace Turbine;

public class SortKey
{
    private SortKey(string keyExpr, AttributeValue attributeValue1, AttributeValue? attributeValue2 = null)
    {
        KeyExpr = keyExpr;
        AttributeValue1 = attributeValue1;
        AttributeValue2 = attributeValue2;
    }

    public string KeyExpr { get; }

    public AttributeValue AttributeValue1 { get; }

    public AttributeValue? AttributeValue2 { get; }

    public static SortKey Exactly(string value)
    {
        return new SortKey("<SORT_KEY> = :skVal", new AttributeValue(value));
    }

    public static SortKey GreaterThan(string value)
    {
        return new SortKey("<SORT_KEY> > :skVal", new AttributeValue(value));
    }

    public static SortKey GreaterThanOrEqual(string value)
    {
        return new SortKey("<SORT_KEY> >= :skVal", new AttributeValue(value));
    }

    public static SortKey LessThan(string value)
    {
        return new SortKey("<SORT_KEY> < :skVal", new AttributeValue(value));
    }

    public static SortKey LessThanOrEqual(string value)
    {
        return new SortKey("<SORT_KEY> <= :skVal", new AttributeValue(value));
    }

    public static SortKey BeginsWith(string value)
    {
        return new SortKey("begins_with(<SORT_KEY>, :skVal)", new AttributeValue(value));
    }

    public static SortKey Between(string value1, string value2)
    {
        return new SortKey("<SORT_KEY> BETWEEN :skVal1 AND :skVal2", new AttributeValue(value1),
            new AttributeValue(value2));
    }
}