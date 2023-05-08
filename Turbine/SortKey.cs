using Amazon.DynamoDBv2.Model;

namespace Turbine;

public sealed class SortKey
{
    private SortKey(bool isExactly, string keyExpr, AttributeValue attributeValue1,
        AttributeValue? attributeValue2 = null)
    {
        IsExactly = isExactly;
        KeyExpr = keyExpr;
        AttributeValue1 = attributeValue1;
        AttributeValue2 = attributeValue2;
    }

    internal bool IsExactly { get; }
    internal string KeyExpr { get; }

    internal AttributeValue AttributeValue1 { get; }

    internal AttributeValue? AttributeValue2 { get; }

    public static SortKey Exactly(string value)
    {
        return new SortKey(true, "<SORT_KEY> = :skVal", new AttributeValue(value));
    }

    public static SortKey GreaterThan(string value)
    {
        return new SortKey(false, "<SORT_KEY> > :skVal", new AttributeValue(value));
    }

    public static SortKey GreaterThanOrEqual(string value)
    {
        return new SortKey(false, "<SORT_KEY> >= :skVal", new AttributeValue(value));
    }

    public static SortKey LessThan(string value)
    {
        return new SortKey(false, "<SORT_KEY> < :skVal", new AttributeValue(value));
    }

    public static SortKey LessThanOrEqual(string value)
    {
        return new SortKey(false, "<SORT_KEY> <= :skVal", new AttributeValue(value));
    }

    public static SortKey BeginsWith(string value)
    {
        return new SortKey(false, "begins_with(<SORT_KEY>, :skVal)", new AttributeValue(value));
    }

    public static SortKey Between(string value1, string value2)
    {
        return new SortKey(false, "<SORT_KEY> BETWEEN :skVal1 AND :skVal2", new AttributeValue(value1),
            new AttributeValue(value2));
    }
}