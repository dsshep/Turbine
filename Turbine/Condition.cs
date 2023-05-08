using Amazon.DynamoDBv2.Model;

namespace Turbine;

public sealed record Condition
{
    private readonly Func<int, ConditionState> conditionBuilder;

    private Condition(Func<int, ConditionState> conditionBuilder)
    {
        this.conditionBuilder = conditionBuilder;
    }

    internal static Condition None { get; } = new(i => new ConditionState(i, "", null, null));

    public static Condition AttributeExists(string attributeName)
    {
        ArgumentNullException.ThrowIfNull(attributeName);

        return new Condition(i => new ConditionState(i, $"attribute_exists({attributeName})", null, null));
    }

    public static Condition AttributeNotExists(string attributeName)
    {
        ArgumentNullException.ThrowIfNull(attributeName);

        return new Condition(i => new ConditionState(i, $"attribute_not_exists({attributeName})", null, null));
    }

    public static Condition BeginsWith(string attributeName, string subStr)
    {
        ArgumentNullException.ThrowIfNull(attributeName);
        ArgumentNullException.ThrowIfNull(subStr);

        return StringCondition("begins_with", attributeName, subStr);
    }

    public static Condition Contains(string attributeName, string subStr)
    {
        ArgumentNullException.ThrowIfNull(attributeName);
        ArgumentNullException.ThrowIfNull(subStr);

        return StringCondition("contains", attributeName, subStr);
    }

    public static Condition Size(string attributeName)
    {
        ArgumentNullException.ThrowIfNull(attributeName);

        return new Condition(i => new ConditionState(i, $"size({attributeName})", null, null));
    }

    private static Condition StringCondition(string function, string attributeName, string subStr)
    {
        return new Condition(i =>
        {
            var key = $":v{i}";

            var attributeValues = new Dictionary<string, AttributeValue>
            {
                { key, new AttributeValue(subStr) }
            };

            return new ConditionState(i + 1, $"{function}({attributeName}, {key})", attributeValues, null);
        });
    }

    public Condition And(Condition right)
    {
        return new Condition(i =>
        {
            var (iNextL, conditionL, attributeValuesL, keyL) = Build(i);
            var (iNextR, conditionR, attributeValuesR, keyR) = right.Build(iNextL);

            var key = keyL is null ? keyL : keyR;

            var attributes = attributeValuesL?.Merge(attributeValuesR ?? new Dictionary<string, AttributeValue>());

            return new ConditionState(iNextR, $"{conditionL} AND {conditionR}", attributes, key);
        });
    }

    public Condition Or(Condition right)
    {
        return new Condition(i =>
        {
            var (iNextL, conditionL, attributeValuesL, keyL) = Build(i);
            var (iNextR, conditionR, attributeValuesR, keyR) = right.Build(iNextL);

            var key = keyL is null ? keyL : keyR;

            var attributes = attributeValuesL?.Merge(attributeValuesR ?? new Dictionary<string, AttributeValue>());

            return new ConditionState(iNextR, $"{conditionL} OR {conditionR}", attributes, key);
        });
    }

    public Condition Not()
    {
        return new Condition(i =>
        {
            var (iNext, condition, attributeValues, key) = Build(i);

            return new ConditionState(iNext, $"NOT {condition} ", attributeValues, key);
        });
    }

    public static Condition Equal<T>(string attributeName, T value)
    {
        return Comparison("=", attributeName, value);
    }

    public Condition EqualTo<T>(T value)
    {
        return ComparisonTo("=", value);
    }

    public static Condition NotEqual<T>(string attributeName, T value)
    {
        return Comparison("<>", attributeName, value);
    }

    public Condition NotEqualTo<T>(T value)
    {
        return ComparisonTo("<>", value);
    }

    public static Condition LessThan<T>(string attributeName, T value)
    {
        return Comparison("<", attributeName, value);
    }

    public Condition LessThanValue<T>(T value)
    {
        return ComparisonTo("<", value);
    }

    public static Condition LessThanOrEqual<T>(string attributeName, T value)
    {
        return Comparison("<=", attributeName, value);
    }

    public Condition LessThanOrEqualTo<T>(T value)
    {
        return ComparisonTo("<=", value);
    }

    public static Condition GreaterThan<T>(string attributeName, T value)
    {
        return Comparison(">", attributeName, value);
    }

    public Condition GreaterThanValue<T>(T value)
    {
        return ComparisonTo(">", value);
    }

    public static Condition GreaterThanOrEqual<T>(string attributeName, T value)
    {
        return Comparison(">=", attributeName, value);
    }

    public Condition GreaterThanOrEqualTo<T>(T value)
    {
        return ComparisonTo(">=", value);
    }

    private static Condition Comparison<T>(string op, string attributeName, T value)
    {
        ArgumentNullException.ThrowIfNull(value);

        return new Condition(i =>
        {
            var attrKey = $":v{i}";

            var attributeValues = new Dictionary<string, AttributeValue>
            {
                { attrKey, Reflection.ToAttributeValue(value) }
            };

            return new ConditionState(i + 1, $"{attributeName} {op} {attrKey}", attributeValues, null);
        });
    }

    private Condition ComparisonTo<T>(string op, T value)
    {
        ArgumentNullException.ThrowIfNull(value);

        return new Condition(i =>
        {
            var (iNext, condition, attributes, key) = Build(i);

            var attrKey = $":v{iNext}";

            var additionalAttributes = new Dictionary<string, AttributeValue>
            {
                { attrKey, Reflection.ToAttributeValue(value) }
            };

            var attributeValues = attributes is null
                ? additionalAttributes
                : attributes.Merge(additionalAttributes);

            return new ConditionState(iNext + 1, $"{condition} {op} {attrKey}", attributeValues, key);
        });
    }

    internal ConditionCheck? ToConditionCheck(string tableName)
    {
        if (this == None)
        {
            return null;
        }

        var (_, condExpr, attributes, key) = Build(1);

        var conditionCheck = new ConditionCheck
        {
            TableName = tableName,
            ConditionExpression = condExpr,
            ExpressionAttributeValues = attributes
        };

        return conditionCheck;
    }

    internal ConditionState Build(int start)
    {
        var (i, condition, attributes, key) = conditionBuilder(start);

        return new ConditionState(
            i,
            condition,
            attributes ?? new Dictionary<string, AttributeValue>(),
            key);
    }

    internal record struct ConditionState(
        int ValueIndex,
        string CondExpr,
        Dictionary<string, AttributeValue>? Attributes,
        (string, string)? Key);
}