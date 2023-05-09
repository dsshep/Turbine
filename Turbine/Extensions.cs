namespace Turbine;

internal static class Extensions
{
    public static KeyValuePair<string, TValue>? KeyValueOrDefault<TValue>(
        this IEnumerable<KeyValuePair<string, TValue>> keyValues,
        string? key)
    {
        var kvp = keyValues
            .FirstOrDefault(kvp =>
                kvp.Key.Equals(key, StringComparison.OrdinalIgnoreCase));

        if (kvp.Equals(default(KeyValuePair<string, TValue>)))
        {
            return null;
        }

        return kvp;
    }

    public static Dictionary<TKey, TValue> Merge<TKey, TValue>(
        this Dictionary<TKey, TValue> dict1,
        Dictionary<TKey, TValue> dict2)
        where TKey : notnull
    {
        var newDict = new Dictionary<TKey, TValue>(dict1.Count + dict2.Count);

        foreach (var kvp in dict1)
        {
            newDict.Add(kvp.Key, kvp.Value);
        }

        foreach (var kvp in dict2)
        {
            newDict.Add(kvp.Key, kvp.Value);
        }

        return newDict;
    }
}