namespace Turbine;

internal static class Extensions
{
    public static KeyValuePair<string, TValue>? KeyValueOrDefault<TValue>(
        this IEnumerable<KeyValuePair<string, TValue>> keyValues, string? key)
    {
        var kvp = keyValues
            .FirstOrDefault(kvp =>
                kvp.Key.Equals(key, StringComparison.OrdinalIgnoreCase));

        if (kvp.Equals(default(KeyValuePair<string, TValue>))) return null;

        return kvp;
    }
}