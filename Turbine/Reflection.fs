namespace Turbine

open System
open System.Linq.Expressions
open Amazon.DynamoDBv2.Model

module internal Reflection =

    let getPropertyName<'T, 'TProperty> (expr: Expression<Func<'T, 'TProperty>>) =
        match expr.Body with
        | :? MemberExpression as memberExpr -> memberExpr.Member.Name
        | _ -> raise (InvalidOperationException("Invalid expression: must be a property access expression"))

    let rec toNetType (t: Type) (av: AttributeValue) =
        match t with
        | _ when t = typeof<string> -> av.S :> obj
        | _ when t = typeof<Guid> -> Guid.Parse(av.S) :> obj
        | _ when t = typeof<int> -> Int32.Parse(av.N) :> obj
        | _ when t = typeof<int64> -> Int64.Parse(av.N) :> obj
        | _ when t = typeof<float> -> Single.Parse(av.N) :> obj
        | _ when t = typeof<double> -> Double.Parse(av.N) :> obj
        | _ when t = typeof<decimal> -> Decimal.Parse(av.N) :> obj
        | _ when t = typeof<bool> -> bool.Parse(av.BOOL.ToString()) :> obj
        | _ when t = typeof<DateTime> -> DateTime.Parse(av.S) :> obj
        | _ when t = typeof<TimeSpan> -> TimeSpan.Parse(av.S) :> obj
        | _ when t = typeof<byte[]> -> av.B.ToArray() :> obj
        | _ when t.IsGenericType && t.GetGenericTypeDefinition() = typeof<Nullable<_>> ->
            let underlyingType = Nullable.GetUnderlyingType(t)
            if av.NULL then null else toNetType underlyingType av
        | _ -> raise (NotSupportedException($"Type '{t.Name}' not supported"))
