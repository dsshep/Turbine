namespace Turbine

open System
open System.IO
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
        | _ -> raise (TurbineException($"Type '{t.Name}' not supported"))

    let toAttributeValue (value: obj) : AttributeValue =
        let t = value.GetType()

        match t with
        | _ when t = typeof<string> -> AttributeValue(S = string value)
        | _ when t = typeof<Guid> -> AttributeValue(S = value.ToString())
        | _ when t = typeof<int> -> AttributeValue(N = value.ToString())
        | _ when t = typeof<int64> -> AttributeValue(N = value.ToString())
        | _ when t = typeof<float> -> AttributeValue(N = value.ToString())
        | _ when t = typeof<double> -> AttributeValue(N = value.ToString())
        | _ when t = typeof<decimal> -> AttributeValue(N = value.ToString())
        | _ when t = typeof<bool> -> AttributeValue(BOOL = (unbox value))
        | _ when t = typeof<DateTime> -> AttributeValue(S = (unbox<DateTime> value).ToString("o"))
        | _ when t = typeof<TimeSpan> -> AttributeValue(S = (unbox<TimeSpan> value).ToString())
        | _ when t = typeof<byte[]> -> AttributeValue(B = new MemoryStream(unbox<byte[]> value))
        | _ when t = typeof<Nullable<_>> && obj.ReferenceEquals(value, null) -> AttributeValue(NULL = true)
        | _ -> raise (TurbineException($"Type '{t.Name}' not supported"))
