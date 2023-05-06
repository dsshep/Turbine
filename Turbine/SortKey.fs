namespace Turbine

open Amazon.DynamoDBv2.Model

type SortKey private (keyExpr: string, attributeValue: AttributeValue) =

    member this.KeyExpr = keyExpr
    member this.AttributeValue = attributeValue

    static member Exactly(value: string) =
        SortKey("<SORT_KEY> = :skVal", AttributeValue(value))
