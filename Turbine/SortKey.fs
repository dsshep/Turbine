namespace Turbine

open Amazon.DynamoDBv2.Model

type SortKey private (keyExpr: string, attributeValue1: AttributeValue, ?attributeValue2: AttributeValue) =

    member this.KeyExpr = keyExpr
    member this.AttributeValue1 = attributeValue1
    member this.AttributeValue2 = attributeValue2

    static member Exactly(value: string) =
        SortKey("<SORT_KEY> = :skVal", AttributeValue(value))

    static member GreaterThan(value: string) =
        SortKey("<SORT_KEY> > :skVal", AttributeValue(value))


    static member GreaterThanOrEqual(value: string) =
        SortKey("<SORT_KEY> >= :skVal", AttributeValue(value))

    static member LessThan(value: string) =
        SortKey("<SORT_KEY> < :skVal", AttributeValue(value))

    static member LessThanOrEqual(value: string) =
        SortKey("<SORT_KEY> <= :skVal", AttributeValue(value))

    static member BeginsWith(value: string) =
        SortKey("begins_with(<SORT_KEY>, :skVal)", AttributeValue(value))

    static member Between(value1: string, value2: string) =
        SortKey("<SORT_KEY> BETWEEN :skVal1 AND :skVal2", AttributeValue(value1), AttributeValue(value2))
