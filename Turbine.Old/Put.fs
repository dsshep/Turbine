namespace Turbine

open System.Collections.Generic
open System.Threading.Tasks
open Amazon.DynamoDBv2
open Amazon.DynamoDBv2.Model

module internal Put =

    [<Struct>]
    type PreparedSk<'T> =
        { Schema: Schema
          Client: AmazonDynamoDBClient
          PkFunc: 'T -> string
          SkFunc: 'T -> string }

    type Put<'T>(preparedSk: PreparedSk<'T>) =
        member private this.ConvertToAttributes(item: 'T) =
            let schema = preparedSk.Schema

            let attributes =
                ResizeArray
                    [ keyValue schema.Pk (AttributeValue(preparedSk.PkFunc(item)))
                      keyValue schema.Sk (AttributeValue(preparedSk.SkFunc(item))) ]

            let entityType = typeof<'T>
            let props = entityType.GetProperties()

            for prop in props do
                let shouldSkip =
                    match schema.PartitionMap.TryGetValue entityType with
                    | true, p -> p = prop.Name
                    | _, _ ->
                        match schema.SortMap.TryGetValue entityType with
                        | true, s -> s = prop.Name
                        | _, _ -> false

                if shouldSkip |> not then
                    let value = prop.GetValue(item)
                    attributes.Add(keyValue prop.Name (Reflection.toAttributeValue value))

            Dictionary<string, AttributeValue>(attributes)

        member private this.PrepareRequest(item: 'T) =
            let schema = preparedSk.Schema
            let putRequest = PutItemRequest(TableName = schema.TableName)

            let attributes = this.ConvertToAttributes item

            putRequest.Item <- attributes

            putRequest

        interface IPut<'T> with
            member this.PutIfNotExists(item: 'T) =
                task {
                    let schema = preparedSk.Schema
                    let client = preparedSk.Client
                    let putRequest = this.PrepareRequest(item)

                    putRequest.ConditionExpression <-
                        $"attribute_not_exists({schema.Pk}) AND attribute_not_exists({schema.Sk})"

                    try
                        let! _ = client.PutItemAsync(putRequest)
                        return true
                    with :? ConditionalCheckFailedException ->
                        return false
                }

            member this.UpsertAsync<'T>(item: 'T) : Task =
                task {
                    let client = preparedSk.Client
                    let putRequest = this.PrepareRequest(item)

                    let! _ = client.PutItemAsync(putRequest)
                    return ()
                }

            member this.UpsertAsync<'T>(items: seq<'T>) : Task =
                task {
                    let schema = preparedSk.Schema
                    let client = preparedSk.Client

                    let putRequests =
                        items
                        |> Seq.map (fun i -> WriteRequest(PutRequest = PutRequest(this.ConvertToAttributes i)))
                        |> Seq.chunkBySize 25
                        |> Seq.map (fun chunk -> ResizeArray chunk)
                        |> Seq.toArray

                    let batchWriteRequest = BatchWriteItemRequest()
                    batchWriteRequest.RequestItems <- Dictionary<string, List<WriteRequest>>()

                    for batchRequest in putRequests do
                        batchWriteRequest.RequestItems[schema.TableName] <- batchRequest
                        let! _ = client.BatchWriteItemAsync(batchWriteRequest)
                        ()
                }

    [<Struct>]
    type PreparedPk<'T> =
        { Schema: Schema
          Client: AmazonDynamoDBClient
          PkFunc: 'T -> string }

    type PutBuilderSk<'T>(preparedPk: PreparedPk<'T>) =
        interface IPutBuilderSk<'T> with
            member this.WithSk(value: string) : IPut<'T> =
                Put<'T>(
                    { Schema = preparedPk.Schema
                      Client = preparedPk.Client
                      PkFunc = preparedPk.PkFunc
                      SkFunc = (fun _ -> value) }
                )

            member this.WithSk(skFunc: 'T -> string) : IPut<'T> =
                Put<'T>(
                    { Schema = preparedPk.Schema
                      Client = preparedPk.Client
                      PkFunc = preparedPk.PkFunc
                      SkFunc = skFunc }
                )

    type PutBuilderPk<'T>(schema: Schema, client: AmazonDynamoDBClient) =
        interface IPutBuilderPk<'T> with
            member this.WithPk(value: string) : IPutBuilderSk<'T> =
                PutBuilderSk(
                    { Schema = schema
                      Client = client
                      PkFunc = (fun _ -> value) }
                )

            member this.WithPk(pkFunc: 'T -> string) : IPutBuilderSk<'T> =
                PutBuilderSk(
                    { Schema = schema
                      Client = client
                      PkFunc = pkFunc }
                )
