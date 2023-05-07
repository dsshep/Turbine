namespace Turbine

open System
open Amazon.DynamoDBv2
open Turbine.Put
open Turbine.Query

type Turbine =
    val private client: AmazonDynamoDBClient

    new(client: AmazonDynamoDBClient) = { client = client }

    member this.Query<'T>(schema: Schema) : IQueryBuilderPk<'T> =
        QueryBuilderPk<'T>(schema, this.client) :> _

    member this.Put<'T>(schema: Schema) : IPutBuilderPk<'T> = PutBuilderPk<'T>(schema, this.client)

    interface IDisposable with
        member this.Dispose() = this.client.Dispose()
