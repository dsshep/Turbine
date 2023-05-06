namespace Turbine

open System
open Amazon.DynamoDBv2
open Turbine.Query

type Turbine =
    val client: AmazonDynamoDBClient

    new(client: AmazonDynamoDBClient) = { client = client }

    member this.Query<'T>(schema: Schema) : IQueryBuilderPk<'T> =
        QueryBuilderPk<'T>(schema, this.client) :> _

    interface IDisposable with
        member this.Dispose() = this.client.Dispose()
