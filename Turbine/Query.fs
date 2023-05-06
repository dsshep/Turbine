namespace Turbine

open System
open System.Collections.Generic
open Amazon.DynamoDBv2
open Amazon.DynamoDBv2.Model

module internal Query =

    [<Struct>]
    type PreparedQuery =
        { Schema: Schema
          Pk: string
          Sk: SortKey }

    type Query<'T> =
        val query: PreparedQuery
        val client: AmazonDynamoDBClient

        new(query: PreparedQuery, client: AmazonDynamoDBClient) = { query = query; client = client }

        member this.DoQuery(itemLimit: Nullable<int>) =
            task {
                let { Pk = pk; Sk = sk; Schema = schema } = this.query
                let sortKeyExpr = sk.KeyExpr.Replace("<SORT_KEY>", schema.Sk)

                let queryRequest = QueryRequest(TableName = schema.TableName)
                queryRequest.KeyConditionExpression <- $"{this.query.Schema.Pk} = :pkVal AND {sortKeyExpr}"

                let expressionAttributes =
                    match sk.AttributeValue2 with
                    | Some a ->
                        [ KeyValuePair<_, _>(":pkVal", AttributeValue(pk))
                          KeyValuePair<_, _>(":skVal1", sk.AttributeValue1)
                          KeyValuePair<_, _>(":skVal2", a) ]
                    | None ->
                        [ KeyValuePair<_, _>(":pkVal", AttributeValue(pk))
                          KeyValuePair<_, _>(":skVal", sk.AttributeValue1) ]

                queryRequest.ExpressionAttributeValues <- Dictionary<string, AttributeValue>(expressionAttributes)

                if itemLimit.HasValue then
                    queryRequest.Limit <- itemLimit.Value

                let! result = this.client.QueryAsync(queryRequest)

                return result.Items
            }

        interface IQuery<'T> with
            member this.QueryAsync() =
                task {
                    let! items = this.DoQuery(1)

                    return
                        if items.Count = 1 then
                            EntityBuilder.hydrateEntity<'T> (this.query.Schema, items[0])
                        else
                            Unchecked.defaultof<_>
                }

            member this.ToListAsync() =
                task {
                    let! items = this.DoQuery(Nullable())

                    return
                        items
                        |> Seq.map (fun item -> EntityBuilder.hydrateEntity<'T> (this.query.Schema, item))
                        |> ResizeArray
                }

    [<Struct>]
    type PreparedPk = { Schema: Schema; Pk: string }

    type QueryBuilderSk<'T> =
        val pk: PreparedPk
        val client: AmazonDynamoDBClient

        new(pk: PreparedPk, client: AmazonDynamoDBClient) = { pk = pk; client = client }

        interface IQueryBuilderSk<'T> with
            member this.WithSk(sortKey: SortKey) =
                Query(
                    { Schema = this.pk.Schema
                      Pk = this.pk.Pk
                      Sk = sortKey },
                    this.client
                )
                :> _

    type QueryBuilderPk<'T> =
        val schema: Schema
        val client: AmazonDynamoDBClient

        new(schema: Schema, client: AmazonDynamoDBClient) = { schema = schema; client = client }

        interface IQueryBuilderPk<'T> with
            member this.WithPk(value: string) =
                QueryBuilderSk({ Schema = this.schema; Pk = value }, this.client) :> _
