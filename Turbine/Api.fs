namespace rec Turbine

open System
open System.Collections.Generic
open Amazon.DynamoDBv2.Model
open Turbine
open System.Threading.Tasks

type internal IPageableQuery<'T> =
    abstract member DoQuery:
        itemLimit: Nullable<int> * ?lastEvalKey: Dictionary<string, AttributeValue> -> Task<QueryResponse>

type PageList<'T> =
    inherit ResizeArray<'T>
    val private hasNextPage: bool
    val private pageSize: Nullable<int>
    val private previousResponse: QueryResponse
    val private query: IPageableQuery<'T>
    val private schema: Schema

    member this.HasNextPage = this.hasNextPage

    member this.NextPageAsync() =
        task {
            let! response = this.query.DoQuery(this.pageSize, this.previousResponse.LastEvaluatedKey)

            let entities =
                response.Items
                |> Seq.map (fun item -> EntityBuilder.hydrateEntity<'T> (this.schema, item))

            return PageList<_>(entities, this.pageSize, response, this.query, this.schema)
        }

    internal new(items: seq<'T>,
                 pageSize: Nullable<int>,
                 previousResponse: QueryResponse,
                 query: IPageableQuery<'T>,
                 schema: Schema) =
        { inherit ResizeArray<'T>(items)
          hasNextPage = previousResponse.LastEvaluatedKey <> null
          pageSize = pageSize
          previousResponse = previousResponse
          query = query
          schema = schema }

type IQuery<'T> =
    abstract member QueryAsync: unit -> Task<'T>

    abstract member ToListAsync: unit -> Task<PageList<'T>>

    abstract member ToListAsync: limit: Nullable<int> -> Task<PageList<'T>>

type IQueryBuilderSk<'T> =
    abstract member WithSk: SortKey -> IQuery<'T>

type IQueryBuilderPk<'T> =
    abstract member WithPk<'TProperty> : value: string -> IQueryBuilderSk<'T>
