module NestedObjectTests

open System
open Xunit
open TestContext
open Turbine

type NestedObject = { Id: Guid; Name: string }

type MyObject =
    { Id: Guid
      Name: string
      Nested: NestedObject }


type AutoPropNested() =
    member val Id: Guid = Unchecked.defaultof<_> with get, set
    member val Name: string = Unchecked.defaultof<_> with get, set

type AutoPropObject() =
    member val Id: Guid = Unchecked.defaultof<_> with get, set
    member val Name: string = Unchecked.defaultof<_> with get, set
    member val Nested: AutoPropNested = Unchecked.defaultof<_> with get, set

[<Fact>]
let ``Can put and query nested object`` () =
    runTest (fun client ->
        task {
            let nestedObject =
                { Id = Guid.NewGuid()
                  Name = "Parent"
                  Nested = { Id = Guid.NewGuid(); Name = "Child" } }

            let tableSchema = TableSchema(tableName)

            let schema =
                tableSchema.AddEntity<MyObject>().MapPk(fun o -> o.Id).MapSk(fun o -> o.Name)

            use turbine = new Turbine(client)

            do! turbine.WithSchema(schema).UpsertAsync(nestedObject)

            let! dynamoNested =
                turbine
                    .Query(schema)
                    .WithPk(nestedObject.Id |> string)
                    .WithSk(SortKey.Exactly(nestedObject.Name))
                    .FirstOrDefaultAsync()

            Assert.Equal(nestedObject, dynamoNested)
        })


[<Fact>]
let ``Can put and query nested object with auto props`` () =
    runTest (fun client ->
        task {
            let nestedObject =
                AutoPropObject(
                    Id = Guid.NewGuid(),
                    Name = "Parent",
                    Nested = AutoPropNested(Id = Guid.NewGuid(), Name = "Child")
                )

            let tableSchema = TableSchema(tableName)

            let schema =
                tableSchema
                    .AddEntity<AutoPropObject>()
                    .MapPk(fun o -> o.Id)
                    .MapSk(fun o -> o.Name)

            use turbine = new Turbine(client)

            do! turbine.WithSchema(schema).UpsertAsync(nestedObject)

            let! dynamoNested =
                turbine
                    .Query(schema)
                    .WithPk(nestedObject.Id |> string)
                    .WithSk(SortKey.Exactly(nestedObject.Name))
                    .FirstOrDefaultAsync()

            Assert.Multiple(
                (fun () -> Assert.Equal(nestedObject.Id, dynamoNested.Id)),
                (fun () -> Assert.Equal(nestedObject.Name, dynamoNested.Name)),
                (fun () -> Assert.Equal(nestedObject.Nested.Id, dynamoNested.Nested.Id)),
                (fun () -> Assert.Equal(nestedObject.Nested.Name, dynamoNested.Nested.Name))
            )
        })
