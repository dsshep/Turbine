module JsonTests

open System
open Xunit

open TestContext
open Turbine

type TestEntity =
    { Id: Guid
      Name: string
      Numbers: ResizeArray<int> }

let private entity =
    { Id = Guid.NewGuid()
      Name = "Turbine"
      Numbers = [ 1; 2; 3; 4 ] |> ResizeArray }

[<Fact>]
let ``Can map to json column`` () =
    runTest (fun client ->
        task {
            let schema =
                TableSchema(tableName)
                    .AddEntity<TestEntity>()
                    .MapPk(fun c -> c.Id)
                    .MapSk(fun c -> c.Name)
                    .ToJsonAttribute("json")

            use turbine = new Turbine(client)

            do! turbine.WithSchema(schema).UpsertAsync(entity)

            let! item =
                turbine
                    .Query(schema)
                    .WithPk(entity.Id.ToString())
                    .WithSk(SortKey.Exactly(entity.Name))
                    .FirstOrDefaultAsync()

            let numbers = entity.Numbers |> Seq.toArray

            Assert.Multiple(
                (fun () -> Assert.Equal(entity.Id, item.Id)),
                (fun () -> Assert.Equal(entity.Name, item.Name)),
                (fun () -> item.Numbers |> Seq.iteri (fun i n -> Assert.Equal(numbers[i], n)))
            )
        })
