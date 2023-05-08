module ListMapTests


open System
open System.Collections.Generic
open Xunit

open TestContext
open Turbine

type TestEntity =
    { Id: Guid
      Name: string
      Numbers: int seq
      Map: IDictionary<string, int> }

let private entity =
    { Id = Guid.NewGuid()
      Name = "Turbine"
      Numbers = [ 1; 2; 3; 4 ]
      Map = [ ("a", 1); ("b", 2) ] |> dict |> Dictionary }

[<Fact>]
let ``Can use map and lists on entities`` () =
    runTest (fun client ->
        task {
            let schema =
                TableSchema(tableName)
                    .AddEntity<TestEntity>()
                    .MapPk(fun c -> c.Id)
                    .MapSk(fun c -> c.Name)

            use turbine = new Turbine(client)

            do! turbine.WithSchema(schema).UpsertAsync(entity)

            let! item =
                turbine
                    .Query(schema)
                    .WithPk(entity.Id.ToString())
                    .WithSk(SortKey.Exactly(entity.Name))
                    .FirstOrDefaultAsync()

            let numbers = entity.Numbers |> Seq.toArray
            let mapArray = entity.Map |> Seq.toArray

            Assert.Multiple(
                (fun () -> Assert.Equal(entity.Id, item.Id)),
                (fun () -> Assert.Equal(entity.Name, item.Name)),
                (fun () -> item.Numbers |> Seq.iteri (fun i n -> Assert.Equal(numbers[i], n))),
                (fun () ->
                    item.Map
                    |> Seq.iteri (fun i (KeyValue(k, v)) ->
                        let (KeyValue(expectedK, expectedV)) = mapArray[i]
                        Assert.Equal(expectedK, k)
                        Assert.Equal(expectedV, v)))
            )
        })
