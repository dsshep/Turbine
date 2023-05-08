module ConditionTests

open System
open Xunit

open TestContext
open Turbine

type TestEntity =
    { Id: Guid; Name: string; ANumber: int }

let private entity =
    { Id = Guid.NewGuid()
      Name = "Turbine"
      ANumber = 5 }

[<Fact>]
let ``Can compose complex condition`` () =
    let condition =
        Condition
            .Size("hardDrive")
            .LessThanValue(3)
            .And(Condition.Equal("type", "ssd"))
            .And(Condition.BeginsWith("model", "seagate"))

    let conditionCheck = condition.ToConditionCheck("")

    Assert.Equal("size(hardDrive) < :v1 AND type = :v2 AND begins_with(model, :v3)", conditionCheck.ConditionExpression)


[<Fact>]
let ``Can use condition on delete`` () =
    runTest (fun client ->
        task {
            let schema =
                TableSchema(tableName)
                    .AddEntity<TestEntity>()
                    .MapPk(fun c -> c.Id)
                    .MapSk(fun c -> c.Name)

            use turbine = new Turbine(client)

            do! turbine.WithSchema(schema).UpsertAsync(entity)

            use transaction = turbine.StartTransact()

            let! success =
                transaction
                    .WithSchema(schema)
                    .Delete(entity, Condition.GreaterThan("ANumber", 1))
                    .Commit()

            let! deleteEntity =
                turbine
                    .Query(schema)
                    .WithPk(entity.Id.ToString())
                    .WithSk(SortKey.Exactly(entity.Name))
                    .FirstOrDefaultAsync()

            Assert.True(success)
            Assert.Null(deleteEntity)
        })

[<Fact>]
let ``Can use condition to prevent delete`` () =
    runTest (fun client ->
        task {
            let schema =
                TableSchema(tableName)
                    .AddEntity<TestEntity>()
                    .MapPk(fun c -> c.Id)
                    .MapSk(fun c -> c.Name)

            use turbine = new Turbine(client)

            do! turbine.WithSchema(schema).UpsertAsync(entity)

            use transaction = turbine.StartTransact()

            let! success =
                transaction
                    .WithSchema(schema)
                    .Delete(entity, Condition.GreaterThan("ANumber", 10))
                    .Commit()

            let! deleteEntity =
                turbine
                    .Query(schema)
                    .WithPk(entity.Id.ToString())
                    .WithSk(SortKey.Exactly(entity.Name))
                    .FirstOrDefaultAsync()

            Assert.False(success)
            Assert.NotNull(deleteEntity)
        })
