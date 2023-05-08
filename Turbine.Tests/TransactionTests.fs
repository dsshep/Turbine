module TransactionTests

open Xunit
open TestData
open TestContext
open Turbine

[<Fact>]
let ``Insert item when already exists fails`` () =
    runTest (fun client ->
        task {
            let entityToInsert = generateCustomer ()

            let schema =
                TableSchema(tableName)
                    .AddEntity<Customer>()
                    .MapPk(fun c -> c.Id)
                    .MapSk(fun c -> c.FullName)

            use turbine = new Turbine(client)

            do! turbine.WithSchema(schema).UpsertAsync(entityToInsert)

            use transaction = turbine.StartTransact()

            let! success =
                transaction
                    .WithSchema(schema)
                    .Condition(
                        entityToInsert,
                        Condition.AttributeNotExists("pk").And(Condition.AttributeNotExists("sk"))
                    )
                    .Upsert(entityToInsert)
                    .Commit()

            Assert.False(success)
        })
