module GsiTests

open Xunit
open TestContext
open TestData
open Turbine

[<Fact>]
let ``Can query gsi`` () =
    runTest (fun client ->
        task {
            let attributeCustomer = generateCustomer ()

            let tableSchema =
                TableSchema(tableName)
                    .AddGsi(
                        gsi1Name,
                        (fun o ->
                            o.PkName <- "gsi1pk"
                            o.SkName <- "gsi1sk")
                    )

            let schema =
                tableSchema
                    .AddEntity<Customer>()
                    .MapPk(fun c -> c.Country)
                    .MapSk(fun c -> c.City)
                    .MapGsi(gsi1Name, (fun c -> c.Id), (fun c -> c.PostCode))

            use turbine = new Turbine(client)

            do! turbine.WithSchema(schema).UpsertAsync(attributeCustomer)

            let! customer =
                turbine
                    .QueryGsi(schema, gsi1Name)
                    .WithPk(attributeCustomer.Id |> string)
                    .WithSk(SortKey.Exactly(attributeCustomer.PostCode))
                    .FirstOrDefaultAsync()

            Assert.Equal(attributeCustomer, customer)
        })
