module DeleteTests


open Turbine
open Xunit

open TestContext
open TestData

[<Fact>]
let ``Can delete entity`` () =
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

            let! customer =
                turbine
                    .Query<Customer>(schema)
                    .WithPk(string entityToInsert.Id)
                    .WithSk(SortKey.Exactly entityToInsert.FullName)
                    .FirstOrDefaultAsync()

            Assert.NotNull(customer)

            do!
                turbine
                    .WithSchema(schema)
                    .DeleteAsync(customer.Id.ToString(), customer.FullName)

            let! deletedCustomer =
                turbine
                    .Query<Customer>(schema)
                    .WithPk(string entityToInsert.Id)
                    .WithSk(SortKey.Exactly entityToInsert.FullName)
                    .FirstOrDefaultAsync()

            Assert.Null(deletedCustomer)
        })

[<Fact>]
let ``Can delete many entities`` () =
    runTest (fun client ->
        task {

            let entitiesToInsert =
                [ for _ = 0 to 100 do
                      generateCustomer () ]
                |> List.sortBy (fun c -> c.FullName)

            let schema =
                TableSchema(tableName)
                    .AddEntity<Customer>()
                    .MapPk(fun c -> c.Country)
                    .MapSk(fun c -> c.FullName)

            use turbine = new Turbine(client)

            do! turbine.WithSchema(schema).UpsertAsync(entitiesToInsert)

            do! turbine.WithSchema(schema).DeleteAsync("GB", SortKey.Between("A", "zzzz"))

            let! customers =
                turbine
                    .Query<Customer>(schema)
                    .WithPk("GB")
                    .WithSk(SortKey.Between("A", "zzzz"))
                    .ToListAsync()

            Assert.Empty(customers)

        })
