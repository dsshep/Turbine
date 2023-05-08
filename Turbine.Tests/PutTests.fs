module PutTests

open System
open Amazon.DynamoDBv2.Model
open Turbine
open Xunit

open TestContext
open TestData

[<Fact>]
let ``Can insert entity`` () =
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

            Assert.Equal(customer, entityToInsert)
        })


[<Fact>]
let ``Can insert batches of entities`` () =
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

            let! customers =
                turbine
                    .Query<Customer>(schema)
                    .WithPk("GB")
                    .WithSk(SortKey.Between("A", "zzzz"))
                    .ToListAsync()

            Assert.Equal<Customer list>(entitiesToInsert, customers |> Seq.toList)
        })


[<Fact>]
let ``Can insert entity only once`` () =
    task {
        do!
            runTest (fun client ->
                task {
                    let entitiesToInsert = generateCustomer ()

                    let schema =
                        TableSchema(tableName)
                            .AddEntity<Customer>()
                            .MapPk(fun c -> c.Country)
                            .MapSk(fun c -> c.FullName)

                    use turbine = new Turbine(client)

                    do! turbine.WithSchema(schema).UpsertAsync(entitiesToInsert)
                    let! result = turbine.WithSchema(schema).PutIfNotExistsAsync(entitiesToInsert)

                    Assert.False(result)
                })
    }

[<Fact>]
let ``Can convert special type`` () =
    runTest (fun client ->
        task {
            let ulidCustomer = generateUlidCustomer ()

            let schema =
                TableSchema(tableName)
                    .AddEntity<CustomerWithUlid>()
                    .MapPk(fun c -> c.Id)
                    .MapSk(fun c -> c.FullName)

            use turbine = new Turbine(client)

            Turbine.FromDynamoConverters[typeof<Ulid>] <- fun (a: AttributeValue) -> Ulid.Parse(a.S) |> box
            Turbine.ToDynamoConverters[typeof<Ulid>] <- fun (u: obj) -> AttributeValue(S = string u)

            do! turbine.WithSchema(schema).UpsertAsync(ulidCustomer)

            let! customer =
                turbine
                    .Query<CustomerWithUlid>(schema)
                    .WithPk(ulidCustomer.Id.ToString())
                    .WithSk(SortKey.Exactly(ulidCustomer.FullName))
                    .FirstOrDefaultAsync()

            Assert.Equal(customer, ulidCustomer)
        })

[<Fact>]
let ``Can use attributes`` () =
    runTest (fun client ->
        task {
            let attributeCustomer = generateAttributeCustomer ()

            let schema = TableSchema(tableName).AddEntity<CustomerWithAttributes>()

            use turbine = new Turbine(client)

            do! turbine.WithSchema(schema).UpsertAsync(attributeCustomer)

            let! customer =
                turbine
                    .Query<CustomerWithAttributes>(schema)
                    .WithPk(attributeCustomer.Id.ToString())
                    .WithSk(SortKey.Exactly(attributeCustomer.FullName))
                    .FirstOrDefaultAsync()

            Assert.Equal(attributeCustomer, customer)
        })
