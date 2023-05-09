module QueryTests

open System.Collections.Generic
open Amazon.DynamoDBv2.Model
open Turbine
open Xunit

open TestContext
open TestData

[<Fact>]
let ``Can perform simple query`` () =
    runTest (fun client ->
        task {
            let! customers = seed tableName (fun c -> string c.Id) (fun c -> c.FullName) (fun _ -> []) client

            let firstCustomer = customers |> Seq.head

            let schema =
                TableSchema(tableName)
                    .AddEntity<AutoPropCustomer>()
                    .MapPk(fun c -> c.Id)
                    .MapSk(fun c -> c.FullName)

            use turbine = new Turbine(client)

            let! customer =
                turbine
                    .Query<AutoPropCustomer>(schema)
                    .WithPk(string firstCustomer.Id)
                    .WithSk(SortKey.Exactly(firstCustomer.FullName))
                    .FirstOrDefaultAsync()

            Assert.Multiple(
                (fun () -> Assert.Equal(customer.Id, firstCustomer.Id)),
                (fun () -> Assert.Equal(customer.FullName, firstCustomer.FullName)),
                (fun () -> Assert.Equal(customer.PhoneNumber, firstCustomer.PhoneNumber)),
                (fun () -> Assert.Equal(customer.Street, firstCustomer.Street)),
                (fun () -> Assert.Equal(customer.City, firstCustomer.City)),
                (fun () -> Assert.Equal(customer.PostCode, firstCustomer.PostCode)),
                (fun () -> Assert.Equal(customer.Country, firstCustomer.Country)),
                (fun () -> Assert.Equal(customer.DateOfBirth, firstCustomer.DateOfBirth)),
                (fun () -> Assert.Equal(customer.HasMadePurchase, firstCustomer.HasMadePurchase))
            )

        })


[<Fact>]
let ``Can perform simple query, with ctor`` () =
    runTest (fun client ->
        task {
            let! customers = seed tableName (fun c -> string c.Id) (fun c -> c.FullName) (fun _ -> []) client

            let firstCustomer = customers |> Seq.head

            let schema =
                TableSchema(tableName)
                    .AddEntity<Customer>()
                    .MapPk(fun c -> c.Id)
                    .MapSk(fun c -> c.FullName)

            use turbine = new Turbine(client)

            let! customer =
                turbine
                    .Query<Customer>(schema)
                    .WithPk(string firstCustomer.Id)
                    .WithSk(SortKey.Exactly(firstCustomer.FullName))
                    .FirstOrDefaultAsync()

            Assert.Equal(customer, firstCustomer)
        })


[<Fact>]
let ``Can fetch list of entities`` () =
    runTest (fun client ->
        task {
            let! customers =
                seed
                    tableName
                    (fun _ -> "NAME")
                    (fun c -> c.FullName)
                    (fun c -> [ KeyValuePair<string, AttributeValue>("id", AttributeValue(string c.Id)) ])
                    client

            let tableSchema = TableSchema(tableName)

            let schema = tableSchema.AddEntity<Customer>().MapSk(fun c -> c.FullName)

            use turbine = new Turbine(client)

            let! customerList =
                turbine
                    .Query<Customer>(schema)
                    .WithPk("NAME")
                    .WithSk(SortKey.BeginsWith("M"))
                    .ToListAsync()

            let expectedCustomers =
                customers
                |> Seq.filter (fun c -> c.FullName.StartsWith("M"))
                |> Seq.sortDescending
                |> Seq.toList

            let orderedCustomers = customerList |> Seq.sortDescending |> Seq.toList

            Assert.Equal<Customer list>(expectedCustomers, orderedCustomers)
        })


[<Fact>]
let ``Can paginate list of entities`` () =
    runTest (fun client ->
        task {
            let! _ =
                seed
                    tableName
                    (fun _ -> "NAME")
                    (fun c -> $"{c.Country}#{c.Id}")
                    (fun c -> [ KeyValuePair<string, AttributeValue>("id", AttributeValue(string c.Id)) ])
                    client

            let schema = TableSchema(tableName).AddEntity<Customer>().MapSk(fun c -> c.FullName)

            use turbine = new Turbine(client)

            let! firstPage =
                turbine
                    .Query<Customer>(schema)
                    .WithPk("NAME")
                    .WithSk(SortKey.BeginsWith("GB"))
                    .ToListAsync(10)

            Assert.True(firstPage.HasNextPage)

            let! nextPage = firstPage.NextPageAsync()

            Assert.Equal(nextPage.Count, firstPage.Count)
            Assert.True(nextPage.HasNextPage)
        })
