namespace Turbine.Tests

open System
open Amazon.DynamoDBv2.Model
open Turbine
open Turbine.Tests.SeedDb
open Xunit

module PutTests =

    [<Fact>]
    let ``Can insert entity`` () =
        task {
            do!
                runTest (fun client ->
                    task {
                        let entityToInsert = generateCustomer ()

                        let schema =
                            Schema(tableName)
                                .AddEntity<Customer>()
                                .PkMapping(fun c -> c.Id)
                                .SkMapping(fun c -> c.FullName)
                                .Schema

                        use turbine = new Turbine(client)

                        do!
                            turbine
                                .Put(schema)
                                .WithPk(entityToInsert.Id.ToString())
                                .WithSk(entityToInsert.FullName)
                                .UpsertAsync(entityToInsert)

                        let! customer =
                            turbine
                                .Query<Customer>(schema)
                                .WithPk(string entityToInsert.Id)
                                .WithSk(SortKey.Exactly entityToInsert.FullName)
                                .QueryAsync()

                        Assert.Multiple(
                            (fun () -> Assert.True(customer.Id = entityToInsert.Id)),
                            (fun () -> Assert.True(customer.FullName = entityToInsert.FullName)),
                            (fun () -> Assert.True(customer.PhoneNumber = entityToInsert.PhoneNumber)),
                            (fun () -> Assert.True(customer.Street = entityToInsert.Street)),
                            (fun () -> Assert.True(customer.City = entityToInsert.City)),
                            (fun () -> Assert.True(customer.PostCode = entityToInsert.PostCode)),
                            (fun () -> Assert.True(customer.Country = entityToInsert.Country))
                        )
                    })
        }

    [<Fact>]
    let ``Can insert batches of entities`` () =
        task {
            do!
                runTest (fun client ->
                    task {
                        let entitiesToInsert =
                            [ for _ = 0 to 100 do
                                  generateCustomer () ]
                            |> List.sortBy (fun c -> c.FullName)

                        let schema =
                            Schema(tableName)
                                .AddEntity<Customer>()
                                .PkMapping(fun c -> c.Country)
                                .SkMapping(fun c -> c.FullName)
                                .Schema

                        use turbine = new Turbine(client)

                        do!
                            turbine
                                .Put(schema)
                                .WithPk(fun c -> c.Country.ToString())
                                .WithSk(fun c -> c.FullName)
                                .UpsertAsync(entitiesToInsert)

                        let! customers =
                            turbine
                                .Query<Customer>(schema)
                                .WithPk("GB")
                                .WithSk(SortKey.Between("A", "ZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZ"))
                                .ToListAsync()

                        Assert.Equal<Customer list>(entitiesToInsert, customers |> Seq.toList)
                    })
        }

    [<Fact>]
    let ``Can convert special type`` () =
        task {
            do!
                runTest (fun client ->
                    task {

                        let ulidCustomer = generateUlidCustomer ()

                        let schema =
                            Schema(tableName)
                                .AddEntity<CustomerWithUlid>()
                                .PkMapping(fun c -> c.Id)
                                .SkMapping(fun c -> c.FullName)
                                .Schema

                        use turbine = new Turbine(client)

                        Turbine.FromDynamoConverters[typeof<Ulid>] <- fun (a: AttributeValue) -> Ulid.Parse(a.S) |> box
                        Turbine.ToDynamoConverters[typeof<Ulid>] <- fun (u: obj) -> AttributeValue(S = string u)

                        do!
                            turbine
                                .Put<CustomerWithUlid>(schema)
                                .WithPk(fun c -> ulidCustomer.Id.ToString())
                                .WithSk(fun c -> c.FullName)
                                .UpsertAsync(ulidCustomer)

                        let! customer =
                            turbine
                                .Query<CustomerWithUlid>(schema)
                                .WithPk(ulidCustomer.Id.ToString())
                                .WithSk(SortKey.Exactly(ulidCustomer.FullName))
                                .QueryAsync()

                        Assert.Multiple(
                            (fun () -> Assert.True(customer.Id = ulidCustomer.Id)),
                            (fun () -> Assert.True(customer.FullName = ulidCustomer.FullName)),
                            (fun () -> Assert.True(customer.PhoneNumber = ulidCustomer.PhoneNumber)),
                            (fun () -> Assert.True(customer.Street = ulidCustomer.Street)),
                            (fun () -> Assert.True(customer.City = ulidCustomer.City)),
                            (fun () -> Assert.True(customer.PostCode = ulidCustomer.PostCode)),
                            (fun () -> Assert.True(customer.Country = ulidCustomer.Country))
                        )
                    })
        }
