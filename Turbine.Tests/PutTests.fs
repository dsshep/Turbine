namespace Turbine.Tests

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