namespace Turbine.Tests

open System.Collections.Generic
open Amazon.DynamoDBv2.Model
open Turbine
open Turbine.Tests.SeedDb

module QueryTests =

    open Xunit
    open Turbine.Tests

    [<Fact>]
    let ``Can perform simple query`` () =
        task {
            do!
                runTest (fun client ->
                    task {
                        let! customers =
                            seed tableName (fun c -> string c.Id) (fun c -> c.FullName) (fun _ -> []) client

                        let firstCustomer = customers |> Seq.head

                        let schema =
                            Schema(tableName)
                                .AddEntity<AutoPropCustomer>()
                                .PkMapping(fun c -> c.Id)
                                .SkMapping(fun c -> c.FullName)
                                .Schema

                        use turbine = new Turbine(client)

                        let! customer =
                            turbine
                                .Query<AutoPropCustomer>(schema)
                                .WithPk(string firstCustomer.Id)
                                .WithSk(SortKey.Exactly(firstCustomer.FullName))
                                .QueryAsync()

                        Assert.Multiple(
                            (fun () -> Assert.True(customer.Id = firstCustomer.Id)),
                            (fun () -> Assert.True(customer.FullName = firstCustomer.FullName)),
                            (fun () -> Assert.True(customer.PhoneNumber = firstCustomer.PhoneNumber)),
                            (fun () -> Assert.True(customer.Street = firstCustomer.Street)),
                            (fun () -> Assert.True(customer.City = firstCustomer.City)),
                            (fun () -> Assert.True(customer.PostCode = firstCustomer.PostCode)),
                            (fun () -> Assert.True(customer.Country = firstCustomer.Country))
                        )

                    })
        }

    [<Fact>]
    let ``Can perform simple query, with ctor`` () =
        task {
            do!
                runTest (fun client ->
                    task {
                        let! customers =
                            seed tableName (fun c -> string c.Id) (fun c -> c.FullName) (fun _ -> []) client

                        let firstCustomer = customers |> Seq.head

                        let schema =
                            Schema(tableName)
                                .AddEntity<Customer>()
                                .PkMapping(fun c -> c.Id)
                                .SkMapping(fun c -> c.FullName)
                                .Schema

                        use turbine = new Turbine(client)

                        let! customer =
                            turbine
                                .Query<Customer>(schema)
                                .WithPk(string firstCustomer.Id)
                                .WithSk(SortKey.Exactly(firstCustomer.FullName))
                                .QueryAsync()

                        Assert.Multiple(
                            (fun () -> Assert.True(customer.Id = firstCustomer.Id)),
                            (fun () -> Assert.True(customer.FullName = firstCustomer.FullName)),
                            (fun () -> Assert.True(customer.PhoneNumber = firstCustomer.PhoneNumber)),
                            (fun () -> Assert.True(customer.Street = firstCustomer.Street)),
                            (fun () -> Assert.True(customer.City = firstCustomer.City)),
                            (fun () -> Assert.True(customer.PostCode = firstCustomer.PostCode)),
                            (fun () -> Assert.True(customer.Country = firstCustomer.Country))
                        )

                    })
        }

    [<Fact>]
    let ``Can fetch list of entities`` () =
        task {
            do!
                runTest (fun client ->
                    task {
                        let! customers =
                            seed
                                tableName
                                (fun _ -> "NAME")
                                (fun c -> c.FullName)
                                (fun c -> [ KeyValuePair<string, AttributeValue>("id", AttributeValue(string c.Id)) ])
                                client

                        let schema =
                            Schema(tableName).AddEntity<Customer>().SkMapping(fun c -> c.FullName).Schema

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
        }

    [<Fact>]
    let ``Can paginate list of entities`` () =
        task {
            do!
                runTest (fun client ->
                    task {
                        let! customers =
                            seed
                                tableName
                                (fun _ -> "NAME")
                                (fun c -> $"{c.Country}#{c.Id}")
                                (fun c -> [ KeyValuePair<string, AttributeValue>("id", AttributeValue(string c.Id)) ])
                                client

                        let schema =
                            Schema(tableName).AddEntity<Customer>().SkMapping(fun c -> c.FullName).Schema

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
        }
