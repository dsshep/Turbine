module Tests

open System
open System.Collections.Generic
open System.Threading.Tasks
open Amazon.DynamoDBv2
open Amazon.DynamoDBv2.Model
open SeedDb
open Testcontainers.DynamoDb
open Turbine
open Xunit

let setEnv () =
    let setEnvVar (key: string) (value: string) =
        if Environment.GetEnvironmentVariable key = null then
            Environment.SetEnvironmentVariable(key, value)

    setEnvVar "AWS_ACCESS_KEY_ID" "AWS_ACCESS_KEY_ID"
    setEnvVar "AWS_SECRET_ACCESS_KEY" "AWS_SECRET_ACCESS_KEY"

let tableName = "TestTable"

let runTest (func: AmazonDynamoDBClient -> Task) =
    task {
        setEnv ()
        let dynamoContainer = DynamoDbBuilder().Build()

        do! dynamoContainer.StartAsync()

        let config =
            AmazonDynamoDBConfig(ServiceURL = dynamoContainer.GetConnectionString())

        use client = new AmazonDynamoDBClient(config)

        let attributeDefinitions =
            ResizeArray
                [ AttributeDefinition(AttributeName = "pk", AttributeType = "S")
                  AttributeDefinition(AttributeName = "sk", AttributeType = "S") ]

        let keySchema =
            ResizeArray
                [ KeySchemaElement(AttributeName = "pk", KeyType = "HASH")
                  KeySchemaElement(AttributeName = "sk", KeyType = "RANGE") ]

        let provisionedThroughput =
            ProvisionedThroughput(ReadCapacityUnits = 5L, WriteCapacityUnits = 5L)

        let request =
            CreateTableRequest(
                TableName = tableName,
                AttributeDefinitions = attributeDefinitions,
                KeySchema = keySchema,
                ProvisionedThroughput = provisionedThroughput
            )

        let! _ = client.CreateTableAsync(request)

        do! func client

        do! dynamoContainer.DisposeAsync()
    }

[<Fact>]
let ``Can perform simple query`` () =
    task {
        do!
            runTest (fun client ->
                task {
                    let! customers = seed tableName (fun c -> string c.Id) (fun c -> c.FullName) (fun _ -> []) client
                    let firstCustomer = customers |> Seq.head

                    let schema =
                        Schema(tableName)
                            .AddEntity<AutoPropCustomer>()
                            .PartitionKey(fun c -> c.Id)
                            .SortKey(fun c -> c.FullName)
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
                    let! customers = seed tableName (fun c -> string c.Id) (fun c -> c.FullName) (fun _ -> []) client
                    let firstCustomer = customers |> Seq.head

                    let schema =
                        Schema(tableName)
                            .AddEntity<Customer>()
                            .PartitionKey(fun c -> c.Id)
                            .SortKey(fun c -> c.FullName)
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
                        Schema(tableName).AddEntity<Customer>().SortKey(fun c -> c.FullName).Schema

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
                        Schema(tableName).AddEntity<Customer>().SortKey(fun c -> c.FullName).Schema

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
