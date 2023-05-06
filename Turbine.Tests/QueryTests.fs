module Tests

open System
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
                    let! customers = seed tableName client
                    let firstCustomer = customers |> Seq.head

                    let schema =
                        Schema(tableName)
                            .AddEntity<CliMutableCustomer>()
                            .PartitionKey(fun c -> c.Id)
                            .SortKey(fun c -> c.FullName)
                            .Schema

                    use turbine = new Turbine(client)

                    let! customer =
                        turbine
                            .Query<CliMutableCustomer>(schema)
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
