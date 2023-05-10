module TestContext

open System
open System.Threading.Tasks
open Amazon.DynamoDBv2
open Amazon.DynamoDBv2.Model
open Testcontainers.DynamoDb

let private setEnv () =
    let setEnvVar (key: string) (value: string) =
        if Environment.GetEnvironmentVariable key = null then
            Environment.SetEnvironmentVariable(key, value)

    setEnvVar "AWS_ACCESS_KEY_ID" "AWS_ACCESS_KEY_ID"
    setEnvVar "AWS_SECRET_ACCESS_KEY" "AWS_SECRET_ACCESS_KEY"

let tableName = "TestTable"
let gsi1Name = "gsi1"

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
                  AttributeDefinition(AttributeName = "sk", AttributeType = "S")
                  AttributeDefinition(AttributeName = "gsi1pk", AttributeType = "S")
                  AttributeDefinition(AttributeName = "gsi1sk", AttributeType = "S") ]

        let keySchema =
            ResizeArray
                [ KeySchemaElement(AttributeName = "pk", KeyType = "HASH")
                  KeySchemaElement(AttributeName = "sk", KeyType = "RANGE") ]

        let gsi1KeySchema =
            ResizeArray
                [ KeySchemaElement(AttributeName = "gsi1pk", KeyType = "HASH")
                  KeySchemaElement(AttributeName = "gsi1sk", KeyType = "RANGE") ]

        let provisionedThroughput =
            ProvisionedThroughput(ReadCapacityUnits = 5L, WriteCapacityUnits = 5L)

        let gsis =
            ResizeArray
                [ GlobalSecondaryIndex(
                      IndexName = gsi1Name,
                      Projection = Projection(ProjectionType = "ALL"),
                      ProvisionedThroughput = provisionedThroughput,
                      KeySchema = gsi1KeySchema
                  ) ]

        let request =
            CreateTableRequest(
                TableName = tableName,
                AttributeDefinitions = attributeDefinitions,
                KeySchema = keySchema,
                ProvisionedThroughput = provisionedThroughput,
                GlobalSecondaryIndexes = gsis
            )

        let! _ = client.CreateTableAsync(request)

        do! func client

        do! dynamoContainer.DisposeAsync()
    }
