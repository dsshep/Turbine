module SeedDb

open System
open System.Collections.Generic
open Bogus
open Amazon.DynamoDBv2
open Amazon.DynamoDBv2.Model

[<CLIMutable>]
type CliMutableCustomer =
    { Id: Guid
      FullName: string
      PhoneNumber: string
      Street: string
      City: string
      PostCode: string
      Country: string }

type Customer =
    { Id: Guid
      FullName: string
      PhoneNumber: string
      Street: string
      City: string
      PostCode: string
      Country: string }

let private faker = Faker "en"

let private generateCustomer () =
    { Id = faker.Random.Guid()
      FullName = faker.Name.FullName()
      PhoneNumber = faker.Phone.PhoneNumber()
      Street = faker.Address.StreetAddress()
      City = faker.Address.City()
      PostCode = faker.Address.ZipCode()
      Country = faker.Address.Country() }

let seed (tableName: string) (client: AmazonDynamoDBClient) =
    let writeBatch (batch: Customer list) =
        task {
            let createWriteRequest item =
                let serialized = System.Text.Json.JsonSerializer.Serialize item

                let itemAttributes =
                    [ KeyValuePair<string, AttributeValue>("pk", AttributeValue(string item.Id))
                      KeyValuePair<string, AttributeValue>("sk", AttributeValue(item.FullName))

                      KeyValuePair<string, AttributeValue>("PhoneNumber", AttributeValue(item.PhoneNumber))
                      KeyValuePair<string, AttributeValue>("Street", AttributeValue(item.Street))
                      KeyValuePair<string, AttributeValue>("City", AttributeValue(item.City))
                      KeyValuePair<string, AttributeValue>("PostCode", AttributeValue(item.PostCode))
                      KeyValuePair<string, AttributeValue>("Country", AttributeValue(item.Country))

                      KeyValuePair<string, AttributeValue>("json", AttributeValue(serialized)) ]

                WriteRequest(PutRequest(Dictionary<string, AttributeValue>(itemAttributes)))

            let writeRequests = batch |> Seq.map createWriteRequest |> ResizeArray
            let requestItems = Dictionary<string, List<WriteRequest>>()
            requestItems[tableName] <- writeRequests

            let batchWriteRequest = BatchWriteItemRequest(requestItems)
            let! _ = client.BatchWriteItemAsync batchWriteRequest

            return ()
        }

    task {
        let customers =
            [ for _ = 1 to 100 do
                  generateCustomer () ]

        for customerBatch in customers |> List.chunkBySize 25 do
            do! writeBatch customerBatch

        return customers
    }
