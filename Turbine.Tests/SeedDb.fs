module SeedDb

open System
open System.Collections.Generic
open Bogus
open Amazon.DynamoDBv2
open Amazon.DynamoDBv2.Model

type AutoPropCustomer() =
    member val Id: Guid = Unchecked.defaultof<_> with get, set
    member val FullName: string = Unchecked.defaultof<_> with get, set
    member val PhoneNumber: string = Unchecked.defaultof<_> with get, set
    member val Street: string = Unchecked.defaultof<_> with get, set
    member val City: string = Unchecked.defaultof<_> with get, set
    member val PostCode: string = Unchecked.defaultof<_> with get, set
    member val Country: string = Unchecked.defaultof<_> with get, set

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

let seed
    (tableName: string)
    (pk: Customer -> string)
    (sk: Customer -> string)
    (additionalAttributes: Customer -> KeyValuePair<string, AttributeValue> list)
    (client: AmazonDynamoDBClient)
    =
    let writeBatch (batch: Customer list) =
        task {
            let createWriteRequest item =
                let serialized = System.Text.Json.JsonSerializer.Serialize item

                let itemAttributes =
                    [ KeyValuePair<string, AttributeValue>("pk", AttributeValue(pk item))
                      KeyValuePair<string, AttributeValue>("sk", AttributeValue(sk item))

                      KeyValuePair<string, AttributeValue>("phoneNumber", AttributeValue(item.PhoneNumber))
                      KeyValuePair<string, AttributeValue>("street", AttributeValue(item.Street))
                      KeyValuePair<string, AttributeValue>("city", AttributeValue(item.City))
                      KeyValuePair<string, AttributeValue>("postCode", AttributeValue(item.PostCode))
                      KeyValuePair<string, AttributeValue>("country", AttributeValue(item.Country))

                      KeyValuePair<string, AttributeValue>("json", AttributeValue(serialized)) ]
                    @ (additionalAttributes item)

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
