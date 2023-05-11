[![Build, Test, and Publish to NuGet](https://github.com/dsshep/Turbine/actions/workflows/main.yml/badge.svg)](https://github.com/dsshep/Turbine/actions/workflows/main.yml)

# Turbine

A high level dotnet API for performing CRUD operations on Dynamo DB entities stored in a single table.

## Getting Started

Install from [nuget](https://www.nuget.org/packages/Turbine): `dotnet add package Turbine`

New to Single Table Design? Check out these resources: 
- [Youtube](https://www.youtube.com/watch?v=6yqfmXiZTlM&t=18s) 
- [AWS Workshop](https://amazon-dynamodb-labs.workshop.aws/hands-on-labs.html) 
- [The What, Why, and When of Single-Table Design with DynamoDB](https://www.alexdebrie.com/posts/dynamodb-single-table/)

### Define the schema
A table schema represents the overall structure of a table. 

If your partition key and sort key are called `pk` and `sk`
respectively, this a `TableSchem` can be defined as:

```csharp
var tableSchema = new TableSchema(tableName);
```

Next, an item schema needs to be defined. This is used to determine what properties map to which of the generic `pk` and 
`sk` attributes on the DynamoDB table:

```csharp
var itemSchema = new Schema(tableName)
    .AddEntity<Customer>()
    .MapPk(c => c.Country)
    .MapSk(c => c.City);
```

For this item schema, the Partition Key column is mapped to `Country` and the Sort Key 
column to `City`. These properties will be used when querying, updating, deleting or putting items. All other public 
properties will be mapped to attribute columns.

### Querying data

Once the table and item schemas have been defined, an instance of Turbine can be created and DynamoDB queried.

For example, to find the first customer in Nottingham:

```csharp
// Create IAmazonDynamoDB client
var client = ...

using var turbine = new Turbine(client)

var customer = await turbine
    .Query(itemSchema)
    .WithPk("GB")
    .WithSk(SortKey.Exactly("Nottingham"))
    .FirstOrDefaultAsync();
```

If we wanted a list of customers in cities beginning with "L":

```csharp
// Create DynamoDB client
var client = ...

using var turbine = new Turbine(client)

var customer = await turbine
    .Query(itemSchema)
    .WithPk("GB")
    .WithSk(SortKey.BeginsWith("L"))
    .ToListAsync()
```

`ToListAsync` takes an optional `limit` parameter, that can be used to set the page size.

## Commands

To add an item, use `UpsertAsync`:

```csharp
await turbine
    .WithSchema(itemSchema)
    .UpsertAsync(customer);
```

`UpsertAsync` can also operate on an `IEnumerable<T>`, using batches of 25 items.

Additionally, a convenience method, `PutIfNotExistsAsync`, will only insert the item if it has a distinct `pk` and `sk`:
```csharp
var exists = await turbine
    .WithSchema(itemSchema)
    .PutIfNotExistsAsync(customer);
```

## Transactions

Transactions are supported by starting a new transaction scope using `turbine.StartTransact()`:

```csharp
var entityToInsert = new ...

var success = 
    await transaction
        .WithSchema(itemSchema)
        .Condition(
            entityToInsert,
            Condition.AttributeNotExists("pk").And(Condition.AttributeNotExists("sk"))
        )
        .Upsert(entityToInsert)
        .Commit();
```
`StartTransact` returns an object that implements `IAsyncDisposable`. On dispose, `Commit` will be called if it hasn't already.

## JSON
Items can be mapped to/ from JSON columns:

```csharp
var itemSchema = new Schema(tableName)
    .AddEntity<Customer>()
    .MapPk(c => c.Country)
    .MapSk(c => c.City)
    .ToJsonAttribute("json");
```

## GSIs
Global Secondary Indices (GSIs) can be defined on the table:

```csharp
var tableSchema = TableSchema(tableName)
    .AddGsi("gsi1", o =>
        o.PkName <- "gsi1pk"
        o.SkName <- "gsi1sk");
```

Then, columns can be mapped in the `ItemSchema<T>`, e.g.:

```csharp
var itemSchema = tableSchema
    .AddEntity<Customer>()
    .MapPk(c => c.Country)
    .MapSk(c => c.City)
    .MapGsi("gsi1", gsiPkMapping: c => c.Id, gsiSkMapping: c => c.PostCode);
```

When querying, the GSI can be specified using `QueryGsi`:

```csharp
turbine
    .QueryGsi(itemSchema, "gsi1")
    .WithPk("<PK>")
    .WithSk(SortKey.Exactly("<SK>"))
    .FirstOrDefaultAsync();
```

## Tasks

v1.0 tasks:

- Update
- Scans
  - Filter expressions
- Sort entities
- Nested entities
- Return metrics/ rcus