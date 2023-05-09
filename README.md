[![Build, Test, and Publish to NuGet](https://github.com/dsshep/Turbine/actions/workflows/main.yml/badge.svg)](https://github.com/dsshep/Turbine/actions/workflows/main.yml)

# Turbine

A high level dotnet API for performing CRUD operations on Dynamo DB entities stored in a single table.

## Getting Started

New to Single Table Design? Check out these resources: [Youtube](https://www.youtube.com/watch?v=6yqfmXiZTlM&t=18s), [AWS Workshop](https://amazon-dynamodb-labs.workshop.aws/hands-on-labs.html).

### Define the schema
Define the Schema for your table and how it maps to the entities stored within it.

By default, Turbine assumes the Partition Key is called `pk` and the sort key `sk`. These can be overridden. A table 
must have at least a Partition Key and Sort key to work with Turbine. 

For example, if you have a table:

| Partition Key | Sort Key | Attributes... |
|---------------|----------|---------------|
| pk            | sk       | ...           |

With an item:

```csharp
class Customer 
{
    public Guid Id { get; set; }
    public string FullName { get; set; }
    public string PhoneNumber { get; set; }
    public string Street { get; set; }
    public string City { get; set; }
    public string PostCode { get; set; }
    public string Country { get; set; }
}
```

The schema can be defined as:

```csharp
var tableSchema = new TableSchema(tableName);

var itemSchema = new Schema(tableName)
    .AddEntity<Customer>()
    .MapPk(c => c.Country)
    .MapSk(c => c.City)
```

This tells Turbine that for this schema definition, the Partition Key column is mapped to `Country` and the Sort Key 
column to `City`. These properties will be used when querying, updating, deleting or putting items. All other public 
properties will be mapped into attribute columns by default.

### Querying data

If we wanted to find the first customer in Nottingham:

```csharp
// Create DynamoDB client
var client = ...

using var turbine = new Turbine(client)

var customer = await turbine
    .Query<Customer>(itemSchema)
    .WithPk("GB")
    .WithSk(SortKey.Exactly("Nottingham"))
    .FirstOrDefaultAsync()
```

If we wanted a list of customers in cities beginning with "L":

```csharp
// Create DynamoDB client
var client = ...

using var turbine = new Turbine(client)

var customer = await turbine
    .Query<Customer>(itemSchema)
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

`UpsertAsync` can also operate on an `IEnumerable<T>`, using a batches of 25 regardless of enumerable size.

Set it to only insert the item if it does not already exist:
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
        .Commit()
```
`StartTransact` returns an object that implements `IAsyncDisposable`. On dispose, if `Commit` it will call commit if it 
has not already been called.

## JSON
Items can be mapped to/ from JSON columns:

```csharp
var itemSchema = new Schema(tableName)
    .AddEntity<Customer>()
    .MapPk(c => c.Country)
    .MapSk(c => c.City)
    .ToJsonAttribute("json")
```


## Tasks

v1.0 tasks:

- Update
- Operations with GSIs
- Scans
  - Filter expressions
- Sort entities
- Nested entities
- Return metrics/ rcus