[![Build, Test, and Publish to NuGet](https://github.com/dsshep/Turbine/actions/workflows/main.yml/badge.svg)](https://github.com/dsshep/Turbine/actions/workflows/main.yml)

# Turbine

A high level dotnet API for performing CRUD operations on Dynamo DB entities stored in a single table.

## Getting Started

New to Single Table Design? Check out these resources: [Yotube](https://www.youtube.com/watch?v=6yqfmXiZTlM&t=18s), [AWS Workshop](https://amazon-dynamodb-labs.workshop.aws/hands-on-labs.html).

(Nuget package not available)

### Define the schema
Define the Schema for your table and how it maps to the entities stored within it.

For example, if you have a table:

| Partition Key | Sort Key | Attributes... |
|---------------|----------|---------------|
| pk            | sk       | ...           |

Where we store an item called customer:

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
    .MapPk(fun c -> c.Country)
    .MapSk(fun c -> c.City)
```
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


## Tasks

v0.1 tasks:

- Update
- Custom hydration
- Query on GSIs
- Scans
  - Filter expressions
- Sort entities
- Nested entities
- Attribute Projections
- Map to/from json
- Return metrics/ rcus