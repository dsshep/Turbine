# Turbine

A high level dotnet API for performing CRUD operations on Dynamo DB entities stored in a single table.

## Getting Started
(Nuget package not available)

### Define the schema
Define the Schema for your table and how it maps to the entities stored within it.

For example, if you have a table:

| Partition Key | Sort Key | Attributes... |
|---------------|----------|---------------|
| pk            | sk       | ...           |

Where we store an entity called customer:

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
var tableSchema = new Schema(tableName)
    .AddEntity<Customer>()
    .PartitionKey(fun c -> c.Country)
    .SortKey(fun c -> c.City)
    .Schema
```
### Query the data

If we wanted to find the first customer in Nottingham:

```csharp
// Create DynamoDB client
var client = ...

using var turbine = new Turbine(client)

var customer = await turbine
    .Query<Customer>(schema)
    .WithPk("GB")
    .WithSk(SortKey.Exactly("Nottingham"))
    .QueryAsync()
```

If we wanted a list of customers in cities beginning with "L":

```csharp
// Create DynamoDB client
var client = ...

using var turbine = new Turbine(client)

var customer = await turbine
    .Query<Customer>(schema)
    .WithPk("GB")
    .WithSk(SortKey.BeginsWith("L"))
    .ToListAsync()
```


## Tasks

v0.1 tasks:

| Task                          | Status |
|-------------------------------|--------|
| Query Entity, Props           | ✅      | 
| Query Entity, Constructor     | ✅      | 
| List Entities, Props          | ✅      | 
| List Entities, Constructor    | ✅      | 
| Delete Entity                 |        |
| Batch Delete                  |        |
| Put Entity                    |        | 
| Batch Put Entity              |        |
| CI pipeline and push to nuget |        |

Beyond:

- Custom hydration, e.g. compound sort keys
- Query on GSIs
- Scans
  - Filter expressions
- Sort entities
- Nested entities
- Attribute Projections
  