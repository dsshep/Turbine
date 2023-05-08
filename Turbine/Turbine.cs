using Amazon.DynamoDBv2;
using Amazon.DynamoDBv2.Model;

namespace Turbine;

public sealed class Turbine : IDisposable
{
    public static readonly IDictionary<Type, Func<AttributeValue, object?>> FromDynamoConverters =
        new Dictionary<Type, Func<AttributeValue, object?>>();

    public static readonly IDictionary<Type, Func<object?, AttributeValue>> ToDynamoConverters =
        new Dictionary<Type, Func<object?, AttributeValue>>();

    private readonly IAmazonDynamoDB client;

    public Turbine(IAmazonDynamoDB client)
    {
        ArgumentNullException.ThrowIfNull(client);
        this.client = client;
    }

    public void Dispose()
    {
        client.Dispose();
    }

    public IQueryBuilderPk<T> Query<T>(ItemSchema<T> itemSchema)
    {
        ArgumentNullException.ThrowIfNull(itemSchema);
        return new QueryBuilderPk<T>(itemSchema, client);
    }

    public ITurbineEntitySchema<T> WithSchema<T>(ItemSchema<T> itemSchema)
    {
        ArgumentNullException.ThrowIfNull(itemSchema);

        return new TurbineEntitySchema<T>(itemSchema, client);
    }

    public ITurbineTransact StartTransact()
    {
        return new TurbineTransact(client);
    }

    private sealed class TurbineEntitySchema<T> : ITurbineEntitySchema<T>
    {
        private readonly IAmazonDynamoDB client;
        private readonly ItemSchema<T> itemSchema;

        public TurbineEntitySchema(ItemSchema<T> itemSchema, IAmazonDynamoDB client)
        {
            this.itemSchema = itemSchema;
            this.client = client;
        }

        public Task UpsertAsync(T entity)
        {
            return new Put<T>(itemSchema, client).UpsertAsync(entity);
        }

        public Task UpsertAsync(IEnumerable<T> entities)
        {
            return new Put<T>(itemSchema, client).UpsertAsync(entities);
        }

        public Task<bool> PutIfNotExistsAsync(T entity)
        {
            return new Put<T>(itemSchema, client).PutIfNotExistsAsync(entity);
        }

        public Task DeleteAsync(string pk, string sk)
        {
            return new Delete<T>(itemSchema, new QueryBuilderPk<T>(itemSchema, client), client).DeleteAsync(pk, sk);
        }

        public Task DeleteAsync(string pk, SortKey sk)
        {
            return new Delete<T>(itemSchema, new QueryBuilderPk<T>(itemSchema, client), client).DeleteAsync(pk, sk);
        }

        public Task DeleteAsync(T item)
        {
            return new Delete<T>(itemSchema, new QueryBuilderPk<T>(itemSchema, client), client).DeleteAsync(item);
        }
    }

    private class TurbineTransact : ITurbineTransact
    {
        private readonly IAmazonDynamoDB client;
        private readonly List<TransactWriteItem> transactItems = new();
        private bool hasCommitted;

        public TurbineTransact(IAmazonDynamoDB client)
        {
            this.client = client;
        }

        public ITurbineTransactEntitySchema<T> WithSchema<T>(ItemSchema<T> itemSchema)
        {
            return new TurbineTransactEntitySchema<T>(itemSchema, this, AddToTransaction);
        }

        public async Task<bool> Commit()
        {
            hasCommitted = true;

            try
            {
                var transactWriteRequest = new TransactWriteItemsRequest
                {
                    TransactItems = transactItems
                };

                _ = await client.TransactWriteItemsAsync(transactWriteRequest);

                return true;
            }
            catch (TransactionCanceledException)
            {
                return false;
            }
        }

        public async ValueTask DisposeAsync()
        {
            if (hasCommitted)
            {
                return;
            }

            await Commit();
        }

        private void AddToTransaction(TransactWriteItem write)
        {
            if (transactItems.Count == 25)
            {
                throw new TurbineException("Cannot add to transaction as there is already 25 items.");
            }

            transactItems.Add(write);
        }

        private class TurbineTransactEntitySchema<T> : ITurbineTransactEntitySchema<T>
        {
            private readonly ItemSchema<T> itemSchema;
            private readonly Action<TransactWriteItem> transactWrite;
            private readonly ITurbineTransact turbineTransact;

            public TurbineTransactEntitySchema(
                ItemSchema<T> itemSchema,
                ITurbineTransact turbineTransact,
                Action<TransactWriteItem> transactWrite)
            {
                this.itemSchema = itemSchema;
                this.turbineTransact = turbineTransact;
                this.transactWrite = transactWrite;
            }

            public ITurbineTransact Upsert(T entity)
            {
                return new TransactPut<T>(itemSchema, turbineTransact, transactWrite).Upsert(entity);
            }

            public ITurbineTransact Upsert(T entity, Condition condition)
            {
                return new TransactPut<T>(itemSchema, turbineTransact, transactWrite).Upsert(entity, condition);
            }

            public ITurbineTransact Upsert(IEnumerable<T> entities)
            {
                return new TransactPut<T>(itemSchema, turbineTransact, transactWrite).Upsert(entities);
            }

            public ITurbineTransact Upsert(IEnumerable<T> entities, Condition condition)
            {
                return new TransactPut<T>(itemSchema, turbineTransact, transactWrite).Upsert(entities, condition);
            }

            public ITurbineTransact Delete(string pk, string sk)
            {
                return new TransactDelete<T>(itemSchema, turbineTransact, transactWrite).Delete(pk, sk);
            }

            public ITurbineTransact Delete(string pk, string sk, Condition condition)
            {
                return new TransactDelete<T>(itemSchema, turbineTransact, transactWrite).Delete(pk, sk, condition);
            }

            public ITurbineTransact Delete(T item)
            {
                return new TransactDelete<T>(itemSchema, turbineTransact, transactWrite).Delete(item);
            }

            public ITurbineTransact Delete(T item, Condition condition)
            {
                return new TransactDelete<T>(itemSchema, turbineTransact, transactWrite).Delete(item, condition);
            }

            public ITurbineTransactEntitySchema<T> Condition(T item, Condition condition)
            {
                ArgumentNullException.ThrowIfNull(item);

                var check = new TransactWriteItem
                {
                    ConditionCheck = condition.ToConditionCheck(itemSchema.TableSchema.TableName)
                };

                check.ConditionCheck!.Key = new Dictionary<string, AttributeValue>
                {
                    { itemSchema.TableSchema.Pk, new(itemSchema.GetPk(item)) },
                    { itemSchema.TableSchema.Sk, new(itemSchema.GetSk(item)) }
                };

                transactWrite(check);

                return this;
            }

            private ITurbineTransactEntitySchema<T> ExistsCheck(T item, Condition condition)
            {
                ArgumentNullException.ThrowIfNull(item);

                var check = condition.ToConditionCheck(itemSchema.TableSchema.TableName);

                if (check is null)
                {
                    return this;
                }

                check.Key = new Dictionary<string, AttributeValue>
                {
                    { itemSchema.TableSchema.Pk, new AttributeValue(itemSchema.GetPk(item)) },
                    { itemSchema.TableSchema.Sk, new AttributeValue(itemSchema.GetSk(item)) }
                };

                transactWrite(new TransactWriteItem
                {
                    ConditionCheck = check
                });

                return this;
            }
        }
    }
}