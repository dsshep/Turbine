namespace rec Turbine

open System
open System.Collections.Generic
open System.Linq.Expressions

type EntitySchema<'T>(schema: Schema, partitionMap: Dictionary<Type, string>, sortMap: Dictionary<Type, string>) =

    member this.PkMapping<'TProperty>(partitionKey: Expression<Func<'T, 'TProperty>>) =
        partitionMap[typeof<'T>] <- Reflection.getPropertyName partitionKey
        this

    member this.SkMapping<'TProperty>(partitionKey: Expression<Func<'T, 'TProperty>>) =
        sortMap[typeof<'T>] <- Reflection.getPropertyName partitionKey
        this

    member this.AddEntity<'T>() =
        EntitySchema<'T>(schema, partitionMap, sortMap)

    member this.Schema = schema


type Schema =
    val internal PartitionMap: Dictionary<Type, string>
    val internal SortMap: Dictionary<Type, string>
    val internal TableName: string
    val internal Sk: string
    val internal Pk: string

    new(tableName: string) =
        { TableName = tableName
          Pk = "pk"
          Sk = "sk"
          PartitionMap = Dictionary<Type, string>()
          SortMap = Dictionary<Type, string>() }

    new(tableName: string, pk: string, sk: string) =
        { TableName = tableName
          Pk = pk
          Sk = sk
          PartitionMap = Dictionary<Type, string>()
          SortMap = Dictionary<Type, string>() }

    member this.AddEntity<'T>() =
        EntitySchema<'T>(this, this.PartitionMap, this.SortMap)
