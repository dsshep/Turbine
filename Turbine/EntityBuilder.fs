namespace Turbine

open System
open System.Collections.Generic
open Amazon.DynamoDBv2.Model

module internal EntityBuilder =

    let private hydrateFromProps
        (
            t: Type,
            instance: obj,
            attributes: IReadOnlyDictionary<string, AttributeValue>,
            schema: Schema
        ) =

        let partitionKeyProp =
            match schema.PartitionMap.TryGetValue t with
            | true, v -> v
            | _, _ -> schema.Pk

        let sortKeyProp =
            match schema.SortMap.TryGetValue t with
            | true, v -> v
            | _, _ -> schema.Sk

        let props = t.GetProperties()

        for prop in props do
            let propName = prop.Name
            let propType = prop.PropertyType

            let attributeValue =
                attributes
                |> Seq.tryFind (fun kvp -> kvp.Key.Equals(propName, StringComparison.OrdinalIgnoreCase))

            match attributeValue with
            | Some(KeyValue(_, av)) ->
                let value = Reflection.toNetType propType av
                prop.SetValue(instance, value)
            | None ->
                if propName.Equals(partitionKeyProp, StringComparison.OrdinalIgnoreCase) then
                    attributes
                    |> Seq.tryFind (fun kvp -> kvp.Key.Equals(schema.Pk, StringComparison.OrdinalIgnoreCase))
                    |> Option.map (fun (KeyValue(_, av)) ->
                        let value = Reflection.toNetType propType av
                        prop.SetValue(instance, value))
                    |> ignore

                if propName.Equals(sortKeyProp, StringComparison.OrdinalIgnoreCase) then
                    attributes
                    |> Seq.tryFind (fun kvp -> kvp.Key.Equals(schema.Sk, StringComparison.OrdinalIgnoreCase))
                    |> Option.map (fun (KeyValue(_, av)) ->
                        let value = Reflection.toNetType propType av
                        prop.SetValue(instance, value))
                    |> ignore

        instance

    let hydrateEntity<'T> (schema: Schema, attributes: IReadOnlyDictionary<string, AttributeValue>) : 'T =
        let entityType = typeof<'T>
        let ctors = entityType.GetConstructors()

        if ctors |> Seq.exists (fun c -> c.GetParameters().Length = 0) then
            let instance = Activator.CreateInstance(entityType)
            hydrateFromProps (entityType, instance, attributes, schema) |> unbox<'T>
        else
            failwith "not implemented"
