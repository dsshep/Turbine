namespace Turbine

open System
open System.Collections.Generic
open Amazon.DynamoDBv2.Model

module internal Option =
    let defaultWithOpt<'T> (defThunk: unit -> 'T option) (opt: 'T option) : 'T option =
        match opt with
        | Some _ -> opt
        | None ->
            let o2 = defThunk ()

            match defThunk () with
            | Some _ -> o2
            | None -> None

module internal Dict =
    let tryFindByStringKey (key: string) (dict: IReadOnlyDictionary<string, _>) =
        dict
        |> Seq.tryFind (fun (KeyValue(k, _)) -> k.Equals(key, StringComparison.OrdinalIgnoreCase))

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

        let props =
            t.GetProperties() |> Array.filter (fun p -> p.GetSetMethod(true) <> null)

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
                    |> Dict.tryFindByStringKey schema.Pk
                    |> Option.map (fun (KeyValue(_, av)) ->
                        let value = Reflection.toNetType propType av
                        prop.SetValue(instance, value))
                    |> ignore

                if propName.Equals(sortKeyProp, StringComparison.OrdinalIgnoreCase) then
                    attributes
                    |> Dict.tryFindByStringKey schema.Sk
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
            let instanceOpt =
                ctors
                |> Seq.map (fun c -> (c, c.GetParameters()))
                |> Seq.sortByDescending (fun (_, p) -> p.Length)
                |> Seq.map (fun (ctor, parameters) ->
                    let args =
                        parameters
                        |> Seq.map (fun p ->
                            attributes
                            |> Seq.tryFind (fun kvp -> kvp.Key.Equals(p.Name, StringComparison.OrdinalIgnoreCase))
                            |> Option.defaultWithOpt (fun () ->
                                let partitionKeyProp =
                                    match schema.PartitionMap.TryGetValue entityType with
                                    | true, v -> v
                                    | _, _ -> schema.Pk

                                let sortKeyProp =
                                    match schema.SortMap.TryGetValue entityType with
                                    | true, v -> v
                                    | _, _ -> schema.Sk

                                if p.Name.Equals(partitionKeyProp, StringComparison.OrdinalIgnoreCase) then
                                    attributes |> Dict.tryFindByStringKey schema.Pk

                                elif p.Name.Equals(sortKeyProp, StringComparison.OrdinalIgnoreCase) then
                                    attributes |> Dict.tryFindByStringKey schema.Sk
                                else
                                    None)
                            |> Option.map (fun kvp -> Reflection.toNetType p.ParameterType kvp.Value)
                            |> Option.defaultValue (
                                if p.ParameterType.IsValueType then
                                    Activator.CreateInstance p.ParameterType
                                else
                                    null
                            ))
                        |> Seq.toArray

                    try
                        ctor.Invoke(args) |> Ok
                    with e ->
                        Error e)
                |> Seq.tryHead

            match instanceOpt with
            | Some(Ok o) -> o |> unbox<'T>
            | Some(Error exn) -> raise (TurbineException($"Could not create instance of {entityType.Name}.", exn))
            | _ -> raise (TurbineException($"Could not create instance of {entityType.Name}."))
