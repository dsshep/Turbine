namespace Turbine

open System
open System.Collections.Generic

[<AutoOpen>]
module Helpers =
    let keyValue key value = KeyValuePair(key, value)


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
