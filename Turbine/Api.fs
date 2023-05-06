namespace Turbine

open Turbine
open System.Threading.Tasks

type TurbineException(message: string) =
    inherit System.Exception(message)

type IQuery<'T> =
    abstract member QueryAsync: unit -> Task<'T>

type IQueryBuilderSk<'T> =
    abstract member WithSk: SortKey -> IQuery<'T>

type IQueryBuilderPk<'T> =
    abstract member WithPk<'TProperty> : value: string -> IQueryBuilderSk<'T>
