namespace Turbine

open Turbine
open System.Threading.Tasks

type IQuery<'T> =
    abstract member QueryAsync: unit -> Task<'T>

    abstract member ToListAsync: unit -> Task<System.Collections.Generic.List<'T>>

type IQueryBuilderSk<'T> =
    abstract member WithSk: SortKey -> IQuery<'T>

type IQueryBuilderPk<'T> =
    abstract member WithPk<'TProperty> : value: string -> IQueryBuilderSk<'T>
