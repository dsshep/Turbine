namespace Turbine

open System

type TurbineException(message: string, ?innerException: Exception) =
    inherit Exception(message, (defaultArg innerException null))
