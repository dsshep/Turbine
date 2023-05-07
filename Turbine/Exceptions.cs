namespace Turbine;

public class TurbineException : Exception
{
    public TurbineException(string message)
        : base(message)
    {
    }

    public TurbineException(string message, Exception? innerExceptions)
        : base(message, innerExceptions)
    {
    }
}