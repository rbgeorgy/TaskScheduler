namespace TaskScheduler.Exceptions
{
    public class NullArgumentException : CommonException
    {
        public NullArgumentException(string argumentName, string functionName) 
            : base(argumentName + ", переданный в функцию " + functionName + ", не может быть null!") { }
    }
}