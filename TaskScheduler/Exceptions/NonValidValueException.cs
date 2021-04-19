namespace TaskScheduler.Exceptions
{
    public class NonValidValueException : CommonException
    {
        public NonValidValueException(string argumentName, string functionName, object value)
            : base(argumentName + ", переданный в функцию " + functionName + ", не может быть " + value +"!") { }
    }
}