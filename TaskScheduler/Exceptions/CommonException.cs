namespace TaskScheduler.Exceptions
{
    public class CommonException : System.Exception
    {
        private static readonly string _defaultMessage = "О нет, что-то пошло не так!";

        protected CommonException() : base(_defaultMessage) { }

        protected CommonException(string userMessage) : base(_defaultMessage + "\n" + userMessage) { }

        protected CommonException(string userMessage, System.Exception innerException) : base(userMessage, innerException) { }

        protected CommonException(string argumentName, string functionName) 
            : base("О нет, что-то пошло не так! Причина: аргумент " + argumentName + " переданный в функцию " + functionName) { }
    }
}