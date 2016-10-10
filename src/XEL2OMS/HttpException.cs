using System;
using System.Runtime.Serialization;

namespace XEL2OMS
{
    [Serializable]
    internal class HttpException : Exception
    {
        private int statusCode;
        private string v;

        public HttpException()
        {
        }

        public HttpException(string message) : base(message)
        {
        }

        public HttpException(string message, Exception innerException) : base(message, innerException)
        {
        }

        public HttpException(int statusCode, string v)
        {
            this.statusCode = statusCode;
            this.v = v;
        }

        protected HttpException(SerializationInfo info, StreamingContext context) : base(info, context)
        {
        }
    }
}