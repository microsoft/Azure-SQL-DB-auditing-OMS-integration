// -----------------------------------------------------------------------
// <copyright file="HttpException.cs" company="Microsoft">
//     Copyright (c) Microsoft Corporation. All rights reserved.
// </copyright>
// -----------------------------------------------------------------------

namespace XEL2OMS
{
    using System;
    using System.Runtime.Serialization;

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