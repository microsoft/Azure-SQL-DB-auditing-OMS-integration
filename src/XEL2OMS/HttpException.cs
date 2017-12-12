// -----------------------------------------------------------------------
// <copyright file="HttpException.cs" company="Microsoft">
//     Copyright (c) Microsoft Corporation. All rights reserved.
// </copyright>
// -----------------------------------------------------------------------

namespace XEL2OMS
{
    using System;
    using System.Runtime.Serialization;

    /// <summary>
    /// The exception that is thrown when a HTTP error occurs.
    /// </summary>
    [Serializable]
    internal class HttpException : Exception
    {
        private int statusCode;
        private string v;

        /// <summary>
        /// Initializes a new instance of the <see cref="HttpException"/> class.
        /// </summary>
        public HttpException()
        {
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="HttpException"/> class.
        /// </summary>
        /// <param name="message">The message that describes the error.</param>
        public HttpException(string message) : base(message)
        {
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="CommunicationException"/> class.
        /// </summary>
        /// <param name="message">The error message that explains the reason for the exception.</param>
        /// <param name="innerException">The exception that is the cause of the current exception, or a null reference (Nothing in Visual Basic) if no inner exception is specified.</param>
        public HttpException(string message, Exception innerException) : base(message, innerException)
        {
        }

        public HttpException(int statusCode, string v)
        {
            this.statusCode = statusCode;
            this.v = v;
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="CommunicationException"/> class.
        /// </summary>
        /// <param name="info">The <see cref="T:System.Runtime.Serialization.SerializationInfo" /> that holds the serialized object data about the exception being thrown.</param>
        /// <param name="context">The <see cref="T:System.Runtime.Serialization.StreamingContext" /> that contains contextual information about the source or destination.</param>
        protected HttpException(SerializationInfo info, StreamingContext context) : base(info, context)
        {
        }
    }
}