// -----------------------------------------------------------------------
// <copyright file="RestResponse.cs" company="Microsoft">
//     Copyright (c) Microsoft Corporation. All rights reserved.
// </copyright>
// -----------------------------------------------------------------------

namespace XEL2OMS
{
    using System;
    using System.Globalization;
    using System.Net;

    public class RestResponse
    {
        public int SentBytes { get; set; }

        public string StatusDescription { get; set; }

        public string ResponseFromServer { get; set; }

        public HttpStatusCode StatusCode { get; set; }

        public TimeSpan Duration { get; set; }

        public override string ToString()
        {
            string escapedResponse = ResponseFromServer.Replace("{", "{{").Replace("}", "}}");
            return string.Format(
                CultureInfo.InvariantCulture,
                "Sent {0} bytes, status = {1}, status code = {2}, duration = {3}, response = '{4}'",
                SentBytes,
                StatusDescription,
                StatusCode,
                Duration,
                escapedResponse);
        }
    }
}