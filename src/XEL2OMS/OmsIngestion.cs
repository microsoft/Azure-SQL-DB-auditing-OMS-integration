// -----------------------------------------------------------------------
// <copyright file="OmsIngestion.cs" company="Microsoft">
//     Copyright (c) Microsoft Corporation. All rights reserved.
// </copyright>
// -----------------------------------------------------------------------

namespace XEL2OMS
{
    using System;
    using System.Configuration;
    using System.Diagnostics;
    using System.IO;
    using System.Net;
    using System.Text;
    using System.Threading.Tasks;
    using Microsoft.Practices.EnterpriseLibrary.TransientFaultHandling;

    public class OmsIngestion
    {
        private readonly string m_CustomerId;
        private readonly string m_SharedKey;
        private readonly TraceSource m_Tracer;
        private readonly RetryPolicy m_Retry;

        public OmsIngestion(TraceSource tracer, string customerId, string sharedKey)
        {
            // Check the shared key is of a valid format
            Convert.FromBase64String(sharedKey);

            m_CustomerId = customerId;
            m_SharedKey = sharedKey;
            m_Tracer = tracer;
            m_Retry = RetryPolicy.DefaultFixed;
        }


        public async Task SendAsync(string requestBody)
        {
            var method = "POST";
            var contentType = "application/json";
            var date = DateTime.UtcNow.ToString("r");

            Stopwatch stopwatch = new Stopwatch();
            stopwatch.Start();

            string address = $"https://{m_CustomerId}.{ConfigurationManager.AppSettings["OmsEndpointAddress"]}/api/logs?api-version=2016-04-01";
            Uri uriAddress = new Uri(address);

            byte[] payload = Encoding.UTF8.GetBytes(requestBody);

            var signature = GetSignature(date, payload.Length);

            HttpWebRequest request = (HttpWebRequest)WebRequest.Create(uriAddress);

            request.Method = method;
            request.ContentType = contentType;
            request.ContentLength = payload.Length;
            request.Headers.Add(HttpRequestHeader.Authorization, signature);
            request.Headers.Add("Log-Type", "SQLAuditLog");
            request.Headers.Add("x-ms-date", date);
            request.Headers.Add("time-generated-field", "EventTime");

            m_Tracer.TraceEvent(TraceEventType.Information, 0, "Sending chunk ({0} bytes) to: {1}", payload.Length, uriAddress);

            // send request over the network
            using (Stream dataStream = await m_Retry.ExecuteAsync(() => request.GetRequestStreamAsync()))
            {
                await m_Retry.ExecuteAsync(() => dataStream.WriteAsync(payload, 0, payload.Length));
            }

            RestResponse response = await m_Retry.ExecuteAsync(() => GetResponse(request, stopwatch, payload.Length));

            if (!(response.StatusCode >= HttpStatusCode.OK && response.StatusCode < HttpStatusCode.Ambiguous))
            {
                m_Tracer.TraceEvent(TraceEventType.Error, 0, "{0} to {1} failed with error {2}", method, address, response.StatusDescription);
                throw new HttpException((int)response.StatusCode, $"{method} to {address} failed");
            }
        }

        private async Task<RestResponse> GetResponse(HttpWebRequest request, Stopwatch stopwatch, int payloadLength, int timeoutMilliseconds = 0)
        {
            WebResponse response = null;
            // get the response
            try
            {
                if (timeoutMilliseconds == 0)
                {
                    response = await m_Retry.ExecuteAsync(() => request.GetResponseAsync());
                }
                else
                {
                    var responseTask = m_Retry.ExecuteAsync(() => request.GetResponseAsync());
                    bool requestCompleted = responseTask.Wait(timeoutMilliseconds);
                    if (!requestCompleted)
                    {
                        m_Tracer.TraceEvent(TraceEventType.Error, 0, "GET request to {0} had reached timeout of {1} ms", request.RequestUri, timeoutMilliseconds);
                        request.Abort();
                        throw new HttpException((int)HttpStatusCode.RequestTimeout, string.Format("GET to {0} failed on timeout", request.RequestUri));
                    }
                    response = responseTask.Result;
                }
                HttpWebResponse httpResponse = (HttpWebResponse)response;
                string responseFromServer = null;
                using (Stream dataStream = response.GetResponseStream())
                {
                    StreamReader reader = new StreamReader(dataStream);
                    responseFromServer = await m_Retry.ExecuteAsync(() => reader.ReadToEndAsync());
                }

                stopwatch.Stop();

                return new RestResponse()
                {
                    SentBytes = payloadLength,
                    StatusDescription = httpResponse.StatusDescription,
                    ResponseFromServer = responseFromServer,
                    StatusCode = httpResponse.StatusCode,
                    Duration = stopwatch.Elapsed
                };
            }
            finally
            {
                if (response != null)
                {
                    response.Dispose();
                }
            }
        }

        private string GetSignature(string date, int contentLength)
        {
            var xHeaders = $"x-ms-date:{date}";
            var stringToHash = $"POST\n{contentLength}\napplication/json\n{xHeaders}\n/api/logs";

            var bytesToHash = Encoding.UTF8.GetBytes(stringToHash);
            var keyBytes = Convert.FromBase64String(m_SharedKey);

            using (var sha256 = new System.Security.Cryptography.HMACSHA256(keyBytes))
            {
                var calculatedHash = sha256.ComputeHash(bytesToHash);
                var encodedHash = Convert.ToBase64String(calculatedHash);

                return $"SharedKey {m_CustomerId}:{encodedHash}";
            }
        }

    }
}