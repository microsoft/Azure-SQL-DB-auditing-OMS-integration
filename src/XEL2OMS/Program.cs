using Microsoft.SqlServer.XEvent.Linq;
using Microsoft.WindowsAzure;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Blob;
using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Diagnostics.CodeAnalysis;
using System.IO;
using System.Linq;
using System.Net;
using System.Threading.Tasks;
using Microsoft.Practices.EnterpriseLibrary.TransientFaultHandling;

namespace XEL2OMS
{
    using Microsoft.Azure;
    using databaseStateDictionary = Dictionary<string, SubfolderState>;
    using serverStateDictionary = Dictionary<string, Dictionary<string, SubfolderState>>;
    using StateDictionary = Dictionary<string, Dictionary<string, Dictionary<string, SubfolderState>>>;

    public static class Program
    {
        private static TraceSource s_consoleTracer = new TraceSource("OMS");
        private static int totalLogs = 0;
        private const int DefaultRetryCount = 3;
        private static readonly string StateFileName = Path.Combine(GetLocalStorageFolder(), "states.json");
        private static readonly StateDictionary StatesList = GetStates(StateFileName);
        private static List<string> auditLogProcessingFailures = new List<string>();

        [SuppressMessage("Microsoft.Design", "CA1031:DoNotCatchGeneralExceptionTypes", Justification = "Should catch any exception")]
        private static List<SQLAuditLog> ParseXEL(QueryableXEventData events, int eventNumber, string blobName)
        {
            int count = 0;
            List<SQLAuditLog> list = new List<SQLAuditLog>();
            foreach (var currentEvent in events)
            {
                if (count >= eventNumber)
                {
                    try
                    {
                        SQLAuditLog currentLog = new SQLAuditLog(currentEvent);
                        list.Add(currentLog);
                    }
                    catch (Exception ex)
                    {
                        s_consoleTracer.TraceEvent(TraceEventType.Error, 0, "Error: Could not send event number: {0}, from blob: {1}", count, blobName, ex);
                    }
                }

                count++;
            }
            return list;
        }

        private static string GetLocalStorageFolder()
        {
            return Environment.GetEnvironmentVariable("WEBROOT_PATH") ?? "";
        }

        public static IEnumerable<List<T>> Chunk<T>(this List<T> source, int chunkSize)
        {
            int offset = 0;
            while (offset < source.Count)
            {
                yield return source.GetRange(offset, Math.Min(source.Count - offset, chunkSize));
                offset += chunkSize;
            }
        }

        private static void PrintHeaders(RequestEventArgs e)
        {
            if (e.RequestInformation.HttpStatusCode != (int)HttpStatusCode.OK)
            {
                e.Request.Headers.Remove("Authorization");
                if (e.Response != null)
                {
                    s_consoleTracer.TraceEvent(TraceEventType.Error, 0, "Dumpping headers: Failed processing: {0}. Reason: {1} \nHTTP Request:\n{2}HTTP Response:\n{3}", e.Request.Address, e.RequestInformation.Exception, e.Request.Headers, e.Response.Headers);
                }
                else
                {
                    s_consoleTracer.TraceEvent(TraceEventType.Error, 0, "Dumpping headers: Failed processing: {0}. Reason: {1} \nHTTP Request:\n{2}", e.Request.Address, e.RequestInformation.Exception, e.Request.Headers);
                }
            }
        }

        private static async Task<int> SendBlobToOMS(CloudBlob blob, int eventNumber, OMSIngestionApi oms)
        {
            RetryPolicy retryPolicy = new RetryPolicy(RetryPolicy.DefaultFixed.ErrorDetectionStrategy, DefaultRetryCount);

            s_consoleTracer.TraceEvent(TraceEventType.Information, 0, "Processing: {0}", blob.Uri);

            string fileName = Path.Combine(GetLocalStorageFolder(), Path.GetRandomFileName() + ".xel");
            try
            {
                OperationContext operationContext = new OperationContext();
                operationContext.RequestCompleted += (sender, e) => PrintHeaders(e);
                await retryPolicy.ExecuteAsync((() => blob.DownloadToFileAsync(fileName, FileMode.OpenOrCreate, null, null, operationContext)));
                List<SQLAuditLog> list;
                using (var events = new QueryableXEventData(fileName))
                {
                    list = ParseXEL(events, eventNumber, blob.Name);
                }
                IEnumerable<List<SQLAuditLog>> chunkedList = list.Chunk(10000);
                foreach (List<SQLAuditLog> chunk in chunkedList)
                {
                    var jsonList = JsonConvert.SerializeObject(chunk);
                    await oms.SendOMSApiIngestionFile(jsonList);
                    eventNumber += chunk.Count;
                    totalLogs += chunk.Count;
                }
            }
            catch (Exception e)
            {
                s_consoleTracer.TraceEvent(TraceEventType.Error, 0, "Failed processing: {0}. Reason: {1}", blob.Uri, e);
                throw;
            }
            finally
            {
                try
                {
                    File.Delete(fileName);
                }
                catch (Exception e)
                {
                    s_consoleTracer.TraceEvent(TraceEventType.Information, 0, "Was not able to delete file: {0}. Reason: {1}", fileName, e.Message);
                }
            }
            s_consoleTracer.TraceEvent(TraceEventType.Information, 0, "Done processing: {0}", blob.Uri);
            return eventNumber;
        }

        private static void SendLogsFromSubfolder(CloudBlobDirectory subfolder, databaseStateDictionary databaseState, OMSIngestionApi oms)
        {
            int nextEvent = 0;
            int eventNumber = 0;
            int datesCompareResult = -1;
            string currentDate = null;

            s_consoleTracer.TraceEvent(TraceEventType.Information, 0, "Processing sub folder: {0}", subfolder.Prefix);

            string subfolderName = new DirectoryInfo(subfolder.Prefix).Name;
            IEnumerable<CloudBlobDirectory> dateFolders = GetSubDirectories(subfolderName, subfolder, databaseState);
            var subfolderState = databaseState[subfolderName];
            string lastBlob = subfolderState.BlobName;
            DateTimeOffset? lastModified = subfolderState.LastModified;
            try
            {
                foreach (var dateFolder in dateFolders)
                {
                    currentDate = new DirectoryInfo(dateFolder.Prefix).Name;
                    datesCompareResult = string.Compare(currentDate, subfolderState.Date, StringComparison.OrdinalIgnoreCase);
                    //current folder is older than last state
                    if (datesCompareResult < 0)
                    {
                        continue;
                    }

                    var tasks = new List<Task<int>>();

                    IEnumerable<CloudBlob> cloudBlobs = dateFolder.ListBlobs(useFlatBlobListing: true).OfType<CloudBlob>()
                        .Where(b => b.Name.EndsWith(".xel", StringComparison.OrdinalIgnoreCase)).ToList();

                    foreach (var blob in cloudBlobs)
                    {
                        string blobName = new FileInfo(blob.Name).Name;

                        if (datesCompareResult == 0)
                        {
                            int blobsCompareResult = string.Compare(blobName, subfolderState.BlobName, StringComparison.OrdinalIgnoreCase);
                            //blob is older than last state
                            if (blobsCompareResult < 0)
                            {
                                continue;
                            }

                            if (blobsCompareResult == 0)
                            {
                                if (blob.Properties.LastModified == subfolderState.LastModified)
                                {
                                    continue;
                                }
                                eventNumber = subfolderState.EventNumber;
                            }
                        }

                        tasks.Add(SendBlobToOMS(blob, eventNumber, oms));

                        lastBlob = blobName;
                        lastModified = blob.Properties.LastModified;
                        eventNumber = 0;
                    }

                    Task.WaitAll(tasks.ToArray());
                    if (tasks.Count > 0)
                    {
                        nextEvent = tasks.Last().Result;
                    }
                    subfolderState.BlobName = lastBlob;
                    subfolderState.LastModified = lastModified;
                    if (datesCompareResult >= 0)
                    {
                        subfolderState.Date = currentDate;
                    }

                    subfolderState.EventNumber = nextEvent;
                    File.WriteAllText(StateFileName, JsonConvert.SerializeObject(StatesList));
                }
                s_consoleTracer.TraceEvent(TraceEventType.Information, 0, "Done processing sub folder: {0}", subfolder.Prefix);
            }
            catch (Exception e)
            {
                s_consoleTracer.TraceEvent(TraceEventType.Error, 0, "Failed processing sub folder: {0}. Reason: {1}", subfolder.Prefix, e);
                UpdateFailuresLog(subfolder.Prefix, e);
            }
        }

        private static void SendLogsFromDatabase(CloudBlobDirectory databaseDirectory, serverStateDictionary serverState, OMSIngestionApi oms)
        {
            s_consoleTracer.TraceEvent(TraceEventType.Information, 0, "Processing audit logs for database: {0}", databaseDirectory.Prefix);

            try
            {
                string databaseName = new DirectoryInfo(databaseDirectory.Prefix).Name;
                IEnumerable<CloudBlobDirectory> subfolders = GetSubDirectories(databaseName, databaseDirectory, serverState);

                foreach (var subfolder in subfolders)
                {
                    SendLogsFromSubfolder(subfolder, serverState[databaseName], oms);
                }

                s_consoleTracer.TraceEvent(TraceEventType.Information, 0, "Done processing audit logs for database: {0}", databaseDirectory.Prefix);
            }
            catch (Exception e)
            {
                s_consoleTracer.TraceEvent(TraceEventType.Information, 0, "Failed processing audit logs for database: {0}. Reason: {1}", databaseDirectory.Prefix, e);
                UpdateFailuresLog(databaseDirectory.Prefix, e);
            }

        }

        private static void SendLogsFromServer(CloudBlobDirectory serverDirectory, OMSIngestionApi oms)
        {
            s_consoleTracer.TraceEvent(TraceEventType.Information, 0, "Processing audit logs for server: {0}", serverDirectory.Prefix);
            try
            {
                string serverName = new DirectoryInfo(serverDirectory.Prefix).Name;
                IEnumerable<CloudBlobDirectory> databases = GetSubDirectories(serverName, serverDirectory, StatesList);

                foreach (var database in databases)
                {
                    SendLogsFromDatabase(database, StatesList[serverName], oms);
                }

                s_consoleTracer.TraceEvent(TraceEventType.Information, 0, "Done processing audit logs for server: {0}", serverDirectory.Prefix);
            }
            catch (Exception e)
            {
                s_consoleTracer.TraceEvent(TraceEventType.Information, 0, "Failed processing audit logs for server: {0}. Reason: {1}", serverDirectory.Prefix, e);
                UpdateFailuresLog(serverDirectory.Prefix, e);
            }
        }

        private static IEnumerable<CloudBlobDirectory> GetSubDirectories<T>(string directoryName, CloudBlobDirectory directory, IDictionary<string, T> dictionary) where T : new()
        {
            if (!dictionary.ContainsKey(directoryName))
            {
                dictionary.Add(directoryName, new T());
            }

            return directory.ListBlobs().OfType<CloudBlobDirectory>().ToList();
        }

        private static StateDictionary GetStates(string fileName)
        {
            StateDictionary statesList;
            if (!File.Exists(fileName))
            {
                statesList = new StateDictionary();
            }
            else
            {
                using (StreamReader file = File.OpenText(fileName))
                {
                    JsonSerializer serializer = new JsonSerializer();
                    statesList = (StateDictionary)serializer.Deserialize(file, typeof(StateDictionary));
                }
            }

            return statesList;
        }

        private static void UpdateFailuresLog(string resource, Exception ex)
        {
            string failureMessage = string.Format("Failed processing audit logs for: {0}. Reason: {1}", resource, ex.Message);
            auditLogProcessingFailures.Add(failureMessage);
        }

        static void Main()
        {
            string connectionString = CloudConfigurationManager.GetSetting("ConnectionString");
            string containerName = "sqldbauditlogs";
            string customerId = CloudConfigurationManager.GetSetting("omsWorkspaceId");
            string sharedKey = CloudConfigurationManager.GetSetting("omsWorkspaceKey");

            CloudStorageAccount storageAccount;

            try
            {
                var oms = new OMSIngestionApi(s_consoleTracer, customerId, sharedKey);

                if (CloudStorageAccount.TryParse(connectionString, out storageAccount) == false)
                {
                    s_consoleTracer.TraceEvent(TraceEventType.Error, 0, "Connection string can't be parsed: {0}",
                        connectionString);
                    return;
                }

                CloudBlobClient blobClient = storageAccount.CreateCloudBlobClient();
                CloudBlobContainer container = blobClient.GetContainerReference(containerName);

                s_consoleTracer.TraceInformation("Sending logs to OMS");

                IEnumerable<CloudBlobDirectory> servers = container.ListBlobs().OfType<CloudBlobDirectory>().ToList();
                foreach (var server in servers)
                {
                    SendLogsFromServer(server, oms);
                }

                File.WriteAllText(StateFileName, JsonConvert.SerializeObject(StatesList));
                s_consoleTracer.TraceInformation("{0} logs were successfully sent", totalLogs);
            }
            catch (FormatException formatException)
            {
                s_consoleTracer.TraceEvent(TraceEventType.Error, 0, "The OMS workspace key is bad formatted. Error: {0}", formatException);
            }
            catch (Exception ex)
            {
                s_consoleTracer.TraceEvent(TraceEventType.Error, 0, "Error: {0}", ex);
            }
            finally
            {
                if (auditLogProcessingFailures.Count > 0)
                {
                    s_consoleTracer.TraceInformation("Processing audit logs of the following resources failed during the operation:\n{0}", string.Join(Environment.NewLine, auditLogProcessingFailures));
                }
            }
        }
    }
}
