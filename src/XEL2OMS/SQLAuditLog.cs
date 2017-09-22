using Microsoft.SqlServer.XEvent.Linq;
using Newtonsoft.Json;
using System;
using System.Collections.Generic;

namespace XEL2OMS
{
    public class ClassTypeData
    {
        public string ClassTypeDescription { get; private set; }

        public string SecurableClassType { get; private set; }

        public ClassTypeData(string classTypeDescription, string securableClassType)
        {
            ClassTypeDescription = classTypeDescription;
            SecurableClassType = securableClassType;
        }
    }

    [JsonObject]
    public class SQLAuditLog
    {
        static Dictionary<int, string> ActionIdDictionary = new Dictionary<int, string>()
        {
            { 538989377, "ACCESS" },
            { 1280462913, "ADD MEMBER" },
            { 538987585, "ALTER" },
            { 1313033281, "ALTER CONNECTION" },
            { 1397902401, "ALTER RESOURCES" },
            { 1397967937, "ALTER SERVER STATE" },
            { 1414745153, "ALTER SETTINGS" },
            { 1381256257, "ALTER TRACE" },
            { 1380013904, "APPLICATION_ROLE_CHANGE_PASSWORD_GROUP" },
            { 1129534785, "AUDIT SESSION CHANGED" },
            { 1179866433, "AUDIT SHUTDOWN ON FAILURE" },
            { 1430343235, "AUDIT_CHANGE_GROUP" },
            { 1213486401, "AUTHENTICATE" },
            { 538984770, "BACKUP" },
            { 541868354, "BACKUP LOG" },
            { 1111773762, "BACKUP_RESTORE_GROUP" },
            { 541214540, "BROKER LOGIN" },
            { 1195525964, "BROKER_LOGIN_GROUP" },
            { 1329742913, "BULK ADMIN" },
            { 1111770956, "CHANGE DEFAULT DATABASE" },
            { 1196181324, "CHANGE DEFAULT LANGUAGE" },
            { 1196180291, "CHANGE LOGIN CREDENTIAL" },
            { 1396922192, "CHANGE OWN PASSWORD" },
            { 541284176, "CHANGE PASSWORD" },
            { 1196184405, "CHANGE USERS LOGIN" },
            { 1178686293, "CHANGE USERS LOGIN AUTO" },
            { 538988611, "CHECKPOINT" },
            { 538988355, "CONNECT" },
            { 1129599829, "COPY PASSWORD" },
            { 538989123, "CREATE" },
            { 1196182851, "CREDENTIAL MAP TO LOGIN" },
            { 1178681924, "DATABASE AUTHENTICATION FAILED" },
            { 1396785732, "DATABASE AUTHENTICATION SUCCEEDED" },
            { 541868612, "DATABASE LOGOUT" },
            { 541935436, "DATABASE MIRRORING LOGIN" },
            { 1111772749, "DATABASE_CHANGE_GROUP" },
            { 1279738180, "DATABASE_LOGOUT_GROUP" },
            { 1196246860, "DATABASE_MIRRORING_LOGIN_GROUP" },
            { 1329873729, "DATABASE_OBJECT_ACCESS_GROUP" },
            { 1329876557, "DATABASE_OBJECT_CHANGE_GROUP" },
            { 1329876820, "DATABASE_OBJECT_OWNERSHIP_CHANGE_GROUP" },
            { 1329877575, "DATABASE_OBJECT_PERMISSION_CHANGE_GROUP" },
            { 1111773263, "DATABASE_OPERATION_GROUP" },
            { 1111773012, "DATABASE_OWNERSHIP_CHANGE_GROUP" },
            { 1111773767, "DATABASE_PERMISSION_CHANGE_GROUP" },
            { 1346653773, "DATABASE_PRINCIPAL_CHANGE_GROUP" },
            { 1346653513, "DATABASE_PRINCIPAL_IMPERSONATION_GROUP" },
            { 1346651201, "DATABASE_ROLE_MEMBER_CHANGE_GROUP" },
            { 1128481348, "DBCC" },
            { 1195590212, "DBCC_GROUP" },
            { 538987588, "DELETE" },
            { 538976324, "DENY" },
            { 541284164, "DENY WITH CASCADE" },
            { 1094993740, "DISABLE" },
            { 538989124, "DROP" },
            { 1280462916, "DROP MEMBER" },
            { 1095059276, "ENABLE" },
            { 538990661, "EXECUTE" },
            { 538984792, "EXTERNAL ACCESS ASSEMBLY" },
            { 1179074884, "FAILED_DATABASE_AUTHENTICATION_GROUP" },
            { 1279674188, "FAILED_LOGIN_GROUP" },
            { 538989638, "FULLTEXT" },
            { 541545542, "FULLTEXT_GROUP" },
            { 541542220, "GLOBAL TRANSACTIONS LOGIN" },
            { 1195853644, "GLOBAL_TRANSACTIONS_LOGIN_GROUP" },
            { 538976327, "GRANT" },
            { 541546311, "GRANT WITH GRANT" },
            { 542133577, "IMPERSONATE" },
            { 538988105, "INSERT" },
            { 1179207500, "LOGIN FAILED" },
            { 1397311308, "LOGIN SUCCEEDED" },
            { 1195595600, "LOGIN_CHANGE_PASSWORD_GROUP" },
            { 542066508, "LOGOUT" },
            { 538988364, "LOGOUT_GROUP" },
            { 1129142096, "MUST CHANGE PASSWORD" },
            { 1296975692, "NAME CHANGE" },
            { 1196182862, "NO CREDENTIAL MAP TO LOGIN" },
            { 538988623, "OPEN" },
            { 1480939344, "PASSWORD EXPIRATION" },
            { 1280333648, "PASSWORD POLICY" },
            { 538985298, "RECEIVE" },
            { 538986066, "REFERENCES" },
            { 1397905232, "RESET OWN PASSWORD" },
            { 542267216, "RESET PASSWORD" },
            { 538989394, "RESTORE" },
            { 538976338, "REVOKE" },
            { 541284178, "REVOKE WITH CASCADE" },
            { 541546322, "REVOKE WITH GRANT" },
            { 542065473, "SCHEMA_OBJECT_ACCESS_GROUP" },
            { 542068301, "SCHEMA_OBJECT_CHANGE_GROUP" },
            { 542068564, "SCHEMA_OBJECT_OWNERSHIP_CHANGE_GROUP" },
            { 542069319, "SCHEMA_OBJECT_PERMISSION_CHANGE_GROUP" },
            { 538987603, "SELECT" },
            { 538988115, "SEND" },
            { 1313035859, "SERVER CONTINUE" },
            { 1146115667, "SERVER PAUSED" },
            { 1146312275, "SERVER SHUTDOWN" },
            { 1381193299, "SERVER STARTED" },
            { 1330859597, "SERVER_OBJECT_CHANGE_GROUP" },
            { 1330859860, "SERVER_OBJECT_OWNERSHIP_CHANGE_GROUP" },
            { 1330860615, "SERVER_OBJECT_PERMISSION_CHANGE_GROUP" },
            { 1448300623, "SERVER_OPERATION_GROUP" },
            { 1448301127, "SERVER_PERMISSION_CHANGE_GROUP" },
            { 1347636813, "SERVER_PRINCIPAL_CHANGE_GROUP" },
            { 1347636553, "SERVER_PRINCIPAL_IMPERSONATION_GROUP" },
            { 1347634241, "SERVER_ROLE_MEMBER_CHANGE_GROUP" },
            { 1448301651, "SERVER_STATE_CHANGE_GROUP" },
            { 1313624147, "SHOW PLAN" },
            { 1313953107, "SUBSCRIBE QUERY NOTIFICATION" },
            { 1397178692, "SUCCESSFUL_DATABASE_AUTHENTICATION_GROUP" },
            { 1146308428, "SUCCESSFUL_LOGIN_GROUP" },
            { 538988372, "TAKE OWNERSHIP" },
            { 1179595331, "TRACE AUDIT C2OFF" },
            { 1313813059, "TRACE AUDIT C2ON" },
            { 1095975252, "TRACE AUDIT START" },
            { 1347633492, "TRACE AUDIT STOP" },
            { 1195594324, "TRACE_CHANGE_GROUP" },
            { 542069332, "TRANSFER" },
            { 542463824, "UNLOCK ACCOUNT" },
            { 538989912, "UNSAFE ASSEMBLY" },
            { 538988629, "UPDATE" },
            { 1430340693, "USER DEFINED AUDIT" },
            { 1346847573, "USER_CHANGE_PASSWORD_GROUP" },
            { 1195459669, "USER_DEFINED_AUDIT_GROUP" },
            { 538990422, "VIEW" },
            { 1413699414, "VIEW CHANGETRACKING" },
            { 1414743126, "VIEW DATABASE STATE" },
            { 1414746966, "VIEW SERVER STATE" },
            { 541934402, "BATCH COMPLETED" },
            { 542397250, "BATCH STARTED" },
            { 541934418, "RPC COMPLETED" },
            { 542397266, "RPC STARTED" },
            { 1396855380, "TRANSACTION BEGIN STARTED" },
            { 1396920916, "TRANSACTION COMMIT STARTED" },
            { 1397903956, "TRANSACTION ROLLBACK STARTED" },
            { 1397969492, "TRANSACTION SAVE POINT STARTED" },
            { 1397772884, "TRANSACTION PROMOTE STARTED" },
            { 1397183060, "TRANSACTION PROPAGATE STARTED" },
            { 1128419924, "TRANSACTION BEGIN COMPLETED" },
            { 1128485460, "TRANSACTION COMMIT COMPLETED" },
            { 1129468500, "TRANSACTION ROLLBACK COMPLETED" },
            { 1129534036, "TRANSACTION SAVE POINT COMPLETED" },
            { 1129337428, "TRANSACTION PROMOTE COMPLETED" },
            { 1128747604, "TRANSACTION PROPAGATE COMPLETED" },
            { 1195530324, "BEGIN TRANSACTION TSQL" },
            { 1329876565, "UNDO STATEMENT TSQL" },
            { 1296259156, "COMMIT TRANSACTION TSQL" },
            { 1112692820, "ROLLBACK TRANSACTION TSQL" }
        };

        static Dictionary<int, ClassTypeData> ClassTypeDictionary = new Dictionary<int, ClassTypeData>()
        {
            { 20801, new ClassTypeData("ADHOC QUERY", "OBJECT") },
            { 17985, new ClassTypeData("AGGREGATE", "OBJECT") },
            { 21057, new ClassTypeData("APPLICATION ROLE", "APPLICATION ROLE") },
            { 21313, new ClassTypeData("ASSEMBLY", "ASSEMBLY") },
            { 19265, new ClassTypeData("ASYMMETRIC KEY", "ASYMMETRIC KEY") },
            { 19521, new ClassTypeData("ASYMMETRIC KEY LOGIN", "LOGIN") },
            { 21825, new ClassTypeData("ASYMMETRIC KEY USER", "USER") },
            { 21828, new ClassTypeData("AUDIT", "DATABASE") },
            { 18241, new ClassTypeData("AVAILABILITY GROUP", "AVAILABILITY GROUP") },
            { 21072, new ClassTypeData("BROKER PRIORITY", "DATABASE") },
            { 21059, new ClassTypeData("CERTIFICATE", "CERTIFICATE") },
            { 19523, new ClassTypeData("CERTIFICATE LOGIN", "LOGIN") },
            { 21827, new ClassTypeData("CERTIFICATE USER", "USER") },
            { 8259, new ClassTypeData("CHECK CONSTRAINT", "OBJECT") },
            { 19267, new ClassTypeData("COLUMN ENCRYPTION KEY", "DATABASE") },
            { 19779, new ClassTypeData("COLUMN MASTER KEY DEFINITION", "DATABASE") },
            { 21571, new ClassTypeData("CONTRACT", "CONTRACT") },
            { 17475, new ClassTypeData("CREDENTIAL", "SERVER") },
            { 20547, new ClassTypeData("CRYPTOGRAPHIC PROVIDER", "SERVER") },
            { 16964, new ClassTypeData("DATABASE", "DATABASE") },
            { 16708, new ClassTypeData("DATABASE AUDIT SPECIFICATION", "DATABASE") },
            { 17220, new ClassTypeData("DATABASE CREDENTIAL", "DATABASE") },
            { 19268, new ClassTypeData("DATABASE ENCRYPTION KEY", "DATABASE") },
            { 17732, new ClassTypeData("DATABASE EVENT SESSION", "DATABASE") },
            { 8260, new ClassTypeData("DEFAULT", "OBJECT") },
            { 20549, new ClassTypeData("ENDPOINT", "ENDPOINT") },
            { 20037, new ClassTypeData("EVENT NOTIFICATION", "OBJECT") },
            { 20036, new ClassTypeData("EVENT NOTIFICATION DATABASE", "DATABASE") },
            { 20047, new ClassTypeData("EVENT NOTIFICATION OBJECT", "DATABASE") },
            { 17491, new ClassTypeData("EVENT NOTIFICATION SERVER", "SERVER") },
            { 17747, new ClassTypeData("EVENT SESSION", "SERVER") },
            { 17477, new ClassTypeData("EXTERNAL DATA SOURCE", "DATABASE") },
            { 17989, new ClassTypeData("EXTERNAL FILE FORMAT", "DATABASE") },
            { 8262, new ClassTypeData("FOREIGN KEY CONSTRAINT", "OBJECT") },
            { 17222, new ClassTypeData("FULLTEXT CATALOG", "FULLTEXT CATALOG") },
            { 19526, new ClassTypeData("FULLTEXT STOPLIST", "FULLTEXT STOPLIST") },
            { 21318, new ClassTypeData("FUNCTION SCALAR ASSEMBLY ", "OBJECT") },
            { 21321, new ClassTypeData("FUNCTION SCALAR INLINE SQL ", "OBJECT") },
            { 20038, new ClassTypeData("FUNCTION SCALAR SQL", "OBJECT") },
            { 21574, new ClassTypeData("FUNCTION TABLE-VALUED ASSEMBLY ", "OBJECT") },
            { 17993, new ClassTypeData("FUNCTION TABLE-VALUED INLINE SQL", "OBJECT") },
            { 18004, new ClassTypeData("FUNCTION TABLE-VALUED SQL", "OBJECT") },
            { 21831, new ClassTypeData("GROUP USER", "USER") },
            { 22601, new ClassTypeData("INDEX", "OBJECT") },
            { 21577, new ClassTypeData("INTERNAL TABLE", "OBJECT") },
            { 22604, new ClassTypeData("LOGIN", "LOGIN") },
            { 19277, new ClassTypeData("MASTER KEY", "DATABASE") },
            { 21581, new ClassTypeData("MESSAGE TYPE", "MESSAGE TYPE") },
            { 16975, new ClassTypeData("OBJECT", "OBJECT") },
            { 18000, new ClassTypeData("PARTITION FUNCTION", "DATABASE") },
            { 21328, new ClassTypeData("PARTITION SCHEME", "DATABASE") },
            { 20816, new ClassTypeData("PREPARED ADHOC QUERY", "OBJECT") },
            { 19280, new ClassTypeData("PRIMARY KEY", "OBJECT") },
            { 20819, new ClassTypeData("QUEUE", "OBJECT") },
            { 20034, new ClassTypeData("REMOTE SERVICE BINDING", "REMOTE SERVICE BINDING") },
            { 18258, new ClassTypeData("RESOURCE GOVERNOR", "SERVER") },
            { 19538, new ClassTypeData("ROLE", "ROLE") },
            { 21586, new ClassTypeData("ROUTE", "ROUTE") },
            { 8274, new ClassTypeData("RULE", "OBJECT") },
            { 17235, new ClassTypeData("SCHEMA", "SCHEMA") },
            { 20550, new ClassTypeData("SEARCH PROPERTY LIST", "SEARCH PROPERTY LIST") },
            { 20563, new ClassTypeData("SECURITY POLICY", "OBJECT") },
            { 20307, new ClassTypeData("SEQUENCE OBJECT", "OBJECT") },
            { 21075, new ClassTypeData("SERVER", "SERVER") },
            { 8257, new ClassTypeData("SERVER AUDIT", "SERVER") },
            { 16723, new ClassTypeData("SERVER AUDIT SPECIFICATION", "SERVER") },
            { 20291, new ClassTypeData("SERVER CONFIG", "SERVER") },
            { 18259, new ClassTypeData("SERVER ROLE", "SERVER ROLE") },
            { 22099, new ClassTypeData("SERVICE", "SERVICE") },
            { 19539, new ClassTypeData("SQL LOGIN", "LOGIN") },
            { 21843, new ClassTypeData("SQL USER", "USER") },
            { 21587, new ClassTypeData("STATISTICS", "OBJECT") },
            { 8272, new ClassTypeData("STORED PROCEDURE", "OBJECT") },
            { 17232, new ClassTypeData("STORED PROCEDURE ASSEMBLY", "OBJECT") },
            { 8280, new ClassTypeData("STORED PROCEDURE EXTENDED", "OBJECT") },
            { 18002, new ClassTypeData("STORED PROCEDURE REPLICATION FILTER", "OBJECT") },
            { 19283, new ClassTypeData("SYMMETRIC KEY", "SYMMETRIC KEY") },
            { 20051, new ClassTypeData("SYNONYM", "OBJECT") },
            { 8277, new ClassTypeData("TABLE", "OBJECT") },
            { 8275, new ClassTypeData("TABLE SYSTEM", "OBJECT") },
            { 21076, new ClassTypeData("TRIGGER", "OBJECT") },
            { 16724, new ClassTypeData("TRIGGER ASSEMBLY", "OBJECT") },
            { 21572, new ClassTypeData("TRIGGER DATABASE", "DATABASE") },
            { 8276, new ClassTypeData("TRIGGER SERVER", "SERVER") },
            { 22868, new ClassTypeData("TYPE", "TYPE") },
            { 20545, new ClassTypeData("Undocumented", "OBJECT") },
            { 20821, new ClassTypeData("UNIQUE CONSTRAINT", "OBJECT") },
            { 21333, new ClassTypeData("USER", "USER") },
            { 8278, new ClassTypeData("VIEW", "OBJECT") },
            { 18263, new ClassTypeData("WINDOWS GROUP", "LOGIN") },
            { 19543, new ClassTypeData("WINDOWS LOGIN", "LOGIN") },
            { 21847, new ClassTypeData("WINDOWS USER", "USER") },
            { 22611, new ClassTypeData("XML SCHEMA COLLECTION", "XML SCHEMA COLLECTION") },
            { 21080, new ClassTypeData("XREL TREE", "OBJECT") },
        };

        [JsonProperty]
        public string EventTime { get; private set; }

        [JsonProperty]
        public int SequenceNumber { get; private set; }

        [JsonProperty]
        public int ActionId { get; private set; }

        [JsonProperty]
        public bool Succeeded { get; private set; }

        [JsonProperty]
        public short SessionId { get; private set; }

        [JsonProperty]
        public int ServerPrincipalId { get; private set; }

        [JsonProperty]
        public int DatabasePrincipalId { get; private set; }

        [JsonProperty]
        public int ObjectId { get; private set; }

        [JsonProperty]
        public short ClassType { get; private set; }

        [JsonProperty]
        public string ClientIp { get; private set; }

        [JsonProperty]
        public string ServerPrincipalName { get; private set; }

        [JsonProperty]
        public string DatabasePrincipalName { get; private set; }

        [JsonProperty]
        public string ServerInstanceName { get; private set; }

        [JsonProperty]
        public string DatabaseName { get; private set; }

        [JsonProperty]
        public string SchemaName { get; private set; }

        [JsonProperty]
        public string ObjectName { get; private set; }

        [JsonProperty]
        public string Statement { get; private set; }

        [JsonProperty]
        public string ClassTypeDescription { get; private set; }

        [JsonProperty]
        public string SecurableClassType { get; private set; }

        [JsonProperty]
        public string ActionName { get; private set; }

        [JsonProperty]
        public string ApplicationName { get; private set; }
        
        [JsonProperty]
        public string DurationInMilliSeconds { get; private set; }

        public SQLAuditLog(PublishedEvent currentEvent)
        {
            if (currentEvent.Fields["event_time"].Value == null)
            {
                throw new InvalidOperationException("Event log has no time field. Can not send it to OMS");
            }

            ActionId = Convert.ToInt32(currentEvent.Fields["action_id"].Value);
            if (ActionIdDictionary.ContainsKey(ActionId))
            {
                ActionName = ActionIdDictionary[ActionId];
            }

            ClassType = Convert.ToInt16(currentEvent.Fields["class_type"].Value);
            if (ClassTypeDictionary.ContainsKey(ClassType))
            {
                ClassTypeDescription = ClassTypeDictionary[ClassType].ClassTypeDescription;
                SecurableClassType = ClassTypeDictionary[ClassType].SecurableClassType;
            }

            var time = (DateTimeOffset)currentEvent.Fields["event_time"].Value;
            EventTime = time.ToString("s");
            SequenceNumber = Convert.ToInt32(currentEvent.Fields["sequence_number"].Value);
            Succeeded = Convert.ToBoolean(currentEvent.Fields["succeeded"].Value);
            SessionId = Convert.ToInt16(currentEvent.Fields["session_id"].Value);
            ServerPrincipalId = Convert.ToInt32(currentEvent.Fields["server_principal_id"].Value);
            DatabasePrincipalId = Convert.ToInt32(currentEvent.Fields["database_principal_id"].Value);
            ObjectId = Convert.ToInt32(currentEvent.Fields["object_id"].Value);
            ClientIp = Convert.ToString(currentEvent.Fields["client_ip"].Value);
            ServerPrincipalName = Convert.ToString(currentEvent.Fields["server_principal_name"].Value);
            DatabasePrincipalName = Convert.ToString(currentEvent.Fields["database_principal_name"].Value);
            ServerInstanceName = Convert.ToString(currentEvent.Fields["server_instance_name"].Value);
            DatabaseName = Convert.ToString(currentEvent.Fields["database_name"].Value);
            SchemaName = Convert.ToString(currentEvent.Fields["schema_name"].Value);
            ObjectName = Convert.ToString(currentEvent.Fields["object_name"].Value);
            Statement = Convert.ToString(currentEvent.Fields["statement"].Value);
            ApplicationName= Convert.ToString(currentEvent.Fields["application_name"].Value);
            DurationInMilliSeconds = Convert.ToInt64(currentEvent.Fields["duration_milliseconds"].Value);
        }

    }

    [JsonObject]
    public class SubfolderState
    {
        [JsonProperty]
        public string BlobName { get; set; }

        [JsonProperty]
        public string Date { get; set; }

        [JsonProperty]
        public DateTimeOffset? LastModified { get; set; }

        [JsonProperty]
        public int EventNumber { get; set; }

        public SubfolderState()
        {
            // If the tool is running for the first time, the logs from the last week will be sent to OMS
            var lastWeek = DateTime.UtcNow.AddDays(-7);
            var month = lastWeek.Month > 10 ? lastWeek.Month.ToString() : string.Format("0{0}", lastWeek.Month.ToString());
            var day = lastWeek.Day > 10 ? lastWeek.Day.ToString() : string.Format("0{0}", lastWeek.Day.ToString());
            Date = string.Format("{0}-{1}-{2}", lastWeek.Year, month, day);
            LastModified = DateTimeOffset.MinValue;
        }
    }
}
