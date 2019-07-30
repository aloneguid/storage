using System;
using System.Collections.Generic;
using System.Text;
using Storage.Net.Blobs;
using Storage.Net.ConnectionString;
using Storage.Net.KeyValue;
using Storage.Net.Messaging;

namespace Storage.Net.Microsoft.Azure.Databricks.Dbfs
{
   class Module : IExternalModule, IConnectionFactory
   {
      public IConnectionFactory ConnectionFactory => this;

      public IBlobStorage CreateBlobStorage(StorageConnectionString connectionString)
      {
         if(connectionString.Prefix == "azure.databricks.dbfs")
         {
            connectionString.GetRequired("baseUri", true, out string baseUri);
            connectionString.GetRequired("token", true, out string token);
            string isReadOnlyString = connectionString.Get("isReadOnly");
            bool.TryParse(isReadOnlyString, out bool isReadOnly);

            return new AzureDatabricksDbfsBlobStorage(baseUri, token, isReadOnly);
         }

         return null;
      }

      public IKeyValueStorage CreateKeyValueStorage(StorageConnectionString connectionString) => null;

      public IMessagePublisher CreateMessagePublisher(StorageConnectionString connectionString) => null;

      public IMessageReceiver CreateMessageReceiver(StorageConnectionString connectionString) => null;
   }
}
