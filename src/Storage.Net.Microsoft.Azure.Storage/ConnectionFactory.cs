using System;
using Storage.Net.Blob;
using Storage.Net.ConnectionString;
using Storage.Net.KeyValue;
using Storage.Net.Messaging;
using Storage.Net.Microsoft.Azure.Storage.Blob;
using Storage.Net.Microsoft.Azure.Storage.KeyValue;
using Storage.Net.Microsoft.Azure.Storage.Messaging;

namespace Storage.Net.Microsoft.Azure.Storage
{
   class ConnectionFactory : IConnectionFactory
   {
      public IBlobStorage CreateBlobStorage(StorageConnectionString connectionString)
      {
         if(connectionString.Prefix == "azure.blob")
         {
            connectionString.GetRequired("account", true, out string accountName);
            connectionString.GetRequired("key", true, out string key);

            return new AzureUniversalBlobStorageProvider(accountName, key);
         }

         return null;
      }

      public IKeyValueStorage CreateKeyValueStorage(StorageConnectionString connectionString)
      {
         if(connectionString.Prefix == "azure.tables")
         {
            connectionString.GetRequired("account", true, out string acctountName);
            connectionString.GetRequired("key", true, out string key);

            return new AzureTableStorageKeyValueStorage(acctountName, key);
         }

         return null;
      }

      public IMessagePublisher CreateMessagePublisher(StorageConnectionString connectionString)
      {
         if(connectionString.Prefix == "azure.queue")
         {
            connectionString.GetRequired("account", true, out string accountName);
            connectionString.GetRequired("key", true, out string key);
            connectionString.GetRequired("queue", true, out string queueName);

            return new AzureStorageQueuePublisher(accountName, key, queueName);
         }

         return null;
      }

      public IMessageReceiver CreateMessageReceiver(StorageConnectionString connectionString)
      {
         if (connectionString.Prefix == "azure.queue")
         {
            connectionString.GetRequired("account", true, out string accountName);
            connectionString.GetRequired("key", true, out string key);
            connectionString.GetRequired("queue", true, out string queueName);

            string invisibilityString = connectionString.Get("invisibility");
            string pollingTimeoutString = connectionString.Get("poll");

            if(!TimeSpan.TryParse(invisibilityString, out TimeSpan invisibility))
            {
               invisibility = TimeSpan.FromMinutes(1);
            }

            if(!TimeSpan.TryParse(pollingTimeoutString, out TimeSpan polling))
            {
               polling = TimeSpan.FromMinutes(1);
            }

            return new AzureStorageQueueReceiver(accountName, key, queueName, invisibility, polling);
         }

         return null;
      }
   }
}
