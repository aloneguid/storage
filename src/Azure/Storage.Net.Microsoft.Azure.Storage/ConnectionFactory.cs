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
         if(connectionString.Prefix == Constants.AzureBlobConnectionPrefix)
         {
            if(bool.TryParse(connectionString.Get(Constants.DevelopmentParam), out bool useDevelopment)
               && useDevelopment)
            {
               return new AzureUniversalBlobStorageProvider();
            }
            else
            {
               connectionString.GetRequired(Constants.AccountParam, true, out string accountName);
               connectionString.GetRequired(Constants.KeyParam, true, out string key);

               return new AzureUniversalBlobStorageProvider(accountName, key);
            }
         }

         return null;
      }

      public IKeyValueStorage CreateKeyValueStorage(StorageConnectionString connectionString)
      {
         if(connectionString.Prefix == Constants.AzureTablesConnectionPrefix)
         {
            if(bool.TryParse(connectionString.Get(Constants.DevelopmentParam), out bool useDevelopment)
               && useDevelopment)
            {
               return new AzureTableStorageKeyValueStorage();
            }
            else
            {
               connectionString.GetRequired(Constants.AccountParam, true, out string accountName);
               connectionString.GetRequired(Constants.KeyParam, true, out string key);

               return new AzureTableStorageKeyValueStorage(accountName, key);
            }
         }

         return null;
      }

      public IMessagePublisher CreateMessagePublisher(StorageConnectionString connectionString)
      {
         if(connectionString.Prefix == Constants.AzureQueueConnectionPrefix)
         {
            connectionString.GetRequired(Constants.QueueParam, true, out string queueName);

            if(bool.TryParse(connectionString.Get(Constants.DevelopmentParam), out bool useDevelopment)
               && useDevelopment)
            {
               return new AzureStorageQueuePublisher(queueName);
            }
            else
            {
               connectionString.GetRequired(Constants.AccountParam, true, out string accountName);
               connectionString.GetRequired(Constants.KeyParam, true, out string key);

               return new AzureStorageQueuePublisher(accountName, key, queueName);
            }
         }

         return null;
      }

      public IMessageReceiver CreateMessageReceiver(StorageConnectionString connectionString)
      {
         if(connectionString.Prefix == Constants.AzureQueueConnectionPrefix)
         {
            connectionString.GetRequired(Constants.QueueParam, true, out string queueName);

            string invisibilityString = connectionString.Get(Constants.InvisibilityParam);
            string pollingTimeoutString = connectionString.Get(Constants.PollParam);

            if(!TimeSpan.TryParse(invisibilityString, out TimeSpan invisibility))
            {
               invisibility = TimeSpan.FromMinutes(1);
            }

            if(!TimeSpan.TryParse(pollingTimeoutString, out TimeSpan polling))
            {
               polling = TimeSpan.FromMinutes(1);
            }

            if(bool.TryParse(connectionString.Get(Constants.DevelopmentParam), out bool useDevelopment)
               && useDevelopment)
            {
               return new AzureStorageQueueReceiver(queueName, invisibility, polling);
            }
            else
            {
               connectionString.GetRequired(Constants.AccountParam, true, out string accountName);
               connectionString.GetRequired(Constants.KeyParam, true, out string key);

               return new AzureStorageQueueReceiver(accountName, key, queueName, invisibility, polling);
            }
         }

         return null;
      }
   }
}
