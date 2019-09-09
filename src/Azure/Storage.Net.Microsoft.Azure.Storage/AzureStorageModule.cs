using System;
using Storage.Net.Blobs;
using Storage.Net.ConnectionString;
using Storage.Net.KeyValue;
using Storage.Net.Messaging;
using Storage.Net.Microsoft.Azure.Storage.Blobs;
using Storage.Net.Microsoft.Azure.Storage.KeyValue;
using Storage.Net.Microsoft.Azure.Storage.Messaging;

namespace Storage.Net.Microsoft.Azure.Storage
{
   class Module : IExternalModule, IConnectionFactory
   {
      private const string BlobPrefix = "azure.blob";
      private const string FilesPrefix = "azure.file";
      public const string AccountParam = "account";
      public const string KeyParam = "key";

      public IConnectionFactory ConnectionFactory => this;

      public IBlobStorage CreateBlobStorage(StorageConnectionString connectionString)
      {
         if(connectionString.Prefix == BlobPrefix)
         {
            if(bool.TryParse(connectionString.Get(Constants.UseDevelopmentStorage), out bool useDevelopment) && useDevelopment)
            {
               return AzureUniversalBlobStorageProvider.CreateForLocalEmulator();
            }
            else
            {
               connectionString.GetRequired(AccountParam, true, out string accountName);
               connectionString.GetRequired(KeyParam, true, out string key);

               return AzureUniversalBlobStorageProvider.CreateFromAccountNameAndKey(accountName, key);
            }
         }
         else if(connectionString.Prefix == FilesPrefix)
         {
            connectionString.GetRequired(AccountParam, true, out string accountName);
            connectionString.GetRequired(KeyParam, true, out string key);

            return AzureFilesBlobStorage.CreateFromAccountNameAndKey(accountName, key);
         }
         else
         {
            //try to re-parse native connection string
            var newcs = new StorageConnectionString(BlobPrefix + "://" + connectionString.Prefix);

            if(newcs.Parameters.TryGetValue("AccountName", out string accountName) &&
               newcs.Parameters.TryGetValue("AccountKey", out string accountKey))
            {
               return AzureUniversalBlobStorageProvider.CreateFromAccountNameAndKey(accountName, accountKey);
            }
         }

         return null;
      }

      public IKeyValueStorage CreateKeyValueStorage(StorageConnectionString connectionString)
      {
         if(connectionString.Prefix == Constants.AzureTablesConnectionPrefix)
         {
            if(bool.TryParse(connectionString.Get(Constants.UseDevelopmentStorage), out bool useDevelopment)
               && useDevelopment)
            {
               return new AzureTableStorageKeyValueStorage();
            }
            else
            {
               connectionString.GetRequired(AccountParam, true, out string accountName);
               connectionString.GetRequired(KeyParam, true, out string key);

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

            if(bool.TryParse(connectionString.Get(Constants.UseDevelopmentStorage), out bool useDevelopment)
               && useDevelopment)
            {
               return new AzureStorageQueuePublisher(queueName);
            }
            else
            {
               connectionString.GetRequired(AccountParam, true, out string accountName);
               connectionString.GetRequired(KeyParam, true, out string key);

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

            if(bool.TryParse(connectionString.Get(Constants.UseDevelopmentStorage), out bool useDevelopment)
               && useDevelopment)
            {
               return new AzureStorageQueueReceiver(queueName, invisibility, polling);
            }
            else
            {
               connectionString.GetRequired(AccountParam, true, out string accountName);
               connectionString.GetRequired(KeyParam, true, out string key);

               return new AzureStorageQueueReceiver(accountName, key, queueName, invisibility, polling);
            }
         }

         return null;
      }

   }
}
