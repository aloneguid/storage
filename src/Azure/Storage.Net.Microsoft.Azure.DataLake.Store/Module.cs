using Storage.Net.Blobs;
using Storage.Net.ConnectionString;
using Storage.Net.KeyValue;
using Storage.Net.Messaging;
using Storage.Net.Microsoft.Azure.DataLake.Store.Gen1;
using Storage.Net.Microsoft.Azure.DataLake.Store.Gen2;

namespace Storage.Net.Microsoft.Azure.DataLake.Store
{
   class Module : IExternalModule, IConnectionFactory
   {
      public IConnectionFactory ConnectionFactory => this;

      public IBlobStorage CreateBlobStorage(StorageConnectionString connectionString)
      {
         if(connectionString.Prefix == "azure.datalake.gen1")
         {
            connectionString.GetRequired("account", true, out string accountName);
            connectionString.GetRequired("tenantId", true, out string tenantId);
            connectionString.GetRequired("principalId", true, out string principalId);
            connectionString.GetRequired("principalSecret", true, out string principalSecret);

            int.TryParse(connectionString.Get("listBatchSize"), out int listBatchSize);

            AzureDataLakeGen1Storage client = AzureDataLakeGen1Storage.CreateByClientSecret(
               accountName, tenantId, principalId, principalSecret);

            if(listBatchSize != 0)
            {
               client.ListBatchSize = listBatchSize;
            }

            return client;
         }
         else if(connectionString.Prefix == "azure.datalake.gen2")
         {
            connectionString.GetRequired("account", true, out string accountName);

            if(connectionString.Parameters.ContainsKey("msi"))
            {
               return AzureDataLakeStoreGen2BlobStorageProvider.CreateByManagedIdentity(accountName);
            }

            string key = connectionString.Get("key");

            if(!string.IsNullOrWhiteSpace(key))
            {
               //connect with shared key

               return AzureDataLakeStoreGen2BlobStorageProvider.CreateBySharedAccessKey(accountName, key);
            }
            else
            {
               //connect with service principal

               connectionString.GetRequired("tenantId", true, out string tenantId);
               connectionString.GetRequired("principalId", true, out string principalId);
               connectionString.GetRequired("principalSecret", true, out string principalSecret);

               return AzureDataLakeStoreGen2BlobStorageProvider.CreateByClientSecret(accountName, tenantId, principalId, principalSecret);
            }

         }

         return null;
      }

      public IKeyValueStorage CreateKeyValueStorage(StorageConnectionString connectionString) => null;
      public IMessagePublisher CreateMessagePublisher(StorageConnectionString connectionString) => null;
      public IMessageReceiver CreateMessageReceiver(StorageConnectionString connectionString) => null;
   }
}
