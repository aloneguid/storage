using Storage.Net.Blobs;
using Storage.Net.ConnectionString;
using Storage.Net.Messaging;

namespace Storage.Net.Microsoft.Azure.Storage.Blobs
{
   class Module : IExternalModule, IConnectionFactory
   {
      public IConnectionFactory ConnectionFactory => this;

      public IBlobStorage CreateBlobStorage(StorageConnectionString connectionString)
      {
         if(connectionString.Prefix == KnownPrefix.AzureBlobStorage)
         {
            if(connectionString.Parameters.ContainsKey(KnownParameter.IsLocalEmulator))
            {
               return StorageFactory.Blobs.AzureBlobStorageWithLocalEmulator();
            }

            connectionString.GetRequired(KnownParameter.AccountName, true, out string accountName);

            string sharedKey = connectionString.Get(KnownParameter.KeyOrPassword);
            if(!string.IsNullOrEmpty(sharedKey))
            {
               return StorageFactory.Blobs.AzureBlobStorageWithSharedKey(accountName, sharedKey);
            }

            string tenantId = connectionString.Get(KnownParameter.TenantId);
            if(!string.IsNullOrEmpty(tenantId))
            {
               connectionString.GetRequired(KnownParameter.ClientId, true, out string clientId);
               connectionString.GetRequired(KnownParameter.ClientSecret, true, out string clientSecret);

               return StorageFactory.Blobs.AzureBlobStorageWithAzureAd(accountName, tenantId, clientId, clientSecret);
            }

            if(connectionString.Parameters.ContainsKey(KnownParameter.MsiEnabled))
            {
               return StorageFactory.Blobs.AzureBlobStorageWithMsi(accountName);
            }
         }
         else if(connectionString.Prefix == KnownPrefix.AzureDataLakeGen2 || connectionString.Prefix == KnownPrefix.AzureDataLake)
         {
            connectionString.GetRequired(KnownParameter.AccountName, true, out string accountName);

            string sharedKey = connectionString.Get(KnownParameter.KeyOrPassword);
            if(!string.IsNullOrEmpty(sharedKey))
            {
               return StorageFactory.Blobs.AzureDataLakeStorageWithSharedKey(accountName, sharedKey);
            }

            string tenantId = connectionString.Get(KnownParameter.TenantId);
            if(!string.IsNullOrEmpty(tenantId))
            {
               connectionString.GetRequired(KnownParameter.ClientId, true, out string clientId);
               connectionString.GetRequired(KnownParameter.ClientSecret, true, out string clientSecret);

               return StorageFactory.Blobs.AzureDataLakeStorageWithAzureAd(accountName, tenantId, clientId, clientSecret);
            }

            if(connectionString.Parameters.ContainsKey(KnownParameter.MsiEnabled))
            {
               return StorageFactory.Blobs.AzureDataLakeStorageWithMsi(accountName);
            }

         }

         return null;
      }

      public IMessenger CreateMessenger(StorageConnectionString connectionString) => null;
   }
}
