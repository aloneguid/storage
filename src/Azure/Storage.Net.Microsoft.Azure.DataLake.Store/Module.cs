using Storage.Net.Blobs;
using Storage.Net.ConnectionString;
using Storage.Net.Messaging;
using Storage.Net.Microsoft.Azure.DataLake.Store.Gen1;

namespace Storage.Net.Microsoft.Azure.DataLake.Store
{
   class Module : IExternalModule, IConnectionFactory
   {
      public IConnectionFactory ConnectionFactory => this;

      public IBlobStorage CreateBlobStorage(StorageConnectionString connectionString)
      {
         if(connectionString.Prefix == KnownPrefix.AzureDataLakeGen1)
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

         return null;
      }

      public IMessenger CreateMessenger(StorageConnectionString connectionString) => null;
   }
}
