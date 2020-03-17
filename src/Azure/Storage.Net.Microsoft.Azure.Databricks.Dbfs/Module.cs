using Storage.Net.Blobs;
using Storage.Net.ConnectionString;
using Storage.Net.Messaging;

namespace Storage.Net.Databricks
{
   class Module : IExternalModule, IConnectionFactory
   {
      public IConnectionFactory ConnectionFactory => this;

      public IBlobStorage CreateBlobStorage(StorageConnectionString connectionString)
      {
         if(connectionString.Prefix == KnownPrefix.DatabricksDbfs)
         {
            connectionString.GetRequired("baseUri", true, out string baseUri);
            connectionString.GetRequired("token", true, out string token);

            return new DatabricksBlobStorage(baseUri, token);
         }

         return null;
      }

      public IMessenger CreateMessenger(StorageConnectionString connectionString) => null;
   }
}
