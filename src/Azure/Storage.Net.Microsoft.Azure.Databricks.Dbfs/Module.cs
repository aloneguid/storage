using Storage.Net.Blobs;
using Storage.Net.ConnectionString;
using Storage.Net.Messaging;

namespace Storage.Net.Microsoft.Azure.Databricks.Dbfs
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
            string isReadOnlyString = connectionString.Get("isReadOnly");
            bool.TryParse(isReadOnlyString, out bool isReadOnly);

            return new AzureDatabricksDbfsBlobStorage(baseUri, token, isReadOnly);
         }

         return null;
      }

      public IMessenger CreateMessenger(StorageConnectionString connectionString) => null;
   }
}
