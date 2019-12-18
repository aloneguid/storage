using Storage.Net.Blobs;
using Storage.Net.ConnectionString;
using Storage.Net.Messaging;

namespace Storage.Net.Gcp.CloudStorage
{
   class Module : IExternalModule, IConnectionFactory
   {
      public IConnectionFactory ConnectionFactory => new Module();

      public IBlobStorage CreateBlobStorage(StorageConnectionString connectionString)
      {
         if(connectionString.Prefix == "google.storage")
         {
            connectionString.GetRequired("bucket", true, out string bucketName);
            connectionString.GetRequired("cred", true, out string base64EncodedJson);

            return StorageFactory.Blobs.GoogleCloudStorageFromJson(bucketName, base64EncodedJson, true);
         }

         return null;
      }

      public IMessenger CreateMessenger(StorageConnectionString connectionString) => null;
   }
}
