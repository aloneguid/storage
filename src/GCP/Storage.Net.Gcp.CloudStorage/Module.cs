using Storage.Net.Blobs;
using Storage.Net.ConnectionString;
using Storage.Net.KeyValue;
using Storage.Net.Messaging;

namespace Storage.Net.Gcp.CloudStorage
{
   class Module : IExternalModule, IConnectionFactory
   {
      public IConnectionFactory ConnectionFactory => new Module();

      public IBlobStorage CreateBlobStorage(StorageConnectionString connectionString)
      {
         return null;
      }

      public IKeyValueStorage CreateKeyValueStorage(StorageConnectionString connectionString) => null;
      public IMessagePublisher CreateMessagePublisher(StorageConnectionString connectionString) => null;
      public IMessageReceiver CreateMessageReceiver(StorageConnectionString connectionString) => null;
   }
}
