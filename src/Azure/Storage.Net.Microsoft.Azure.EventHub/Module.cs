using System;
using System.Collections.Generic;
using System.Text;
using Storage.Net.Blobs;
using Storage.Net.ConnectionString;
using Storage.Net.KeyValue;
using Storage.Net.Messaging;

namespace Storage.Net.Microsoft.Azure.EventHub
{
   class Module : IExternalModule, IConnectionFactory
   {
      public IConnectionFactory ConnectionFactory => this;

      public IBlobStorage CreateBlobStorage(StorageConnectionString connectionString) => null;
      public IKeyValueStorage CreateKeyValueStorage(StorageConnectionString connectionString) => null;

      public IMessenger CreateMessenger(StorageConnectionString connectionString)
      {
         if(connectionString.Prefix == KnownPrefix.AzureEventHub)
         {
            if(connectionString.IsNative)
            {
               return new AzureEventHubMessenger(connectionString.Native, null);
            }
         }

         return null;
      }
   }
}
