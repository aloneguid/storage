using System;
using System.Collections.Generic;
using System.IO;
using Storage.Net.Blob;
using Storage.Net.Blob.Files;
using Storage.Net.KeyValue;
using Storage.Net.KeyValue.Files;
using Storage.Net.Messaging;

namespace Storage.Net.ConnectionString
{
   class BuiltInConnectionFactory : IConnectionFactory
   {
      public IBlobStorage CreateBlobStorage(StorageConnectionString connectionString)
      {
         if(connectionString.Prefix == "disk")
         {
            connectionString.GetRequired("path", true, out string path);

            return new DiskDirectoryBlobStorage(new DirectoryInfo(path));
         }

         if(connectionString.Prefix == "inmemory")
         {
            return new InMemoryBlobStorage();
         }

         if(connectionString.Prefix == "zip")
         {
            connectionString.GetRequired("path", true, out string path);

            return new ZipFileBlobStorageProvider(path);
         }

         return null;
      }

      public IKeyValueStorage CreateKeyValueStorage(StorageConnectionString connectionString)
      {
         if(connectionString.Prefix == "disk")
         {
            connectionString.GetRequired("path", true, out string path);

            return new CsvFileKeyValueStorage(new DirectoryInfo(path));
         }

         return null;
      }

      public IMessagePublisher CreateMessagePublisher(StorageConnectionString connectionString)
      {
         return null;
      }

      public IMessageReceiver CreateMessageReceiver(StorageConnectionString connectionString)
      {
         return null;
      }
   }
}
