using System;
using System.Collections.Generic;
using System.Text;
using Storage.Net.Blobs;
using Storage.Net.ConnectionString;
using Storage.Net.KeyValue;
using Storage.Net.Messaging;

namespace Storage.Net.SFtp
{
   class ConnectionFactory : IConnectionFactory
   {
      public const ushort DefaultPort = 22;

      public IBlobStorage CreateBlobStorage(StorageConnectionString connectionString)
      {
         if(connectionString.Prefix == "sftp")
         {
            connectionString.GetRequired("host", true, out string host);
            connectionString.GetRequired("user", true, out string user);
            connectionString.GetRequired("password", true, out string password);
            ushort port = DefaultPort;
            ushort.TryParse(connectionString.Get("port"), out port);

            return new SshNetSFtpBlobStorage(host, port, user, password);
         }

         return null;
      }

      public IKeyValueStorage CreateKeyValueStorage(StorageConnectionString connectionString) => null;
      public IMessagePublisher CreateMessagePublisher(StorageConnectionString connectionString) => null;
      public IMessageReceiver CreateMessageReceiver(StorageConnectionString connectionString) => null;
   }
}
