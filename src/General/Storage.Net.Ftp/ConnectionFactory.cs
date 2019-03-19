using System;
using System.Collections.Generic;
using System.Net;
using System.Text;
using Storage.Net.Blob;
using Storage.Net.ConnectionString;
using Storage.Net.KeyValue;
using Storage.Net.Messaging;

namespace Storage.Net.Ftp
{
   class ConnectionFactory : IConnectionFactory
   {
      public IBlobStorage CreateBlobStorage(StorageConnectionString connectionString)
      {
         if(connectionString.Prefix == "ftp")
         {
            connectionString.GetRequired("host", true, out string host);
            connectionString.GetRequired("user", true, out string user);
            connectionString.GetRequired("password", true, out string password);

            return new FluentFtpBlobStorage(host, new NetworkCredential(user, password));
         }

         return null;
      }

      public IKeyValueStorage CreateKeyValueStorage(StorageConnectionString connectionString) => null;
      public IMessagePublisher CreateMessagePublisher(StorageConnectionString connectionString) => null;
      public IMessageReceiver CreateMessageReceiver(StorageConnectionString connectionString) => null;
   }
}
