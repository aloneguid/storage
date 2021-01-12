using Storage.Net.Blobs;
using Storage.Net.ConnectionString;
using Storage.Net.Messaging;

namespace Storage.Net.Sftp
{
   /// <summary>
   /// The <see cref="T:Storage.Net.Sftp.ConnectionFactory"/> class is responsible for creating
   /// <see cref="T:Storage.Net.Sftp.SshNetSftpBlobStorage"/> instances from supported connection strings.
   /// </summary>
   /// <seealso cref="T:Storage.Net.ConnectionString.IConnectionFactory" />
   class ConnectionFactory : IConnectionFactory
   {
      /// <summary>
      /// The default port for SFTP connections.
      /// </summary>
      public const ushort DefaultPort = 22;

      /// <summary>
      /// Creates a blob storage instance from the specified connection string if supported; Otherwise it returns null.
      /// </summary>
      /// <param name="connectionString">The connection string to parse.</param>
      /// <returns></returns>
      public IBlobStorage CreateBlobStorage(StorageConnectionString connectionString)
      {
         if (connectionString.Prefix == "sftp")
         {
            connectionString.GetRequired("host", true, out string host);
            connectionString.GetRequired("user", true, out string user);
            connectionString.GetRequired("password", true, out string password);
            ushort port = ushort.TryParse(connectionString.Get("port"), out port) ? port : DefaultPort;

            return new SshNetSftpBlobStorage(host, port, user, password);
         }

         return null;
      }

      /// <summary>
      /// Creates a message publisher instance from the specified connection string if supported; Otherwise it returns null.
      /// </summary>
      /// <param name="connectionString">The connection string to parse.</param>
      /// <returns></returns>
      public IMessenger CreateMessenger(StorageConnectionString connectionString) => null;
   }
}
