using System;
using Storage.Net.Blobs;
using Storage.Net.ConnectionString;
using Storage.Net.SFtp;

namespace Storage.Net
{
   public static class Factory
   {
      /// <summary>
      /// Register Azure module.
      /// </summary>
      public static IModulesFactory UseSFtpStorage(this IModulesFactory factory)
      {
         return factory.Use(new Module());
      }

      private class Module : IExternalModule
      {
         public IConnectionFactory ConnectionFactory => new ConnectionFactory();
      }

      /// <summary>
      /// Constructs an instance of FTP client from host name and credentials
      /// </summary>
      public static IBlobStorage SFtp(this IBlobStorageFactory factory,
         string host, ushort port, string user, string password)
      {
         return new SshNetSFtpBlobStorage(host, port, user, password);
      }

      /// <summary>
      /// Constructs an instance of FTP client by accepting a custom instance of FluentFTP client
      /// </summary>
      public static IBlobStorage SFtp(this IBlobStorageFactory factory,
         string host, string user, string password)
      {
         return new SshNetSFtpBlobStorage(host, ConnectionFactory.DefaultPort, user, password);
      }
   }
}
