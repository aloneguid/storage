using System;
using System.Net;
using FluentFTP;
using Storage.Net.Blob;
using Storage.Net.ConnectionString;
using Storage.Net.Ftp;

namespace Storage.Net
{
   public static class Factory
   {
      /// <summary>
      /// Register Azure module.
      /// </summary>
      public static IModulesFactory UseFtpStorage(this IModulesFactory factory)
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
      public static IBlobStorage Ftp(this IBlobStorageFactory factory,
         string hostNameOrAddress, NetworkCredential credentials)
      {
         return new FluentFtpBlobStorage(hostNameOrAddress, credentials);
      }

      /// <summary>
      /// Constructs an instance of FTP client by accepting a custom instance of FluentFTP client
      /// </summary>
      public static IBlobStorage FtpFromFluentFtpClient(this IBlobStorageFactory factory,
         FtpClient ftpClient)
      {
         return new FluentFtpBlobStorage(ftpClient, false);
      }
   }
}
