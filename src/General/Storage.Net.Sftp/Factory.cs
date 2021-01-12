using Renci.SshNet;
using Storage.Net.Blobs;
using Storage.Net.ConnectionString;
using Storage.Net.Sftp;

namespace Storage.Net
{
   /// <summary>
   /// This class provides methods for creating new instances of the <see cref="T:Storage.Net.Sftp.SshNetSftpBlobStorage" /> class.
   /// </summary>
   public static class Factory
   {
      private class Module : IExternalModule
      {
         public IConnectionFactory ConnectionFactory => new ConnectionFactory();
      }

      /// <summary>
      /// Register Sftp module.
      /// </summary>
      public static IModulesFactory UseSftpStorage(this IModulesFactory factory)
         => factory.Use(new Module());

      /// <summary>
      /// Initializes a new instance of the <see cref="T:Storage.Net.Sftp.SshNetSftpBlobStorage" /> class.
      /// </summary>
      /// <param name="connectionInfo">The connection info.</param>
      /// <exception cref="T:System.ArgumentNullException"><paramref name="connectionInfo" /> is <b>null</b>.</exception>
      public static IBlobStorage Sftp(this IBlobStorageFactory factory, ConnectionInfo connectionInfo)
         => new SshNetSftpBlobStorage(connectionInfo);

      /// <summary>
      /// Initializes a new instance of the <see cref="T:Storage.Net.Sftp.SshNetSftpBlobStorage" /> class.
      /// </summary>
      /// <param name="host">Connection host.</param>
      /// <param name="port">Connection port.</param>
      /// <param name="username">Authentication username.</param>
      /// <param name="password">Authentication password.</param>
      /// <exception cref="T:System.ArgumentNullException"><paramref name="password" /> is <b>null</b>.</exception>
      /// <exception cref="T:System.ArgumentException"><paramref name="host" /> is invalid. <para>-or-</para> <paramref name="username" /> is <b>null</b> or contains only whitespace characters.</exception>
      /// <exception cref="T:System.ArgumentOutOfRangeException"><paramref name="port" /> is not within <see cref="F:System.Net.IPEndPoint.MinPort" /> and <see cref="F:System.Net.IPEndPoint.MaxPort" />.</exception>
      public static IBlobStorage Sftp(this IBlobStorageFactory factory, string host, int port, string username, string password)
         => new SshNetSftpBlobStorage(host, port, username, password);

      /// <summary>
      /// Initializes a new instance of the <see cref="T:Storage.Net.Sftp.SshNetSftpBlobStorage" /> class.
      /// </summary>
      /// <param name="host">Connection host.</param>
      /// <param name="username">Authentication username.</param>
      /// <param name="password">Authentication password.</param>
      /// <exception cref="T:System.ArgumentNullException"><paramref name="password" /> is <b>null</b>.</exception>
      /// <exception cref="T:System.ArgumentException"><paramref name="host" /> is invalid. <para>-or-</para> <paramref name="username" /> is <b>null</b> contains only whitespace characters.</exception>
      public static IBlobStorage Sftp(this IBlobStorageFactory factory, string host, string username, string password)
         => new SshNetSftpBlobStorage(host, username, password);

      /// <summary>
      /// Initializes a new instance of the <see cref="T:Storage.Net.Sftp.SshNetSftpBlobStorage" /> class.
      /// </summary>
      /// <param name="host">Connection host.</param>
      /// <param name="port">Connection port.</param>
      /// <param name="username">Authentication username.</param>
      /// <param name="keyFiles">Authentication private key file(s) .</param>
      /// <exception cref="T:System.ArgumentNullException"><paramref name="keyFiles" /> is <b>null</b>.</exception>
      /// <exception cref="T:System.ArgumentException"><paramref name="host" /> is invalid. <para>-or-</para> <paramref name="username" /> is nu<b>null</b>ll or contains only whitespace characters.</exception>
      /// <exception cref="T:System.ArgumentOutOfRangeException"><paramref name="port" /> is not within <see cref="F:System.Net.IPEndPoint.MinPort" /> and <see cref="F:System.Net.IPEndPoint.MaxPort" />.</exception>
      public static IBlobStorage Sftp(this IBlobStorageFactory factory, string host, int port, string username, params PrivateKeyFile[] keyFiles)
         => new SshNetSftpBlobStorage(host, port, username, keyFiles);

      /// <summary>
      /// Initializes a new instance of the <see cref="T:Storage.Net.Sftp.SshNetSftpBlobStorage" /> class.
      /// </summary>
      /// <param name="host">Connection host.</param>
      /// <param name="username">Authentication username.</param>
      /// <param name="keyFiles">Authentication private key file(s) .</param>
      /// <exception cref="T:System.ArgumentNullException"><paramref name="keyFiles" /> is <b>null</b>.</exception>
      /// <exception cref="T:System.ArgumentException"><paramref name="host" /> is invalid. <para>-or-</para> <paramref name="username" /> is <b>null</b> or contains only whitespace characters.</exception>
      public static IBlobStorage Sftp(this IBlobStorageFactory factory, string host, string username, params PrivateKeyFile[] keyFiles)
         => new SshNetSftpBlobStorage(host, username, keyFiles);

      /// <summary>
      /// Initializes a new instance of the <see cref="T:Storage.Net.Sftp.SshNetSftpBlobStorage" /> class.
      /// </summary>
      /// <param name="sftpClient">The SFTP client.</param>
      /// <param name="disposeClient">if set to <see langword="true" /> [dispose client].</param>
      /// <exception cref="System.ArgumentNullException">sftpClient</exception>
      public static IBlobStorage Sftp(this IBlobStorageFactory factory, SftpClient sftpClient, bool disposeClient = false)
         => new SshNetSftpBlobStorage(sftpClient, disposeClient);
   }
}
