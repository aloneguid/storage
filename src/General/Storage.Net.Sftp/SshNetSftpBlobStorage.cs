using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Polly;
using Polly.Retry;
using Renci.SshNet;
using Renci.SshNet.Async;
using Renci.SshNet.Sftp;
using Storage.Net.Blobs;

namespace Storage.Net.Sftp
{
   public class SshNetSftpBlobStorage : IExtendedBlobStorage
   {
      /// <summary>
      /// The retry policy
      /// </summary>
      private static readonly AsyncRetryPolicy _retryPolicy = Policy.Handle<Exception>().RetryAsync(3);

      /// <summary>
      /// Holds a reference to the <see cref="T:Storage.Net.Sftp.SshNetSftpBlobStorage" /> instance.
      /// </summary>
      private readonly SftpClient _client;

      /// <summary>
      /// A boolean flag indicating whether to dispose the client instance upon disposing this object.
      /// </summary>
      private readonly bool _disposeClient;

      /// <summary>
      /// A boolean flag indicating whether this instance is disposed.
      /// </summary>
      private bool _disposed = false;

      /// <summary>
      /// Gets or sets the maximum retry count.
      /// </summary>
      /// <value>
      /// The maximum retry count.
      /// </value>
      public int MaxRetryCount { get; set; } = 3;

      /// <summary>
      /// Initializes a new instance of the <see cref="T:Storage.Net.Sftp.SshNetSftpBlobStorage" /> class.
      /// </summary>
      /// <param name="connectionInfo">The connection info.</param>
      /// <exception cref="T:System.ArgumentNullException"><paramref name="connectionInfo" /> is <b>null</b>.</exception>
      public SshNetSftpBlobStorage(ConnectionInfo connectionInfo)
        : this(new SftpClient(connectionInfo), true)
      {
      }

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
      public SshNetSftpBlobStorage(string host, int port, string username, string password)
        : this(new SftpClient(host, port, username, password), true)
      {
      }

      /// <summary>
      /// Initializes a new instance of the <see cref="T:Storage.Net.Sftp.SshNetSftpBlobStorage" /> class.
      /// </summary>
      /// <param name="host">Connection host.</param>
      /// <param name="username">Authentication username.</param>
      /// <param name="password">Authentication password.</param>
      /// <exception cref="T:System.ArgumentNullException"><paramref name="password" /> is <b>null</b>.</exception>
      /// <exception cref="T:System.ArgumentException"><paramref name="host" /> is invalid. <para>-or-</para> <paramref name="username" /> is <b>null</b> contains only whitespace characters.</exception>
      public SshNetSftpBlobStorage(string host, string username, string password)
        : this(new SftpClient(host, username, password), true)
      {
      }

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
      public SshNetSftpBlobStorage(string host, int port, string username, params PrivateKeyFile[] keyFiles)
        : this(new SftpClient(host, port, username, keyFiles), true)
      {
      }

      /// <summary>
      /// Initializes a new instance of the <see cref="T:Storage.Net.Sftp.SshNetSftpBlobStorage" /> class.
      /// </summary>
      /// <param name="host">Connection host.</param>
      /// <param name="username">Authentication username.</param>
      /// <param name="keyFiles">Authentication private key file(s) .</param>
      /// <exception cref="T:System.ArgumentNullException"><paramref name="keyFiles" /> is <b>null</b>.</exception>
      /// <exception cref="T:System.ArgumentException"><paramref name="host" /> is invalid. <para>-or-</para> <paramref name="username" /> is <b>null</b> or contains only whitespace characters.</exception>
      public SshNetSftpBlobStorage(string host, string username, params PrivateKeyFile[] keyFiles)
        : this(new SftpClient(host, username, keyFiles), true)
      {
      }

      /// <summary>
      /// Initializes a new instance of the <see cref="T:Storage.Net.Sftp.SshNetSftpBlobStorage" /> class.
      /// </summary>
      /// <param name="sftpClient">The SFTP client.</param>
      /// <param name="disposeClient">if set to <see langword="true" /> [dispose client].</param>
      /// <exception cref="System.ArgumentNullException">sftpClient</exception>
      public SshNetSftpBlobStorage(SftpClient sftpClient, bool disposeClient = false)
      {
         _client = sftpClient ?? throw new ArgumentNullException(nameof(sftpClient));
         _client.HostKeyReceived += (sender, args) => { };
         _disposeClient = disposeClient;
      }

      /// <summary>
      /// Deletes a list of objects by their full path.
      /// </summary>
      /// <param name="fullPaths">The collection of full paths to delete. If this paths points to a folder, the folder is deleted recursively.</param>
      /// <param name="cancellationToken"></param>
      /// <returns></returns>
      public async Task DeleteAsync(IEnumerable<string> fullPaths, CancellationToken cancellationToken = default)
      {
         ThrowIfDisposed();

         SftpClient client = GetClient();

         await Task.WhenAll(fullPaths.Select(fullPath => DeleteAsync(fullPath, client, cancellationToken))).ConfigureAwait(false);
      }

      /// <summary>
      /// Deletes an object by it's full path.
      /// </summary>
      /// <param name="fullPath">The full path.</param>
      /// <param name="client">The sftp client to use.</param>
      /// <param name="cancellationToken">The cancellation token.</param>
      /// <returns></returns>
      private Task DeleteAsync(string fullPath, SftpClient client, CancellationToken cancellationToken = default)
      {
         if (cancellationToken.IsCancellationRequested)
         {
            return Task.FromCanceled(cancellationToken);
         }

         fullPath = StoragePath.Normalize(fullPath);

         client.Delete(fullPath);

         return Task.CompletedTask;
      }

      /// <summary>
      /// Determine whether the blobs exists in the storage
      /// </summary>
      /// <param name="fullPaths">List of paths to blobs</param>
      /// <param name="cancellationToken"></param>
      /// <returns>
      /// List of results of true and false indicating existence
      /// </returns>
      public async Task<IReadOnlyCollection<bool>> ExistsAsync(IEnumerable<string> fullPaths, CancellationToken cancellationToken = default)
      {
         ThrowIfDisposed();

         SftpClient client = GetClient();

         return await Task.WhenAll(fullPaths.Select(fullPath => ExistsAsync(fullPath, client, cancellationToken))).ConfigureAwait(false);
      }

      /// <summary>
      /// Determine whether the blobs exists in the storage
      /// </summary>
      /// <param name="fullPath">List of paths to blobs</param>
      /// <param name="client">The sftp client to use.</param>
      /// <param name="cancellationToken"></param>
      /// <returns>
      /// List of results of true and false indicating existence
      /// </returns>
      private Task<bool> ExistsAsync(string fullPath, SftpClient client, CancellationToken cancellationToken = default)
      {
         if (cancellationToken.IsCancellationRequested)
         {
            return Task.FromCanceled<bool>(cancellationToken);
         }

         fullPath = StoragePath.Normalize(fullPath);

         bool fullPathExists = client.Exists(fullPath);

         return Task.FromResult(fullPathExists);
      }

      /// <summary>
      /// Gets blob information which is useful for retrieving blob metadata
      /// </summary>
      /// <param name="fullPaths"></param>
      /// <param name="cancellationToken"></param>
      /// <returns>
      /// List of blob IDs
      /// </returns>
      public async Task<IReadOnlyCollection<Blob>> GetBlobsAsync(IEnumerable<string> fullPaths, CancellationToken cancellationToken = default)
      {
         ThrowIfDisposed();

         SftpClient client = GetClient();

         var results = new List<Blob>();
         foreach (IGrouping<string, string> fullPathGrouping in fullPaths.GroupBy(StoragePath.GetParent))
         {
            string fullPath = StoragePath.Normalize(fullPathGrouping.SingleOrDefault());

            if (cancellationToken.IsCancellationRequested)
            {
               break;
            }

            IEnumerable<SftpFile> directoryContents = await client.ListDirectoryAsync(fullPathGrouping.Key);

            IEnumerable<Blob> blobCollection = directoryContents
               .Where(f => (f.IsDirectory || f.IsRegularFile) && f.FullName == fullPath)
               .Select(ConvertSftpFileToBlob);

            results.AddRange(blobCollection);
         }

         return results;
      }

      /// <summary>
      /// Returns the list of available blobs
      /// </summary>
      /// <param name="options"></param>
      /// <param name="cancellationToken"></param>
      /// <returns>
      /// List of blob IDs
      /// </returns>
      public async Task<IReadOnlyCollection<Blob>> ListAsync(ListOptions options = null, CancellationToken cancellationToken = default)
      {
         ThrowIfDisposed();

         options ??= new ListOptions();
         options.MaxResults ??= int.MaxValue;
         options.BrowseFilter ??= _ => true;

         SftpClient client = GetClient();

         IEnumerable<SftpFile> directoryContents = await client.ListDirectoryAsync(options.FolderPath);

         IEnumerable<Blob> blobCollection = directoryContents
            .Where(dc => (options.FilePrefix == null || dc.Name.StartsWith(options.FilePrefix))
                         && (dc.IsDirectory || dc.IsRegularFile || dc.OwnerCanRead)
                         && !cancellationToken.IsCancellationRequested
                         && dc.Name != "."
                         && dc.Name != "..")
            .Take(options.MaxResults.Value)
            .Select(ConvertSftpFileToBlob)
            .Where(options.BrowseFilter);

         return blobCollection.ToList();
      }

      /// <summary>
      /// Opens the blob stream to read.
      /// </summary>
      /// <param name="fullPath">Blob's full path</param>
      /// <param name="cancellationToken"></param>
      /// <returns>
      /// Stream in an open state, or null if blob doesn't exist by this ID. It is your responsibility to close and dispose this
      /// stream after use.
      /// </returns>
      public async Task<Stream> OpenReadAsync(string fullPath, CancellationToken cancellationToken = default)
      {
         ThrowIfDisposed();

         fullPath = StoragePath.Normalize(fullPath);

         SftpClient client = GetClient();

         try
         {
            byte[] fileBytes = await Task.FromResult(Policy.Handle<Exception>().Retry(MaxRetryCount).Execute(() => client.ReadAllBytes(fullPath)));
            return new MemoryStream(fileBytes);
         }
         catch (Exception /*exception*/)
         {
            return null;
         }
      }

      /// <summary>
      /// Starts a new transaction
      /// </summary>
      /// <returns></returns>
      public Task<ITransaction> OpenTransactionAsync()
      {
         ThrowIfDisposed();
         return Task.FromResult(EmptyTransaction.Instance);
      }

      /// <summary>
      /// Rename a blob (folder or file)
      /// </summary>
      /// <param name="oldPath"></param>
      /// <param name="newPath"></param>
      /// <param name="cancellationToken"></param>
      /// <returns></returns>
      public Task RenameAsync(string oldPath, string newPath, CancellationToken cancellationToken = default)
      {
         ThrowIfDisposed();

         oldPath = StoragePath.Normalize(oldPath);
         newPath = StoragePath.Normalize(newPath);

         SftpClient client = GetClient();

         client.RenameFile(oldPath, newPath);

         return Task.CompletedTask;
      }

      /// <summary>
      /// Set blob information which is useful for setting blob attributes (user metadata etc.)
      /// </summary>
      /// <param name="blobs"></param>
      /// <param name="cancellationToken"></param>
      /// <returns></returns>
      /// <exception cref="System.NotSupportedException"></exception>
      public Task SetBlobsAsync(IEnumerable<Blob> blobs, CancellationToken cancellationToken = default)
      {
         ThrowIfDisposed();
         throw new NotSupportedException();
      }

      /// <summary>
      /// Uploads data to a blob from stream.
      /// </summary>
      /// <param name="fullPath">Blob metadata</param>
      /// <param name="dataStream">Stream to upload from</param>
      /// <param name="append">When true, appends to the file instead of writing a new one.</param>
      /// <param name="cancellationToken"></param>
      /// <returns>
      /// Writeable stream
      /// </returns>
      public async Task WriteAsync(string fullPath, Stream dataStream, bool append = false, CancellationToken cancellationToken = default)
      {
         ThrowIfDisposed();

         fullPath = StoragePath.Normalize(fullPath);

         SftpClient client = GetClient();

         await _retryPolicy.ExecuteAsync(async () =>
         {
            using(Stream dest = client.OpenWrite(fullPath))
            {
               await dataStream.CopyToAsync(dest).ConfigureAwait(false);
            }
         }).ConfigureAwait(false);
      }

      /// <summary>
      /// Gets the <see cref="T:Renci.SshNet.SftpClient" /> instance.
      /// </summary>
      /// <returns>The <see cref="T:Renci.SshNet.SftpClient" /> instance.</returns>
      protected SftpClient GetClient()
      {
         ThrowIfDisposed();

         if (!_client.IsConnected)
         {
            _client.Connect();
         }

         return _client;
      }

      /// <summary>
      /// Converts the specified <see cref="T:Renci.SshNet.Sftp.SftpFile"/> into a <see cref="T:Storage.Net.Blobs.Blob"/> instance.
      /// </summary>
      /// <param name="file">The file.</param>
      /// <returns></returns>
      private static Blob ConvertSftpFileToBlob(SftpFile file)
      {
         if (file.IsDirectory || file.IsRegularFile || file.OwnerCanRead)
         {
            BlobItemKind itemKind = file.IsDirectory
               ? BlobItemKind.Folder
               : BlobItemKind.File;

            return new Blob(file.FullName, itemKind)
            {
               Size = file.Length,
               LastModificationTime = file.LastWriteTime
            };
         }

         return null;
      }

      /// <summary>
      /// Performs application-defined tasks associated with freeing, releasing, or resetting unmanaged resources.
      /// </summary>
      public void Dispose()
      {
         Dispose(true);
         GC.SuppressFinalize(this);
      }

      /// <summary>
      /// Releases unmanaged and - optionally - managed resources.
      /// </summary>
      /// <param name="disposing"><see langword="true" /> to release both managed and unmanaged resources; <see langword="false" /> to release only unmanaged resources.</param>
      protected virtual void Dispose(bool disposing)
      {
         if (_disposed)
         {
            return;
         }

         // Release any managed resources here.
         if (disposing && _disposeClient)
         {
            _client.Dispose();
         }

         _disposed = true;
      }

      /// <summary>
      /// Throws an <see cref="T:System.ObjectDisposedException" /> if this object has been disposed.
      /// </summary>
      /// <exception cref="T:System.ObjectDisposedException">The current instance is disposed.</exception>
      protected void ThrowIfDisposed()
      {
         if (_disposed)
         {
            throw new ObjectDisposedException(GetType().FullName);
         }
      }
   }
}
