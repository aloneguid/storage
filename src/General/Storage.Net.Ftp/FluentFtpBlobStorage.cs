using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net;
using System.Threading;
using System.Threading.Tasks;
using FluentFTP;
using Polly;
using Polly.Retry;
using Storage.Net.Blobs;

namespace Storage.Net.Ftp
{
   class FluentFtpBlobStorage : IBlobStorage
   {
      private readonly FtpClient _client;
      private readonly bool _dispose;
      private static readonly AsyncRetryPolicy retryPolicy = Policy.Handle<FtpException>().RetryAsync(3);

      public FluentFtpBlobStorage(
         string hostNameOrAddress,
         NetworkCredential credentials,
         FtpDataConnectionType dataConnectionType = FtpDataConnectionType.AutoActive)
         : this(new FtpClient(hostNameOrAddress, credentials) { DataConnectionType = dataConnectionType }, true)
      {
         
      }

      public FluentFtpBlobStorage(FtpClient ftpClient, bool dispose = false)
      {
         _client = ftpClient ?? throw new ArgumentNullException(nameof(ftpClient));
         _dispose = dispose;
      }

      private async Task<FtpClient> GetClientAsync()
      {
         if(!_client.IsConnected)
         {
            await _client.ConnectAsync().ConfigureAwait(false);

            //not supported on this platform?
            //await _client.SetHashAlgorithmAsync(FtpHashAlgorithm.MD5);
         }

         return _client;
      }

      public async Task<IReadOnlyCollection<Blob>> ListAsync(ListOptions options = null, CancellationToken cancellationToken = default)
      {
         FtpClient client = await GetClientAsync().ConfigureAwait(false);

         if (options == null) options = new ListOptions();

         FtpListItem[] items = await client.GetListingAsync(options.FolderPath).ConfigureAwait(false);

         var results = new List<Blob>();
         foreach(FtpListItem item in items)
         {
            if(options.FilePrefix != null && !item.Name.StartsWith(options.FilePrefix))
            {
               continue;
            }

            Blob blob = ToBlobId(item);
            if (blob == null) continue;

            if(options.BrowseFilter != null)
            {
               bool include = options.BrowseFilter(blob);
               if (!include) continue;
            }

            results.Add(blob);

            if (options.MaxResults != null && results.Count >= options.MaxResults.Value) break;
         }

         return results;
      }

      private Blob ToBlobId(FtpListItem ff)
      {
         if (ff.Type != FtpFileSystemObjectType.Directory && ff.Type != FtpFileSystemObjectType.File) return null;

         var id = new  Blob(ff.FullName,
            ff.Type == FtpFileSystemObjectType.File
            ? BlobItemKind.File
            : BlobItemKind.Folder);

         if (ff.RawPermissions != null)
         {
            id.Properties["RawPermissions"] = ff.RawPermissions;
         }

         return id;
      }

      public async Task DeleteAsync(IEnumerable<string> fullPaths, CancellationToken cancellationToken = default)
      {
         FtpClient client = await GetClientAsync().ConfigureAwait(false);

         foreach(string path in fullPaths)
         {
            try
            {
               await client.DeleteFileAsync(path).ConfigureAwait(false);
            }
            catch(FtpCommandException ex) when(ex.CompletionCode == "550")
            {
               await client.DeleteDirectoryAsync(path, cancellationToken).ConfigureAwait(false);
               //550 stands for "file not found" or "permission denied".
               //"not found" is fine to ignore, however I'm not happy about ignoring the second error.
            }
         }
      }

      public async Task<IReadOnlyCollection<bool>> ExistsAsync(IEnumerable<string> ids, CancellationToken cancellationToken = default)
      {
         FtpClient client = await GetClientAsync().ConfigureAwait(false);

         var results = new List<bool>();
         foreach (string path in ids)
         {
            bool e = await client.FileExistsAsync(path).ConfigureAwait(false);
            results.Add(e);
         }

         return results;
      }

      public async Task<IReadOnlyCollection<Blob>> GetBlobsAsync(IEnumerable<string> ids, CancellationToken cancellationToken = default)
      {
         FtpClient client = await GetClientAsync().ConfigureAwait(false);

         var results = new List<Blob>();
         foreach(string path in ids)
         {
            string cpath = StoragePath.Normalize(path, true);
            string parentPath = StoragePath.GetParent(cpath);

            FtpListItem[] all = await client.GetListingAsync(parentPath).ConfigureAwait(false);
            FtpListItem foundItem = all.FirstOrDefault(i => i.FullName == cpath);

            if(foundItem == null)
            {
               results.Add(null);
               continue;
            }

            var r = new Blob(path)
            {
               Size = foundItem.Size,
               LastModificationTime = foundItem.Modified
            };
            results.Add(r);
         }
         return results;
      }

      public Task SetBlobsAsync(IEnumerable<Blob> blobs, CancellationToken cancellationToken = default)
      {
         throw new NotSupportedException();
      }

      public async Task<Stream> OpenReadAsync(string fullPath, CancellationToken cancellationToken = default)
      {
         FtpClient client = await GetClientAsync().ConfigureAwait(false);

         try
         {
            return await client.OpenReadAsync(fullPath, FtpDataType.Binary, 0, true).ConfigureAwait(false);
         }
         catch(FtpCommandException ex) when (ex.CompletionCode == "550")
         {
            return null;
         }
      }

      public Task<ITransaction> OpenTransactionAsync() => Task.FromResult(EmptyTransaction.Instance);

      public async Task<Stream> OpenWriteAsync(string fullPath, bool append = false, CancellationToken cancellationToken = default)
      {
         FtpClient client = await GetClientAsync().ConfigureAwait(false);

         return await retryPolicy.ExecuteAsync<Stream>(async () =>
         {
            return await client.OpenWriteAsync(fullPath, FtpDataType.Binary, true).ConfigureAwait(false);
         }).ConfigureAwait(false);
      }

      public void Dispose()
      {
         if (_dispose && _client.IsDisposed)
         {
            _client.Dispose();
         }
      }
   }
}
