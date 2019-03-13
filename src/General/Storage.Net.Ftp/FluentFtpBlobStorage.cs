using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net;
using System.Threading;
using System.Threading.Tasks;
using FluentFTP;
using Storage.Net.Blob;

namespace Storage.Net.Ftp
{
   class FluentFtpBlobStorage : IBlobStorage
   {
      private readonly FtpClient _client;
      private readonly bool _dispose;

      public FluentFtpBlobStorage(string hostNameOrAddress, NetworkCredential credentials)
         : this(new FtpClient(hostNameOrAddress, credentials), true)
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
            await _client.ConnectAsync();

            //not supported on this platform?
            //await _client.SetHashAlgorithmAsync(FtpHashAlgorithm.MD5);
         }

         return _client;
      }

      public async Task<IReadOnlyCollection<BlobId>> ListAsync(ListOptions options = null, CancellationToken cancellationToken = default(CancellationToken))
      {
         FtpClient client = await GetClientAsync();

         if (options == null) options = new ListOptions();

         FtpListItem[] items = await client.GetListingAsync(options.FolderPath);

         var results = new List<BlobId>();
         foreach(FtpListItem item in items)
         {
            if(options.FilePrefix != null && !item.Name.StartsWith(options.FilePrefix))
            {
               continue;
            }

            BlobId bid = ToBlobId(item);
            if (bid == null) continue;

            if(options.BrowseFilter != null)
            {
               bool include = options.BrowseFilter(bid);
               if (!include) continue;
            }

            results.Add(bid);

            if (options.MaxResults != null && results.Count >= options.MaxResults.Value) break;
         }

         return results;
      }

      private BlobId ToBlobId(FtpListItem ff)
      {
         if (ff.Type != FtpFileSystemObjectType.Directory && ff.Type != FtpFileSystemObjectType.File) return null;

         var id = new  BlobId(ff.FullName,
            ff.Type == FtpFileSystemObjectType.File
            ? BlobItemKind.File
            : BlobItemKind.Folder);

         id.Properties = new Dictionary<string, string>();
         if (ff.RawPermissions != null)
         {
            id.Properties["RawPermissions"] = ff.RawPermissions;
         }

         return id;
      }

      public async Task DeleteAsync(IEnumerable<string> ids, CancellationToken cancellationToken = default)
      {
         FtpClient client = await GetClientAsync();

         foreach(string path in ids)
         {
            await client.DeleteFileAsync(path);
         }
      }

      public async Task<IReadOnlyCollection<bool>> ExistsAsync(IEnumerable<string> ids, CancellationToken cancellationToken = default)
      {
         FtpClient client = await GetClientAsync();

         var results = new List<bool>();
         foreach (string path in ids)
         {
            bool e = await client.FileExistsAsync(path);
            results.Add(e);
         }

         return results;
      }

      public async Task<IEnumerable<BlobMeta>> GetMetaAsync(IEnumerable<string> ids, CancellationToken cancellationToken = default)
      {
         FtpClient client = await GetClientAsync();

         var results = new List<BlobMeta>();
         foreach(string path in ids)
         {
            string cpath = StoragePath.Normalize(path, true);
            string parentPath = StoragePath.GetParent(cpath);

            FtpListItem[] all = await client.GetListingAsync(parentPath);
            FtpListItem foundItem = all.FirstOrDefault(i => i.FullName == cpath);

            if(foundItem == null)
            {
               results.Add(null);
               continue;
            }

            //FtpHash hash = await _client.GetHashAsync(cpath);

            var meta = new BlobMeta(foundItem.Size, null, foundItem.Modified);
            results.Add(meta);
         }
         return results;
      }
      
      public async Task<Stream> OpenReadAsync(string id, CancellationToken cancellationToken = default)
      {
         FtpClient client = await GetClientAsync();

         try
         {
            return await client.OpenReadAsync(id, FtpDataType.Binary, 0, true);
         }
         catch(FtpCommandException ex) when (ex.CompletionCode == "550")
         {
            return null;
         }
      }

      public Task<ITransaction> OpenTransactionAsync() => Task.FromResult(EmptyTransaction.Instance);

      public async Task<Stream> OpenWriteAsync(string id, bool append = false, CancellationToken cancellationToken = default)
      {
         FtpClient client = await GetClientAsync();

         return await client.OpenWriteAsync(id, FtpDataType.Binary, true);
      }

      public async Task WriteAsync(string id, Stream sourceStream, bool append = false, CancellationToken cancellationToken = default)
      {
         FtpClient client = await GetClientAsync();

         await client.UploadAsync(sourceStream, id, FtpExists.Overwrite, true, cancellationToken, null);
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
