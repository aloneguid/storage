using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Azure.Storage;
using Microsoft.Azure.Storage.Auth;
using Microsoft.Azure.Storage.File;
using Storage.Net.Blobs;
using Storage.Net.Streaming;

namespace Storage.Net.Microsoft.Azure.Storage.Blobs
{
   class AzureFilesBlobStorage : GenericBlobStorage
   {
      private readonly CloudFileClient _client;

      public AzureFilesBlobStorage(CloudFileClient client)
      {
         _client = client ?? throw new ArgumentNullException(nameof(client));
      }

      public static AzureFilesBlobStorage CreateFromAccountNameAndKey(string accountName, string key)
      {
         if(accountName == null)
            throw new ArgumentNullException(nameof(accountName));

         if(key == null)
            throw new ArgumentNullException(nameof(key));

         var account = new CloudStorageAccount(
            new StorageCredentials(accountName, key),
            true);

         return new AzureFilesBlobStorage(account.CreateCloudFileClient());
      }

      public override async Task<IReadOnlyCollection<Blob>> ListAsync(ListOptions options = null, CancellationToken cancellationToken = default)
      {
         if(options == null) options = new ListOptions();
         var result = new List<Blob>();

         (CloudFileShare share, string path) = await GetPathPartsAsync(options.FolderPath);

         return result;
      }

      public override async Task<Stream> OpenWriteAsync(string fullPath, bool append = false, CancellationToken cancellationToken = default)
      {
         CloudFile file = await GetFileReferenceAsync(fullPath, true, cancellationToken).ConfigureAwait(false);

         return new FixedStream(new MemoryStream(), null, async (fx) =>
         {
            var ms = (MemoryStream)fx.Parent;
            ms.Position = 0;

            await file.UploadFromStreamAsync(ms).ConfigureAwait(false);
         });
      }

      protected override async Task DeleteSingleAsync(string fullPath, CancellationToken cancellationToken)
      {
         CloudFile file = await GetFileReferenceAsync(fullPath, cancellationToken: cancellationToken).ConfigureAwait(false);

         await file.DeleteIfExistsAsync(cancellationToken).ConfigureAwait(false);
      }

      protected override async Task<bool> ExistsAsync(string fullPath, CancellationToken cancellationToken)
      {
         CloudFile file = await GetFileReferenceAsync(fullPath, cancellationToken: cancellationToken).ConfigureAwait(false);

         if(file == null)
            return false;

         return await file.ExistsAsync().ConfigureAwait(false);
      }

      private async Task<(CloudFileShare, string)> GetPathPartsAsync(string fullPath, bool createShare = false, CancellationToken cancellationToken = default)
      {
         string[] parts = StoragePath.Split(fullPath);

         if(parts.Length == 0)
            return (null, null);

         string shareName = parts[0];

         CloudFileShare share = _client.GetShareReference(shareName);
         if(createShare)
            await share.CreateIfNotExistsAsync(cancellationToken).ConfigureAwait(false);

         string path = parts.Length == 1
            ? StoragePath.RootFolderPath
            : StoragePath.Combine(parts.Skip(1));

         return (share, path);
      }

      private async Task<CloudFile> GetFileReferenceAsync(string fullPath, bool createParents = false, CancellationToken cancellationToken = default)
      {
         string[] parts = StoragePath.Split(fullPath);
         if(parts.Length == 0)
            return null;

         string shareName = parts[0];

         CloudFileShare share = _client.GetShareReference(shareName);
         if(createParents)
            await share.CreateIfNotExistsAsync(cancellationToken).ConfigureAwait(false);

         CloudFileDirectory dir = share.GetRootDirectoryReference();
         for(int i = 1; i < parts.Length - 1; i++)
         {
            string sub = parts[i];
            dir = dir.GetDirectoryReference(sub);
         }

         if(createParents)
            await dir.CreateIfNotExistsAsync().ConfigureAwait(false);

         return dir.GetFileReference(parts[parts.Length - 1]);
      }
   }
}
