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
using AzStorageException = Microsoft.Azure.Storage.StorageException;

namespace Storage.Net.Microsoft.Azure.Storage.Files
{
   class AzureFilesBlobStorage : GenericBlobStorage
   {
      private readonly CloudFileClient _client;

      protected override bool CanListHierarchy => false;

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

      protected override async Task<IReadOnlyCollection<Blob>> ListAtAsync(
         string path, ListOptions options, CancellationToken cancellationToken)
      {
         if(StoragePath.IsRootPath(path))
         {
            //list file shares

            ShareResultSegment shares = await _client.ListSharesSegmentedAsync(null, cancellationToken).ConfigureAwait(false);

            return shares.Results.Select(AzConvert.ToBlob).ToList();
         }
         else
         {
            var chunk = new List<Blob>();

            CloudFileDirectory dir = await GetDirectoryReferenceAsync(path, cancellationToken).ConfigureAwait(false);

            FileContinuationToken token = null;
            do
            {
               try
               {
                  FileResultSegment segment = await dir.ListFilesAndDirectoriesSegmentedAsync(options.FilePrefix, token, cancellationToken).ConfigureAwait(false);

                  token = segment.ContinuationToken;

                  chunk.AddRange(segment.Results.Select(r => AzConvert.ToBlob(path, r)));
               }
               catch(AzStorageException ex) when(ex.RequestInformation.ErrorCode == "ShareNotFound")
               {
                  break;
               }
               catch(AzStorageException ex) when(ex.RequestInformation.ErrorCode == "ResourceNotFound")
               {
                  break;
               }
            }
            while(token != null);

            return chunk;
         }
      }

      public override async Task WriteAsync(
         string fullPath,
         Stream dataStream,
         bool append = false,
         CancellationToken cancellationToken = default)
      {
         CloudFile file = await GetFileReferenceAsync(fullPath, true, cancellationToken).ConfigureAwait(false);

         await file.UploadFromStreamAsync(new StorageSourceStream(dataStream), cancellationToken).ConfigureAwait(false);
      }

      public override async Task<Stream> OpenReadAsync(string fullPath, CancellationToken cancellationToken = default)
      {
         CloudFile file = await GetFileReferenceAsync(fullPath, true, cancellationToken).ConfigureAwait(false);

         try
         {
            return await file.OpenReadAsync(cancellationToken).ConfigureAwait(false);
         }
         catch(AzStorageException ex) when(ex.RequestInformation.ErrorCode == "ResourceNotFound")
         {
            return null;
         }
      }

      protected override async Task<Blob> GetBlobAsync(string fullPath, CancellationToken cancellationToken)
      {
         CloudFile file = await GetFileReferenceAsync(fullPath, false, cancellationToken).ConfigureAwait(false);

         try
         {
            await file.FetchAttributesAsync(cancellationToken).ConfigureAwait(false);

            return AzConvert.ToBlob(StoragePath.GetParent(fullPath), file);
         }
         catch(AzStorageException ex) when(ex.RequestInformation.ErrorCode == "ShareNotFound")
         {
            return null;
         }
         catch(AzStorageException ex) when(ex.RequestInformation.ErrorCode == "ResourceNotFound")
         {
            return null;
         }
      }

      protected override async Task DeleteSingleAsync(string fullPath, CancellationToken cancellationToken)
      {
         CloudFile file = await GetFileReferenceAsync(fullPath, false, cancellationToken).ConfigureAwait(false);

         try
         {
            await file.DeleteAsync(cancellationToken).ConfigureAwait(false);
         }
         catch(AzStorageException ex) when(ex.RequestInformation.ErrorCode == "ResourceNotFound")
         {
            //this may be a folder

            CloudFileDirectory dir = await GetDirectoryReferenceAsync(fullPath, cancellationToken).ConfigureAwait(false);
            if(await dir.ExistsAsync().ConfigureAwait(false))
            {
               await DeleteDirectoryAsync(dir, cancellationToken).ConfigureAwait(false);
               await dir.DeleteIfExistsAsync().ConfigureAwait(false);
            }
         }
      }

      private async Task DeleteDirectoryAsync(CloudFileDirectory dir, CancellationToken cancellationToken)
      {
         FileContinuationToken token = null;

         do
         {
            FileResultSegment chunk = await dir.ListFilesAndDirectoriesSegmentedAsync(token, cancellationToken).ConfigureAwait(false);

            foreach(IListFileItem item in chunk.Results)
            {
               if(item is CloudFile file)
               {
                  await file.DeleteIfExistsAsync(cancellationToken).ConfigureAwait(false);
               }
               else if(item is CloudFileDirectory subdir)
               {
                  await DeleteDirectoryAsync(subdir, cancellationToken).ConfigureAwait(false);
                  await subdir.DeleteIfExistsAsync().ConfigureAwait(false);
               }
            }

            token = chunk.ContinuationToken;
         }
         while(token != null);
      }

      protected override async Task<bool> ExistsAsync(string fullPath, CancellationToken cancellationToken)
      {
         CloudFile file = await GetFileReferenceAsync(fullPath, false, cancellationToken).ConfigureAwait(false);

         if(file == null)
            return false;

         return await file.ExistsAsync().ConfigureAwait(false);
      }

      private async Task<CloudFile> GetFileReferenceAsync(string fullPath, bool createParents, CancellationToken cancellationToken)
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

            if(createParents)
               await dir.CreateIfNotExistsAsync().ConfigureAwait(false);
         }

         return dir.GetFileReference(parts[parts.Length - 1]);
      }

      private Task<CloudFileDirectory> GetDirectoryReferenceAsync(string fullPath, CancellationToken cancellationToken)
      {
         string[] parts = StoragePath.Split(fullPath);
         if(parts.Length == 0)
            return null;

         string shareName = parts[0];

         CloudFileShare share = _client.GetShareReference(shareName);

         CloudFileDirectory dir = share.GetRootDirectoryReference();
         for(int i = 1; i < parts.Length; i++)
         {
            string sub = parts[i];
            dir = dir.GetDirectoryReference(sub);
         }

         return Task.FromResult(dir);
      }
   }
}