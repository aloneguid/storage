using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.WindowsAzure.Storage.Blob;
using Storage.Net.Blob;

namespace Storage.Net.Microsoft.Azure.Storage.Blob
{
   class AzureBlobDirectoryBrowser
   {
      private readonly CloudBlobContainer _container;
      private readonly bool _prependContainerName;
      private readonly SemaphoreSlim _throttler;

      public AzureBlobDirectoryBrowser(CloudBlobContainer container, bool prependContainerName, int maxTasks)
      {
         _container = container;
         _prependContainerName = prependContainerName;
         _throttler = new SemaphoreSlim(maxTasks);
      }

      public async Task<IReadOnlyCollection<BlobId>> ListFolderAsync(ListOptions options, CancellationToken cancellationToken)
      {
         var result = new List<BlobId>();

         await ListFolderAsync(result, options.FolderPath, options, cancellationToken);

         return result;
      }

      private async Task ListFolderAsync(List<BlobId> container, string path, ListOptions options, CancellationToken cancellationToken)
      {
         CloudBlobDirectory dir = GetCloudBlobDirectory(path);

         BlobContinuationToken token = null;

         var batch = new List<BlobId>();

         await _throttler.WaitAsync();
         try
         {
            do
            {
               BlobResultSegment segment = await dir.ListBlobsSegmentedAsync(
                  false, BlobListingDetails.None, null, token, null, null, cancellationToken);

               token = segment.ContinuationToken;

               foreach (IListBlobItem blob in segment.Results)
               {
                  BlobId id = ToBlobId(blob, options.IncludeMetaWhenKnown);

                  if (options.IsMatch(id) && (options.BrowseFilter == null || options.BrowseFilter(id)))
                  {
                     batch.Add(id);
                  }
               }

            }
            while (token != null && ((options.MaxResults == null) || (container.Count + batch.Count < options.MaxResults.Value)));
         }
         finally
         {
            _throttler.Release();
         }

         batch = batch.Where(options.IsMatch).ToList();
         if (options.Add(container, batch)) return;

         options.ListProgressCallback?.Invoke(container.Count, -1);

         if (options.Recurse)
         {
            List<BlobId> folderIds = batch.Where(r => r.Kind == BlobItemKind.Folder).ToList();

            await Task.WhenAll(
               folderIds.Select(folderId => ListFolderAsync(
                  container,
                  StoragePath.Combine(path, folderId.Id),
                  options,
                  cancellationToken)));
         }
      }

      private CloudBlobDirectory GetCloudBlobDirectory(string path)
      {
         path = path == null ? string.Empty : path.Trim(StoragePath.PathSeparator);

         CloudBlobDirectory dir = _container.GetDirectoryReference(path);

         return dir;
      }

      private BlobId ToBlobId(IListBlobItem blob, bool attachMetadata)
      {
         BlobId id;

         if (blob is CloudBlockBlob blockBlob)
         {
            string fullName = _prependContainerName
               ? StoragePath.Combine(_container.Name, blockBlob.Name)
               : blockBlob.Name;

            id = new BlobId(fullName, BlobItemKind.File);
         }
         else if (blob is CloudAppendBlob appendBlob)
         {
            string fullName = _prependContainerName
               ? StoragePath.Combine(_container.Name, appendBlob.Name)
               : appendBlob.Name;

            id = new BlobId(fullName, BlobItemKind.File);
         }
         else if (blob is CloudBlobDirectory dirBlob)
         {
            string fullName = _prependContainerName
               ? StoragePath.Combine(_container.Name, dirBlob.Prefix)
               : dirBlob.Prefix;

            id = new BlobId(fullName, BlobItemKind.Folder);
         }
         else
         {
            throw new InvalidOperationException($"unknown item type {blob.GetType()}");
         }

         //attach metadata if we can
         if(attachMetadata && blob is CloudBlob cloudBlob)
         {
            id.Meta = AzureUniversalBlobStorageProvider.GetblobMeta(cloudBlob);
         }

         return id;

      }

   }
}
