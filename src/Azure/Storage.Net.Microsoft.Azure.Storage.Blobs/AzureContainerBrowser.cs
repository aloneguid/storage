using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Azure.Storage.Blobs;
using Azure.Storage.Blobs.Models;
using NetBox.Async;
using Storage.Net.Blobs;

namespace Storage.Net.Microsoft.Azure.Storage.Blobs
{
   class AzureContainerBrowser : IDisposable
   {
      private readonly BlobContainerClient _client;
      private readonly bool _prependContainerName;
      private readonly AsyncLimiter _asyncLimiter;

      public AzureContainerBrowser(BlobContainerClient client, bool prependContainerName, int maxTasks)
      {
         _client = client ?? throw new ArgumentNullException(nameof(client));
         _prependContainerName = prependContainerName;
         _asyncLimiter = new AsyncLimiter(maxTasks);
      }

      public async Task<IReadOnlyCollection<Blob>> ListFolderAsync(ListOptions options, CancellationToken cancellationToken)
      {
         var result = new List<Blob>();

         await foreach(BlobHierarchyItem item in
            _client.GetBlobsByHierarchyAsync(
               delimiter: options.Recurse ? null : "/",
               prefix: FormatFolderPrefix(options.FolderPath),
               traits: options.IncludeAttributes ? BlobTraits.Metadata : BlobTraits.None).ConfigureAwait(false))
         {

            Blob blob = AzConvert.ToBlob(_prependContainerName ? _client.Name : null, item);

            if(options.IsMatch(blob) && (options.BrowseFilter == null || options.BrowseFilter(blob)))
            {
               result.Add(blob);
            }
         }

         if(options.Recurse)
         {
            AssumeImplicitPrefixes(
               _prependContainerName ? StoragePath.Combine(_client.Name, options.FolderPath) : options.FolderPath,
               result);
         }

         return result;
      }

      private static void AssumeImplicitPrefixes(string absoluteRoot, List<Blob> blobs)
      {
         absoluteRoot = StoragePath.Normalize(absoluteRoot);

         List<Blob> implicitFolders = blobs
            .Select(b => b.FullPath)
            .Select(p => p.Substring(absoluteRoot.Length))
            .Select(p => StoragePath.GetParent(p))
            .Where(p => !StoragePath.IsRootPath(p))
            .Distinct()
            .Select(p => new Blob(p, BlobItemKind.Folder))
            .ToList();

         blobs.AddRange(implicitFolders);
      }

      private static string FormatFolderPrefix(string folderPath)
      {
         folderPath = StoragePath.Normalize(folderPath).Substring(1);

         if(StoragePath.IsRootPath(folderPath))
            return null;

         if(!folderPath.EndsWith("/"))
            folderPath += "/";

         return folderPath;
      }

      public void Dispose()
      {
         _asyncLimiter.Dispose();
      }
   }
}
