using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Azure.DataLake.Store;
using Microsoft.Azure.DataLake.Store.RetryPolicies;
using Storage.Net.Blob;

namespace Storage.Net.Microsoft.Azure.DataLake.Store.Blob
{
   class DirectoryBrowser
   {
      private readonly AdlsClient _client;
      private readonly int _listBatchSize;

      public DirectoryBrowser(AdlsClient client, int listBatchSize)
      {
         _client = client ?? throw new ArgumentNullException(nameof(client));
         _listBatchSize = listBatchSize;
      }

      public async Task<IReadOnlyCollection<BlobId>> Browse(ListOptions options, CancellationToken token)
      {
         string path = StoragePath.Normalize(options.FolderPath);
         var result = new List<BlobId>();

         await Browse(path, options, result, token);

         return result;
      }

      private async Task Browse(string path, ListOptions options, ICollection<BlobId> container, CancellationToken token)
      {
         List<BlobId> batch;

         try
         {
            IEnumerable<BlobId> entries = 
               (await EnumerateDirectoryAsync(path, options, UserGroupRepresentation.ObjectID))
               .Where(options.IsMatch);

            if (options.BrowseFilter != null)
            {
               entries = entries.Where(options.BrowseFilter);
            }

            batch = entries.ToList();
         }
         //skip files with forbidden access
         catch (AdlsException ex) when (ex.HttpStatus == HttpStatusCode.Forbidden || ex.HttpStatus == HttpStatusCode.NotFound)
         {
            batch = null;
         }

         if (batch == null) return;

         if (options.Add(container, batch)) return;

         if(options.Recurse)
         {
            List<BlobId> folders = batch.Where(b => b.Kind == BlobItemKind.Folder).ToList();

            if(folders.Count > 0)
            {
               await Task.WhenAll(
                  folders.Select(bid => Browse(
                     StoragePath.Combine(path, bid.Id),
                     options,
                     container,
                     token
                  )));
            }
         }
      }
      private async Task<IReadOnlyCollection<BlobId>> EnumerateDirectoryAsync(
         string path,
         ListOptions options,
         UserGroupRepresentation userIdFormat = UserGroupRepresentation.ObjectID,
         CancellationToken cancelToken = default)
      {
         var result = new List<BlobId>();

         string listAfter = "";

         while(options.MaxResults == null || result.Count < options.MaxResults.Value)
         {
            List<DirectoryEntry> page = await EnumerateDirectoryAsync(path, _listBatchSize, listAfter, "", userIdFormat, cancelToken);

            //no more results
            if(page == null || page.Count == 0)
            {
               break;
            }

            //set pointer to next page
            listAfter = page[page.Count - 1].Name;

            result.AddRange(page.Select(p => ToBlobId(path, p, options.IncludeMetaWhenKnown)));
         }

         return result;
      }

      internal async Task<List<DirectoryEntry>> EnumerateDirectoryAsync(string path,
         int maxEntries, string listAfter, string listBefore, UserGroupRepresentation userIdFormat = UserGroupRepresentation.ObjectID, CancellationToken cancelToken = default(CancellationToken))
      {
         if (string.IsNullOrEmpty(path)) throw new ArgumentException("Path is null");

         var resp = new OperationResponse();
         List<DirectoryEntry> page = await Core.ListStatusAsync(path, listAfter, listBefore, maxEntries, userIdFormat, _client,
            new RequestOptions(new ExponentialRetryPolicy(2, 1000)),
            resp);
         return page;
         //return new FileStatusOutput(listBefore, listAfter, maxEntries, userIdFormat, _client, path);
      }

      private static BlobId ToBlobId(string path, DirectoryEntry entry, bool includeMeta)
      {
         BlobMeta meta = includeMeta ? new BlobMeta(entry.Length, null, entry.LastModifiedTime) : null;

         if (entry.Type == DirectoryEntryType.FILE)
            return new BlobId(path, entry.Name, BlobItemKind.File) { Meta = meta };
         else
            return new BlobId(path, entry.Name, BlobItemKind.Folder) { Meta = meta };
      }
   }
}
