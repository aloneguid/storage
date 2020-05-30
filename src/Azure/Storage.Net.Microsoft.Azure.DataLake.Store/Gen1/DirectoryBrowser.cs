using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Azure.DataLake.Store;
using Microsoft.Azure.DataLake.Store.RetryPolicies;
using Storage.Net.Blobs;

namespace Storage.Net.Microsoft.Azure.DataLake.Store.Gen1
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

      public async Task<IReadOnlyCollection<Blob>> BrowseAsync(ListOptions options, CancellationToken token)
      {
         string path = StoragePath.Normalize(options.FolderPath);
         var result = new List<Blob>();

         await BrowseAsync(path, options, result, token).ConfigureAwait(false);

         return result;
      }

      private async Task BrowseAsync(string path, ListOptions options, ICollection<Blob> container, CancellationToken token)
      {
         List<Blob> batch;

         try
         {
            IEnumerable<Blob> entries = 
               (await EnumerateDirectoryAsync(path, options, UserGroupRepresentation.ObjectID).ConfigureAwait(false))
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
            var folders = batch.Where(b => b.Kind == BlobItemKind.Folder).ToList();

            if(folders.Count > 0)
            {
               await Task.WhenAll(
                  folders.Select(b => BrowseAsync(
                     StoragePath.Combine(path, b.Name),
                     options,
                     container,
                     token
                  ))).ConfigureAwait(false);
            }
         }
      }
      private async Task<IReadOnlyCollection<Blob>> EnumerateDirectoryAsync(
         string path,
         ListOptions options,
         UserGroupRepresentation userIdFormat = UserGroupRepresentation.ObjectID,
         CancellationToken cancelToken = default)
      {
         var result = new List<Blob>();

         string listAfter = "";

         while(options.MaxResults == null || result.Count < options.MaxResults.Value)
         {
            List<DirectoryEntry> page =
               await EnumerateDirectoryAsync(path, _listBatchSize, listAfter, "", userIdFormat, cancelToken).ConfigureAwait(false);

            //no more results
            if(page == null || page.Count == 0)
            {
               break;
            }

            //set pointer to next page
            listAfter = page[page.Count - 1].Name;

            result.AddRange(page.Select(p => ToBlobId(path, p, options.IncludeAttributes)));
         }

         return result;
      }

      internal async Task<List<DirectoryEntry>> EnumerateDirectoryAsync(string path,
         int maxEntries, string listAfter, string listBefore, UserGroupRepresentation userIdFormat = UserGroupRepresentation.ObjectID, CancellationToken cancelToken = default)
      {
         //ADLS requires a root prefix
         path = StoragePath.Normalize(path);

         var resp = new OperationResponse();
         List<DirectoryEntry> page = await Core.ListStatusAsync(path, listAfter, listBefore, maxEntries, userIdFormat, _client,
            new RequestOptions(new ExponentialRetryPolicy(2, 1000)),
            resp).ConfigureAwait(false);
         return page;
         //return new FileStatusOutput(listBefore, listAfter, maxEntries, userIdFormat, _client, path);
      }

      private static Blob ToBlobId(string path, DirectoryEntry entry, bool includeMeta)
      {
         var blob = new Blob(path, entry.Name, entry.Type == DirectoryEntryType.FILE ? BlobItemKind.File : BlobItemKind.Folder);
         blob.Size = entry.Length;
         blob.LastModificationTime = entry.LastModifiedTime;
         return blob;
      }
   }
}
