using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Refit;
using Storage.Net.Blobs;
using Storage.Net.Microsoft.Azure.DataLake.Store.Gen2.Rest;
using Storage.Net.Microsoft.Azure.DataLake.Store.Gen2.Rest.Model;

namespace Storage.Net.Microsoft.Azure.DataLake.Store.Gen2
{
   class DirectoryBrowser
   {
      private readonly IDataLakeApi _api;

      public DirectoryBrowser(IDataLakeApi api)
      {
         _api = api;
      }

      public async Task<IReadOnlyCollection<Blob>> ListAsync(ListOptions options, CancellationToken cancellationToken)
      {
         if(StoragePath.IsRootPath(options.FolderPath))
         {
            //only filesystems are in the root path
            var result = new List<Blob>(await ListFilesystemsAsync(options, cancellationToken).ConfigureAwait(false));

            if(options.Recurse)
            {
               foreach(Blob folder in result.Where(b => b.IsFolder).ToList())
               {
                  int? maxResults = options.MaxResults == null
                     ? null
                     : (int?)(options.MaxResults.Value - result.Count);

                  result.AddRange(await ListPathAsync(folder, maxResults, options, cancellationToken).ConfigureAwait(false));
               }
            }

            return result;
         }
         else
         {
            return await ListPathAsync(options.FolderPath, options.MaxResults, options, cancellationToken).ConfigureAwait(false);
         }
      }

      private async Task<IReadOnlyCollection<Blob>> ListFilesystemsAsync(ListOptions options, CancellationToken cancellationToken)
      {
         //todo: paging

         FilesystemList filesystems = await _api.ListFilesystemsAsync().ConfigureAwait(false);

         IEnumerable<Blob> result = filesystems.Filesystems
            .Select(LConvert.ToBlob);

         if(options.BrowseFilter != null)
            result = result.Where(fs => options.BrowseFilter == null || options.BrowseFilter(fs));

         if(options.MaxResults != null)
            result = result.Take(options.MaxResults.Value);

         return result.ToList();
      }

      private async Task<IReadOnlyCollection<Blob>> ListPathAsync(string path, int? maxResults, ListOptions options, CancellationToken cancellationToken)
      {
         //get filesystem name and folder path
         string[] parts = StoragePath.Split(path);

         string fs = parts[0];
         string relativePath = StoragePath.Combine(parts.Skip(1));

         PathList list;

         try
         {
            list = await _api.ListPathAsync(fs, relativePath, recursive: options.Recurse).ConfigureAwait(false);
         }
         catch(ApiException ex) when (ex.StatusCode == HttpStatusCode.NotFound)
         {
            // specified path is not found, nothing serious
            return new List<Blob>();
         }

         IEnumerable<Blob> result = list.Paths.Select(p => LConvert.ToBlob(fs, p));

         if(options.FilePrefix != null)
            result = result.Where(b => b.IsFolder || b.Name.StartsWith(options.FilePrefix));

         if(options.BrowseFilter != null)
            result = result.Where(b => options.BrowseFilter(b));

         if(maxResults != null)
            result = result.Take(maxResults.Value);

         return result.ToList();
      }
   }
}
