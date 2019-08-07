using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
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
         var container = new List<Blob>();

         await ListAsync(container, options.FolderPath, options, cancellationToken).ConfigureAwait(false);

         return container;
      }


      private async Task ListAsync(List<Blob> container, string path, ListOptions options, CancellationToken cancellationToken)
      {
         var batch = new List<Blob>();

         if(StoragePath.IsRootPath(path))
         {
            batch.AddRange(await ListFilesystemsAsync().ConfigureAwait(false));
         }
         else
         {
            throw new NotImplementedException();
         }

         container.AddRange(batch);

         if(options.Recurse)
         {
            await Task.WhenAll(batch.Where(b => !b.IsFolder).Select(f => ListAsync(container, f, options, cancellationToken)));
         }
      }

      private async Task<IReadOnlyCollection<Blob>> ListFilesystemsAsync()
      {
         FilesystemList filesystems = await _api.ListFilesystemsAsync().ConfigureAwait(false);

         return filesystems.Filesystems
            .Select(LConvert.ToBlob)
            .ToList();
      }
   }
}
