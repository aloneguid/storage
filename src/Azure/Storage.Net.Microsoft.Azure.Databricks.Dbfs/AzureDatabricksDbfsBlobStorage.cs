using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Azure.Databricks.Client;
using Storage.Net.Blobs;
using FileInfo = Microsoft.Azure.Databricks.Client.FileInfo;

namespace Storage.Net.Microsoft.Azure.Databricks.Dbfs
{
   class AzureDatabricksDbfsBlobStorage : IBlobStorage
   {
      private readonly DatabricksClient _client;
      private readonly IDbfsApi _dbfs;
      private readonly bool _isReadOnly;

      public AzureDatabricksDbfsBlobStorage(string baseUri, string token, bool isReadOnly)
      {
         if(baseUri is null)
            throw new ArgumentNullException(nameof(baseUri));
         if(token is null)
            throw new ArgumentNullException(nameof(token));

         _client = DatabricksClient.CreateClient(baseUri, token);
         _dbfs = _client.Dbfs;
         _isReadOnly = isReadOnly;
      }

      public Task DeleteAsync(IEnumerable<string> fullPaths, CancellationToken cancellationToken = default) => throw new NotImplementedException();

      public Task<IReadOnlyCollection<bool>> ExistsAsync(IEnumerable<string> fullPaths, CancellationToken cancellationToken = default)
      {
         throw new NotImplementedException();
      }

      public Task<IReadOnlyCollection<Blob>> GetBlobsAsync(IEnumerable<string> fullPaths, CancellationToken cancellationToken = default) => throw new NotSupportedException();

      public async Task<IReadOnlyCollection<Blob>> ListAsync(ListOptions options = null, CancellationToken cancellationToken = default)
      {
         if(options == null)
            options = new ListOptions();

         var result = new List<Blob>();

         await ListFolderAsync(options.FolderPath, result, options);

         return result;
      }

      private async Task ListFolderAsync(string path, List<Blob> container, ListOptions options)
      {
         IEnumerable<FileInfo> objects = await _dbfs.List(StoragePath.Normalize(options.FolderPath, true));

         List<Blob> batch = objects.Select(DConvert.ToBlob).ToList();
         container.AddRange(batch);

         if(options.Recurse)
         {
            await Task.WhenAll(batch.Where(b => b.IsFolder).Select(f => ListFolderAsync(f.FullPath, container, options)));
         }
      }

      public async Task<Stream> OpenReadAsync(string fullPath, CancellationToken cancellationToken = default)
      {
         fullPath = StoragePath.Normalize(fullPath, true);

         var ms = new MemoryStream(0);
         await _dbfs.Download(fullPath, ms);
         ms.Position = 0;

         return ms;
      }

      public Task<ITransaction> OpenTransactionAsync() => Task.FromResult(EmptyTransaction.Instance);

      public Task<Stream> OpenWriteAsync(string fullPath, bool append = false, CancellationToken cancellationToken = default)
      {
         CheckReadOnly();

         throw new NotImplementedException();
      }

      public Task SetBlobsAsync(IEnumerable<Blob> blobs, CancellationToken cancellationToken = default) => throw new NotSupportedException();

      public void Dispose()
      {

      }

      private void CheckReadOnly()
      {
         if(_isReadOnly)
         {
            throw new InvalidOperationException("file system is read-only");
         }
      }
   }
}
