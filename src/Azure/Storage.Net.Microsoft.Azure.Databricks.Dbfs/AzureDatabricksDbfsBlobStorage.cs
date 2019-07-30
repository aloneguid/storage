﻿using System;
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

      public AzureDatabricksDbfsBlobStorage(string baseUri, string token)
      {
         if(baseUri is null)
            throw new ArgumentNullException(nameof(baseUri));
         if(token is null)
            throw new ArgumentNullException(nameof(token));

         _client = DatabricksClient.CreateClient(baseUri, token);
         _dbfs = _client.Dbfs;
      }

      public Task DeleteAsync(IEnumerable<string> fullPaths, CancellationToken cancellationToken = default) => throw new NotImplementedException();

      public void Dispose()
      {

      }
      public Task<IReadOnlyCollection<bool>> ExistsAsync(IEnumerable<string> fullPaths, CancellationToken cancellationToken = default) => throw new NotImplementedException();
      public Task<IReadOnlyCollection<Blob>> GetBlobsAsync(IEnumerable<string> fullPaths, CancellationToken cancellationToken = default) => throw new NotImplementedException();

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

      public Task<Stream> OpenReadAsync(string fullPath, CancellationToken cancellationToken = default) => throw new NotImplementedException();
      public Task<ITransaction> OpenTransactionAsync() => throw new NotImplementedException();
      public Task<Stream> OpenWriteAsync(string fullPath, bool append = false, CancellationToken cancellationToken = default) => throw new NotImplementedException();
      public Task SetBlobsAsync(IEnumerable<Blob> blobs, CancellationToken cancellationToken = default) => throw new NotImplementedException();
   }
}
