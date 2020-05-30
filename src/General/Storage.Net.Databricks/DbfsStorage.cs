﻿using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Azure.Databricks.Client;
using Storage.Net.Blobs;
using FileInfo = Microsoft.Azure.Databricks.Client.FileInfo;

namespace Storage.Net.Databricks
{
   class DbfsStorage : IBlobStorage
   {
      private readonly IDbfsApi _dbfs;

      public DbfsStorage(IDbfsApi dbfsApi)
      {
         _dbfs = dbfsApi;
      }

      public async Task DeleteAsync(IEnumerable<string> fullPaths, CancellationToken cancellationToken = default)
      {
         await Task.WhenAll(fullPaths.Select(fp => DeleteAsync(fp)));
      }

      private async Task DeleteAsync(string fullPath)
      {
         fullPath = StoragePath.Normalize(fullPath);

         await _dbfs.Delete(fullPath, true);
      }

      public async Task<IReadOnlyCollection<bool>> ExistsAsync(IEnumerable<string> fullPaths, CancellationToken cancellationToken = default)
      {
         return await Task.WhenAll(fullPaths.Select(fp => ExistsAsync(fp)));
      }

      private async Task<bool> ExistsAsync(string fullPath)
      {
         fullPath = StoragePath.Normalize(fullPath);

         try
         {
            await _dbfs.GetStatus(fullPath);
         }
         catch(ClientApiException ex) when(ex.StatusCode == HttpStatusCode.NotFound)
         {
            return false;
         }

         return true;
      }

      public async Task<IReadOnlyCollection<Blob>> GetBlobsAsync(IEnumerable<string> fullPaths, CancellationToken cancellationToken = default)
      {
         return await Task.WhenAll(fullPaths.Select(fp => GetBlobAsync(fp)));
      }

      private async Task<Blob> GetBlobAsync(string fullPath)
      {
         fullPath = StoragePath.Normalize(fullPath);
         FileInfo status;
         try
         {
            status = await _dbfs.GetStatus(fullPath);
         }
         catch(ClientApiException ex) when(ex.StatusCode == HttpStatusCode.NotFound)
         {
            return null;
         }

         return new Blob(fullPath, status.IsDirectory ? BlobItemKind.Folder : BlobItemKind.File)
         {
            Size = status.FileSize
         };
      }

      public async Task<IReadOnlyCollection<Blob>> ListAsync(ListOptions options = null, CancellationToken cancellationToken = default)
      {
         if(options == null)
            options = new ListOptions();

         var result = new List<Blob>();

         await ListFolderAsync(options.FolderPath, result, options);

         if(options.MaxResults != null)
         {
            result = result.Take(options.MaxResults.Value).ToList();
         }

         return result;
      }

      private async Task ListFolderAsync(string path, List<Blob> container, ListOptions options)
      {
         IEnumerable<FileInfo> objects;

         try
         {
            objects = await _dbfs.List(StoragePath.Normalize(path));
         }
         catch(ClientApiException ex) when(ex.StatusCode == HttpStatusCode.NotFound)
         {
            objects = new List<FileInfo>();
         }


         List<Blob> batch = objects
            .Select(DConvert.ToBlob)
            .Where(options.IsMatch)
            .Where(b => options.BrowseFilter == null || options.BrowseFilter(b))
            .ToList();

         container.AddRange(batch);

         if(options.Recurse)
         {
            await Task.WhenAll(batch.Where(b => b.IsFolder).Select(f => ListFolderAsync(f.FullPath, container, options)));
         }
      }

      public async Task<Stream> OpenReadAsync(string fullPath, CancellationToken cancellationToken = default)
      {
         fullPath = StoragePath.Normalize(fullPath);

         var ms = new MemoryStream(0);
         try
         {
            await _dbfs.Download(fullPath, ms);
         }
         catch(ClientApiException ex) when(ex.StatusCode == HttpStatusCode.NotFound || ex.StatusCode == HttpStatusCode.BadRequest)
         {
            return null;
         }
         ms.Position = 0;

         return ms;
      }

      public Task<ITransaction> OpenTransactionAsync() => Task.FromResult(EmptyTransaction.Instance);

      public async Task WriteAsync(string fullPath, Stream dataStream, bool append = false, CancellationToken cancellationToken = default)
      {
         if(dataStream is null)
            throw new ArgumentNullException(nameof(dataStream));

         fullPath = StoragePath.Normalize(fullPath);

         if(append)
            throw new ArgumentOutOfRangeException(nameof(append), "append mode is not supported");

         await _dbfs.Upload(fullPath, true, dataStream);
      }

      public Task SetBlobsAsync(IEnumerable<Blob> blobs, CancellationToken cancellationToken = default) => throw new NotSupportedException();

      public Task MoveBlobAsync(string fromPath, string toPath, CancellationToken cancellationToken = default)
      {
         throw new NotImplementedException();
      }

      public void Dispose()
      {

      }
   }
}
