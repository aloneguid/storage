using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net;
using System.Threading;
using System.Threading.Tasks;
using Storage.Net.Blobs;
using Storage.Net.Microsoft.Azure.DataLake.Store.Gen2.Rest;
using Storage.Net.Microsoft.Azure.DataLake.Store.Gen2.Rest.Model;
using Refit;

namespace Storage.Net.Microsoft.Azure.DataLake.Store.Gen2
{
   class AzureDataLakeStoreGen2BlobStorageProvider : IBlobStorage
   {
      private readonly IDataLakeApi _restApi;

      private AzureDataLakeStoreGen2BlobStorageProvider(IDataLakeApi restApi)
      {
         _restApi = restApi;
      }

      public int ListBatchSize { get; set; } = 5000;

      public static AzureDataLakeStoreGen2BlobStorageProvider CreateBySharedAccessKey(string accountName,
         string sharedAccessKey)
      {
         if(accountName == null)
            throw new ArgumentNullException(nameof(accountName));

         if(sharedAccessKey == null)
            throw new ArgumentNullException(nameof(sharedAccessKey));

         return new AzureDataLakeStoreGen2BlobStorageProvider(
            DataLakeApiFactory.CreateApiWithSharedKey(accountName, sharedAccessKey));
      }

      public static AzureDataLakeStoreGen2BlobStorageProvider CreateByClientSecret(
         string accountName,
         string tenantId,
         string clientId,
         string clientSecret)
      {
         if(accountName is null)
            throw new ArgumentNullException(nameof(accountName));
         if(tenantId is null)
            throw new ArgumentNullException(nameof(tenantId));
         if(clientId is null)
            throw new ArgumentNullException(nameof(clientId));
         if(clientSecret is null)
            throw new ArgumentNullException(nameof(clientSecret));

         return new AzureDataLakeStoreGen2BlobStorageProvider(
            DataLakeApiFactory.CreateApiWithServicePrincipal(accountName, tenantId, clientId, clientSecret));
      }

      public static AzureDataLakeStoreGen2BlobStorageProvider CreateByManagedIdentity(string accountName)
      {
         if(accountName == null)
            throw new ArgumentNullException(nameof(accountName));

         return new AzureDataLakeStoreGen2BlobStorageProvider(
            DataLakeApiFactory.CreateApiWithManagedIdentity(accountName));
      }

      public async Task DeleteAsync(IEnumerable<string> fullPaths, CancellationToken cancellationToken = default)
      {
         var fullPathsList = fullPaths.ToList();
         GenericValidation.CheckBlobFullPaths(fullPathsList);

         await Task.WhenAll(fullPathsList.Select(path => DeleteAsync(path, cancellationToken)));
      }

      private async Task DeleteAsync(string fullPath, CancellationToken cancellationToken)
      {
         DecomposePath(fullPath, out string fs, out string rp);

         await _restApi.DeletePathAsync(fs, rp, true).ConfigureAwait(false);
      }

      public async Task<IReadOnlyCollection<Blob>> ListAsync(ListOptions options = null, CancellationToken cancellationToken = default)
      {
         if(options == null)
            options = new ListOptions();

         return await new DirectoryBrowser(_restApi).ListAsync(options, cancellationToken).ConfigureAwait(false);
      }

      public async Task<Stream> OpenReadAsync(string fullPath, CancellationToken cancellationToken = default)
      {
         GenericValidation.CheckBlobFullPath(fullPath);
         DecomposePath(fullPath, out string fs, out string rp);

         PathProperties pp;

         try
         {
            pp = await GetPathPropertiesAsync(fs, rp, "getStatus").ConfigureAwait(false);
         }
         catch(ApiException ex) when(ex.StatusCode == HttpStatusCode.NotFound)
         {
            //the file is not found, return null
            return null;
         }

         return new BufferedStream(new ReadStream(_restApi, (long)pp.Length, fs, rp), 4096);
      }

      public Task<Stream> OpenWriteAsync(string fullPath, bool append = false, CancellationToken cancellationToken = default)
      {
         DecomposePath(fullPath, out string filesystemName, out string relativePath);

         //FlushingStream already handles missing filesystem and attempts to create it on error
         return Task.FromResult<Stream>(new WriteStream(_restApi, filesystemName, relativePath));
      }

      private void DecomposePath(string path, out string filesystemName, out string relativePath)
      {
         GenericValidation.CheckBlobFullPath(path);
         string[] parts = StoragePath.Split(path);

         if(parts.Length == 1)
         {
            throw new ArgumentException($"path {path} must include filesystem name as root folder", nameof(path));
         }

         filesystemName = parts[0];

         relativePath = StoragePath.Combine(parts.Skip(1));
      }

      public async Task<IReadOnlyCollection<bool>> ExistsAsync(IEnumerable<string> fullPaths,
         CancellationToken cancellationToken = default)
      {
         GenericValidation.CheckBlobFullPaths(fullPaths);

         return await Task.WhenAll(fullPaths.Select(path => ExistsAsync(path, cancellationToken)));
      }

      private async Task<bool> ExistsAsync(string fullPath, CancellationToken cancellationToken)
      {
         DecomposePath(fullPath, out string fs, out string rp);

         try
         {
            await GetPathPropertiesAsync(fs, rp, "getStatus").ConfigureAwait(false);
         }
         catch(ApiException ex) when(ex.StatusCode == HttpStatusCode.NotFound)
         {
            return false;
         }

         return true;
      }

      public async Task<IReadOnlyCollection<Blob>> GetBlobsAsync(IEnumerable<string> fullPaths,
         CancellationToken cancellationToken = default)
      {
         return await Task.WhenAll(fullPaths.Select(path => GetBlobAsync(path, cancellationToken))).ConfigureAwait(false);
      }

      private async Task<Blob> GetBlobAsync(string fullPath, CancellationToken cancellationToken)
      {
         DecomposePath(fullPath, out string fs, out string rp);
         PathProperties pp;

         try
         {
            pp = await GetPathPropertiesAsync(fs, rp, "getStatus").ConfigureAwait(false);
         }
         catch(ApiException ex) when(ex.StatusCode == HttpStatusCode.NotFound)
         {
            return null;
         }

         return LConvert.ToBlob(fullPath, pp);
      }

      public Task SetBlobsAsync(IEnumerable<Blob> blobs, CancellationToken cancellationToken = default)
      {
         throw new NotSupportedException("ADLS Gen2 doesn't support file metadata");
      }

      public void Dispose()
      {
      }

      public Task<ITransaction> OpenTransactionAsync()
      {
         return Task.FromResult(EmptyTransaction.Instance);
      }


      private async Task<PathProperties> GetPathPropertiesAsync(
         string filesystem,
         string path,
         string action = null,
         string upn = null,
         int? timeoutSeconds = null)
      {
         ApiResponse<string> response = await _restApi.GetPathPropertiesAsync(filesystem, path, action, upn, timeoutSeconds);
         await response.EnsureSuccessStatusCodeAsync();
         return new PathProperties(response);
      }
   }
}