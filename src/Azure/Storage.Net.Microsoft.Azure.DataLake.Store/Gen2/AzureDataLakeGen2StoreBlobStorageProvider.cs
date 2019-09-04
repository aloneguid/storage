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
using NetBox.Extensions;
using Storage.Net.Microsoft.Azure.DataLake.Store.Gen2.Model;

namespace Storage.Net.Microsoft.Azure.DataLake.Store.Gen2
{
   class AzureDataLakeStoreGen2BlobStorageProvider : IAzureDataLakeGen2BlobStorage
   {
      private readonly IDataLakeApi _restApi;

      private AzureDataLakeStoreGen2BlobStorageProvider(IDataLakeApi restApi)
      {
         _restApi = restApi;
      }

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

         var provider = new AzureDataLakeStoreGen2BlobStorageProvider(
            DataLakeApiFactory.CreateApiWithServicePrincipal(accountName, tenantId, clientId, clientSecret));
         //provider.Graph = new GraphService(tenantId, clientId, clientSecret);

         return provider;
      }

      public static AzureDataLakeStoreGen2BlobStorageProvider CreateByManagedIdentity(string accountName)
      {
         if(accountName == null)
            throw new ArgumentNullException(nameof(accountName));

         return new AzureDataLakeStoreGen2BlobStorageProvider(
            DataLakeApiFactory.CreateApiWithManagedIdentity(accountName));
      }

      private Stream EmptyStream => new MemoryStream(new byte[0]);

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

         IReadOnlyCollection<Blob> result = await new DirectoryBrowser(_restApi).ListAsync(options, cancellationToken).ConfigureAwait(false);

         if(options.IncludeAttributes)
         {
            result = await Task.WhenAll(result.Select(b => GetWithMetadata(b, cancellationToken)));
         }

         return result;
      }

      private async Task<Blob> GetWithMetadata(Blob blob, CancellationToken cancellationToken)
      {
         if(blob.IsFile)
         {
            return await GetBlobAsync(blob, cancellationToken).ConfigureAwait(false);
         }

         return blob;
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

         return new BufferedStream(new ReadStream(_restApi, (long)pp.Length, fs, rp));
      }

      public Task<Stream> OpenWriteAsync(string fullPath, bool append = false, CancellationToken cancellationToken = default)
      {
         DecomposePath(fullPath, out string filesystemName, out string relativePath);

         //FlushingStream already handles missing filesystem and attempts to create it on error
         return Task.FromResult<Stream>(new BufferedStream(new WriteStream(_restApi, filesystemName, relativePath)));
      }

      private void DecomposePath(string path, out string filesystemName, out string relativePath, bool requireRelativePath = true)
      {
         GenericValidation.CheckBlobFullPath(path);
         string[] parts = StoragePath.Split(path);

         if(requireRelativePath && parts.Length < 2)
         {
            throw new ArgumentException($"path '{path}' must include filesystem name as root folder, i.e. 'filesystem/path'", nameof(path));
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
         DecomposePath(fullPath, out string fs, out string rp, false);

         if(StoragePath.IsRootPath(rp))
         {
            try
            {
               ApiResponse<string> response = await _restApi.GetFilesystemProperties(fs);
               await response.EnsureSuccessStatusCodeAsync();
               var fsProps = new PathProperties(response);
               return LConvert.ToBlob(fs, fsProps);
            }
            catch(ApiException ex) when (ex.StatusCode == HttpStatusCode.NotFound)
            {
               return null;
            }
         }

         PathProperties pp;

         try
         {
            pp = await GetPathPropertiesAsync(fs, rp, "getProperties").ConfigureAwait(false);
         }
         catch(ApiException ex) when(ex.StatusCode == HttpStatusCode.NotFound)
         {
            return null;
         }

         return LConvert.ToBlob(fullPath, pp);
      }

      public Task SetBlobsAsync(IEnumerable<Blob> blobs, CancellationToken cancellationToken = default)
      {
         return Task.WhenAll(blobs.Select(b => SetBlobAsync(b, cancellationToken)));
      }

      private async Task SetBlobAsync(Blob blob, CancellationToken cancellationToken)
      {
         GenericValidation.CheckBlobFullPath(blob);
         DecomposePath(blob, out string fs, out string rp);

         string properties = string.Join(",", blob.Metadata.Select(kv => $"{kv.Key}={kv.Value.Base64Encode()}"));

         await _restApi.UpdatePathAsync(fs, rp, "setProperties",
            properties: properties,
            body: EmptyStream);
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
         bool? upn = null,
         int? timeoutSeconds = null)
      {
         ApiResponse<string> response = await _restApi.GetPathPropertiesAsync(filesystem, path, action, upn, timeoutSeconds);
         await response.EnsureSuccessStatusCodeAsync();
         return new PathProperties(response);
      }

      #region [ ADLS Specific ]

      public async Task SetAccessControlAsync(string fullPath, AccessControl accessControl)
      {
         if(accessControl is null)
            throw new ArgumentNullException(nameof(accessControl));

         GenericValidation.CheckBlobFullPath(fullPath);
         DecomposePath(fullPath, out string fs, out string rp);

         await _restApi.UpdatePathAsync(fs, rp, "setAccessControl",
            body: EmptyStream,
            acl: accessControl.ToString());
      }

      public async Task<AccessControl> GetAccessControlAsync(string fullPath, bool getUpn)
      {
         GenericValidation.CheckBlobFullPath(fullPath);
         DecomposePath(fullPath, out string fs, out string rp);

         ApiResponse<string> response = await _restApi.GetPathPropertiesAsync(fs, rp, "getAccessControl", upn: getUpn).ConfigureAwait(false);
         await response.EnsureSuccessStatusCodeAsync().ConfigureAwait(false);

         return new AccessControl(
            response.GetHeader("x-ms-owner"),
            response.GetHeader("x-ms-group"),
            response.GetHeader("x-ms-permissions"),
            response.GetHeader("x-ms-acl"));
      }

      public Task CreateFilesystemAsync(string filesystem)
      {
         return _restApi.CreateFilesystemAsync(filesystem);
      }

      public Task DeleteFilesystemAsync(string filesystem)
      {
         return _restApi.DeleteFilesystemAsync(filesystem);
      }

      public async Task<IEnumerable<string>> ListFilesystemsAsync()
      {
         FilesystemList list = await _restApi.ListFilesystemsAsync().ConfigureAwait(false);
         return list.Filesystems.Select(x => x.Name);
      }

      #endregion
   }
}