using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net;
using System.Threading;
using System.Threading.Tasks;
using Storage.Net.Blobs;
using Storage.Net.Microsoft.Azure.DataLakeGen2.Store.Gen2.BLL;
using Storage.Net.Microsoft.Azure.DataLake.Store.Gen2.Models;
using Storage.Net.Microsoft.Azure.DataLake.Store.Gen2.Rest;
using Storage.Net.Microsoft.Azure.DataLake.Store.Gen2.Rest.Model;
using Refit;

namespace Storage.Net.Microsoft.Azure.DataLake.Store.Gen2
{
   class AzureDataLakeStoreGen2BlobStorageProvider : IBlobStorage
   {
      private readonly DataLakeGen2Client _legacyClient;
      private readonly IDataLakeApi _restApi;

      private AzureDataLakeStoreGen2BlobStorageProvider(DataLakeGen2Client client, IDataLakeApi restApi)
      {
         _legacyClient = client ?? throw new ArgumentNullException(nameof(client));
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
            DataLakeGen2Client.Create(accountName, sharedAccessKey),
            DataLakeApiFactory.CreateApi(accountName, sharedAccessKey));
      }

      public static AzureDataLakeStoreGen2BlobStorageProvider CreateByClientSecret(string accountName,
         NetworkCredential credential)
      {
         if(credential == null)
            throw new ArgumentNullException(nameof(credential));

         if(string.IsNullOrEmpty(credential.Domain))
            throw new ArgumentException("Tenant ID (Domain in NetworkCredential) part is required");

         if(string.IsNullOrEmpty(credential.UserName))
            throw new ArgumentException("Principal ID (Username in NetworkCredential) part is required");

         if(string.IsNullOrEmpty(credential.Password))
            throw new ArgumentException("Principal Secret (Password in NetworkCredential) part is required");

         return new AzureDataLakeStoreGen2BlobStorageProvider(DataLakeGen2Client.Create(accountName, credential.Domain,
            credential.UserName, credential.Password), null);
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
         var info = new PathInformation(fullPath);

         return await TryGetPropertiesAsync(info.Filesystem, info.Path, cancellationToken) == null
            ? null
            : _legacyClient.OpenRead(info.Filesystem, info.Path);
      }

      public Task<Stream> OpenWriteAsync(string fullPath, bool append = false, CancellationToken cancellationToken = default)
      {
         DecomposePath(fullPath, out string filesystemName, out string relativePath);

         //FlushingStream already handles missing filesystem and attempts to create it on error
         return Task.FromResult<Stream>(new FlushingStream(_restApi, filesystemName, relativePath));
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
            await _restApi.GetPathPropertiesAsync(fs, rp, "getStatus").ConfigureAwait(false);
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
         var fullPathsList = fullPaths.ToList();
         GenericValidation.CheckBlobFullPaths(fullPathsList);

         return await Task.WhenAll(fullPathsList.Select(async x =>
         {
            var info = new PathInformation(x);
            Properties properties = await TryGetPropertiesAsync(info.Filesystem, info.Path, cancellationToken);
            return properties == null ? null : new Blob(x, properties.IsDirectory ? BlobItemKind.Folder : BlobItemKind.File)
            {
               LastModificationTime = properties.LastModified,
               Size = properties.Length
            };
         }));
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

      private async Task<Properties> TryGetPropertiesAsync(string filesystem, string path, CancellationToken cancellationToken)
      {
         try
         {
            return await _legacyClient.GetPropertiesAsync(filesystem, path, cancellationToken);
         }
         catch(DataLakeGen2Exception e) when (e.StatusCode == HttpStatusCode.NotFound)
         {
            return null;
         }
      }

      private class PathInformation
      {
         public PathInformation(string id)
         {
            string[] split = id.Split('/');

            if(split.Length < 1)
            {
               throw new ArgumentException("id must contain a filesystem.");
            }

            Filesystem = split.First();
            Path = string.Join("/", split.Skip(1));
         }

         public string Filesystem { get; }
         public string Path { get; }
      }
   }
}