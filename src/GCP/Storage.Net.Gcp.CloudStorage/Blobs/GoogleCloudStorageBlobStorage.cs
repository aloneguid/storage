using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Google.Api.Gax;
using Google.Apis.Auth.OAuth2;
using Google.Apis.Storage.v1.Data;
using Google.Cloud.Storage.V1;
using Storage.Net.Blobs;
using Objects = Google.Apis.Storage.v1.Data.Objects;
using Object = Google.Apis.Storage.v1.Data.Object;
using Storage.Net.Streaming;
using Google;
using System.Net;
using System.Linq;

namespace Storage.Net.Gcp.CloudStorage.Blobs
{
   class GoogleCloudStorageBlobStorage : IBlobStorage
   {
      //for intro see https://cloud.google.com/storage/docs/reference/libraries#client-libraries-install-csharp

      private readonly StorageClient _client;
      private readonly string _bucketName;

      public GoogleCloudStorageBlobStorage(string bucketName, GoogleCredential credential = null, EncryptionKey encryptionKey = null)
      {
         _client = StorageClient.Create(credential, encryptionKey);
         this._bucketName = bucketName;
      }

      public async Task<IReadOnlyCollection<Blob>> ListAsync(ListOptions options = null, CancellationToken cancellationToken = default)
      {
         if(options == null)
            options = new ListOptions();

         PagedAsyncEnumerable<Objects, Object> objects = _client.ListObjectsAsync(
            _bucketName,
            StoragePath.IsRootPath(options.FolderPath) ? null : options.FolderPath,
            new ListObjectsOptions
            {
               Delimiter = options.Recurse ? null : "/"
            });

         return await GConvert.ToBlobsAsync(objects, options);
      }

      public async Task DeleteAsync(IEnumerable<string> fullPaths, CancellationToken cancellationToken = default)
      {
         GenericValidation.CheckBlobFullPaths(fullPaths);

         await Task.WhenAll(fullPaths.Select(fp => DeleteAsync(fp, cancellationToken)));
      }

      private async Task DeleteAsync(string fullPath, CancellationToken cancellationToken)
      {
         try
         {
            await _client.DeleteObjectAsync(_bucketName, StoragePath.Normalize(fullPath), cancellationToken: cancellationToken).ConfigureAwait(false);
         }
         catch(GoogleApiException ex) when (ex.HttpStatusCode == HttpStatusCode.NotFound)
         {
            //when not found, just ignore

            //todo: this may be a folder though
         }
      }

      public async Task<IReadOnlyCollection<bool>> ExistsAsync(IEnumerable<string> fullPaths, CancellationToken cancellationToken = default)
      {
         GenericValidation.CheckBlobFullPaths(fullPaths);

         return await Task.WhenAll(fullPaths.Select(fp => ExistsAsync(fp, cancellationToken))).ConfigureAwait(false);
      }

      private async Task<bool> ExistsAsync(string fullPath, CancellationToken cancellationToken)
      {
         try
         {
            await _client.GetObjectAsync(
               _bucketName, StoragePath.Normalize(fullPath),
               null,
               cancellationToken).ConfigureAwait(false);

            return true;
         }
         catch(GoogleApiException ex) when(ex.HttpStatusCode == HttpStatusCode.NotFound)
         {
            return false;
         }
      }

      public Task<IReadOnlyCollection<Blob>> GetBlobsAsync(IEnumerable<string> fullPaths, CancellationToken cancellationToken = default) => throw new NotImplementedException();
      

      public Task<ITransaction> OpenTransactionAsync() => EmptyTransaction.TaskInstance;

      public Task<Stream> OpenWriteAsync(string fullPath, bool append = false, CancellationToken cancellationToken = default)
      {
         if(append)
            throw new NotSupportedException();
         GenericValidation.CheckBlobFullPath(fullPath);
         fullPath = StoragePath.Normalize(fullPath, false);

         // no write streaming support in this crappy SDK

         return Task.FromResult<Stream>(new FixedStream(new MemoryStream(), null, async (fx) =>
         {
            var ms = (MemoryStream)fx.Parent;
            ms.Position = 0;

            await _client.UploadObjectAsync(
               _bucketName, fullPath, null, ms,
               cancellationToken: cancellationToken).ConfigureAwait(false);
         }));
      }

      public async Task<Stream> OpenReadAsync(string fullPath, CancellationToken cancellationToken = default)
      {
         GenericValidation.CheckBlobFullPath(fullPath);
         fullPath = StoragePath.Normalize(fullPath, false);

         // no read streaming support in this crappy SDK

         var ms = new MemoryStream();
         try
         {
            await _client.DownloadObjectAsync(_bucketName, fullPath, ms, cancellationToken: cancellationToken).ConfigureAwait(false);
         }
         catch(GoogleApiException ex) when(ex.HttpStatusCode == HttpStatusCode.NotFound)
         {
            return null;
         }
         ms.Position = 0;
         return ms;
      }


      public Task SetBlobsAsync(IEnumerable<Blob> blobs, CancellationToken cancellationToken = default) => throw new NotImplementedException();

      public void Dispose()
      {

      }
   }
}
