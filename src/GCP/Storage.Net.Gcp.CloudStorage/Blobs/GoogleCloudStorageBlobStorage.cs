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
         PagedAsyncEnumerable<Objects, Object> objects = _client.ListObjectsAsync(_bucketName);

         return await GConvert.ToBlobsAsync(objects);
      }

      public Task DeleteAsync(IEnumerable<string> fullPaths, CancellationToken cancellationToken = default) => throw new NotImplementedException();
      

      public Task<IReadOnlyCollection<bool>> ExistsAsync(IEnumerable<string> fullPaths, CancellationToken cancellationToken = default) => throw new NotImplementedException();
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
         await _client.DownloadObjectAsync(_bucketName, fullPath, ms, cancellationToken: cancellationToken);
         ms.Position = 0;
         return ms;
      }


      public Task SetBlobsAsync(IEnumerable<Blob> blobs, CancellationToken cancellationToken = default) => throw new NotImplementedException();

      public void Dispose()
      {

      }
   }
}
