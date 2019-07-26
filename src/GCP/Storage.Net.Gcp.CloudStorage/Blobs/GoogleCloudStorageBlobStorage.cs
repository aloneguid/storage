using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Google.Apis.Auth.OAuth2;
using Google.Apis.Storage.v1.Data;
using Google.Cloud.Storage.V1;
using Storage.Net.Blobs;

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
         Bucket bucket = await _client.GetBucketAsync(_bucketName);

         return null;
      }

      public Task DeleteAsync(IEnumerable<string> fullPaths, CancellationToken cancellationToken = default) => throw new NotImplementedException();
      public void Dispose()
      {

      }

      public Task<IReadOnlyCollection<bool>> ExistsAsync(IEnumerable<string> fullPaths, CancellationToken cancellationToken = default) => throw new NotImplementedException();
      public Task<IReadOnlyCollection<Blob>> GetBlobsAsync(IEnumerable<string> fullPaths, CancellationToken cancellationToken = default) => throw new NotImplementedException();
      
      public Task<Stream> OpenReadAsync(string fullPath, CancellationToken cancellationToken = default) => throw new NotImplementedException();
      public Task<ITransaction> OpenTransactionAsync() => throw new NotImplementedException();
      public Task<Stream> OpenWriteAsync(string fullPath, bool append = false, CancellationToken cancellationToken = default) => throw new NotImplementedException();
      public Task SetBlobsAsync(IEnumerable<Blob> blobs, CancellationToken cancellationToken = default) => throw new NotImplementedException();
   }
}
