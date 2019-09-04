using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Azure.Storage;
using Microsoft.Azure.Storage.Auth;
using Microsoft.Azure.Storage.File;
using Storage.Net.Blobs;

namespace Storage.Net.Microsoft.Azure.Storage.Blobs
{
   class AzureFilesBlobStorage : IBlobStorage
   {
      private readonly CloudFileClient _client;

      public AzureFilesBlobStorage(CloudFileClient client)
      {
         _client = client ?? throw new ArgumentNullException(nameof(client));
      }


      public static AzureFilesBlobStorage CreateFromAccountNameAndKey(string accountName, string key)
      {
         if(accountName == null)
            throw new ArgumentNullException(nameof(accountName));

         if(key == null)
            throw new ArgumentNullException(nameof(key));

         var account = new CloudStorageAccount(
            new StorageCredentials(accountName, key),
            true);

         return new AzureFilesBlobStorage(account.CreateCloudFileClient());
      }


      public Task DeleteAsync(IEnumerable<string> fullPaths, CancellationToken cancellationToken = default) => throw new NotImplementedException();
      public void Dispose()
      {

      }

      public Task<IReadOnlyCollection<bool>> ExistsAsync(IEnumerable<string> fullPaths, CancellationToken cancellationToken = default) => throw new NotImplementedException();
      public Task<IReadOnlyCollection<Blob>> GetBlobsAsync(IEnumerable<string> fullPaths, CancellationToken cancellationToken = default) => throw new NotImplementedException();
      public Task<IReadOnlyCollection<Blob>> ListAsync(ListOptions options = null, CancellationToken cancellationToken = default) => throw new NotImplementedException();
      public Task<Stream> OpenReadAsync(string fullPath, CancellationToken cancellationToken = default) => throw new NotImplementedException();
      public Task<ITransaction> OpenTransactionAsync() => throw new NotImplementedException();
      public Task<Stream> OpenWriteAsync(string fullPath, bool append = false, CancellationToken cancellationToken = default) => throw new NotImplementedException();
      public Task SetBlobsAsync(IEnumerable<Blob> blobs, CancellationToken cancellationToken = default) => throw new NotImplementedException();
   }
}
