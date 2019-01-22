using System;
using System.Collections.Generic;
using System.IO;
using System.Net.Http;
using System.Threading;
using System.Threading.Tasks;
using Storage.Net.Blob;

namespace Storage.Net.Microsoft.Azure.Storage.Blob.Lite
{
   class AzureBlobStorageThinClient : IBlobStorage
   {
      private readonly string _accountName;
      private readonly string _key;
      private readonly HttpClient _httpClient;
      private readonly string _baseUrl;

      //see authentication at https://docs.microsoft.com/en-us/rest/api/storageservices/authorize-with-shared-key

      public AzureBlobStorageThinClient(string accountName, string key)
      {
         _accountName = accountName ?? throw new ArgumentNullException(nameof(accountName));
         _key = key ?? throw new ArgumentNullException(nameof(key));
         _httpClient = new HttpClient();
         _httpClient.BaseAddress = new Uri($"https://{accountName}.blob.core.windows.net/");
      }

      public Task DeleteAsync(IEnumerable<string> ids, CancellationToken cancellationToken = default)
      {
         throw new NotImplementedException();
      }

      public void Dispose()
      {
         throw new NotImplementedException();
      }

      public Task<IReadOnlyCollection<bool>> ExistsAsync(IEnumerable<string> ids, CancellationToken cancellationToken = default)
      {
         throw new NotImplementedException();
      }

      public Task<IEnumerable<BlobMeta>> GetMetaAsync(IEnumerable<string> ids, CancellationToken cancellationToken = default)
      {
         throw new NotImplementedException();
      }

      public Task<IReadOnlyCollection<BlobId>> ListAsync(ListOptions options = null, CancellationToken cancellationToken = default)
      {
         throw new NotImplementedException();
      }

      public Task<Stream> OpenReadAsync(string id, CancellationToken cancellationToken = default)
      {
         throw new NotImplementedException();
      }

      public Task<ITransaction> OpenTransactionAsync()
      {
         throw new NotImplementedException();
      }

      public Task<Stream> OpenWriteAsync(string id, bool append = false, CancellationToken cancellationToken = default)
      {
         throw new NotImplementedException();
      }

      public Task WriteAsync(string id, Stream sourceStream, bool append = false, CancellationToken cancellationToken = default)
      {
         throw new NotImplementedException();
      }

      /*private async Task ExecuteAsync(string uri, HttpMethod method, CancellationToken cancellationToken)
      {
         var message = new HttpRequestMessage(method, uri, )

         _httpClient.SendAsync()
      }*/

   }
}