using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net;
using System.Threading;
using System.Threading.Tasks;
using Storage.Net.Blob;
using Storage.Net.Microsoft.Azure.DataLakeGen2.Store.Blob.BLL;
using Storage.Net.Microsoft.Azure.DataLakeGen2.Store.Blob.Interfaces;
using Storage.Net.Microsoft.Azure.DataLakeGen2.Store.Blob.Models;

namespace Storage.Net.Microsoft.Azure.DataLakeGen2.Store
{
   class AzureDataLakeStoreGen2BlobStorageProvider : IBlobStorage
   {
      private readonly IDataLakeGen2Client _client;

      private AzureDataLakeStoreGen2BlobStorageProvider(IDataLakeGen2Client client)
      {
         _client = client ?? throw new ArgumentNullException(nameof(client));
      }

      public int ListBatchSize { get; set; } = 5000;

      public static AzureDataLakeStoreGen2BlobStorageProvider CreateBySharedAccessKey(string accountName,
         string sharedAccessKey)
      {
         if(accountName == null)
            throw new ArgumentNullException(nameof(accountName));

         if(sharedAccessKey == null)
            throw new ArgumentNullException(nameof(sharedAccessKey));

         return new AzureDataLakeStoreGen2BlobStorageProvider(DataLakeGen2Client.Create(accountName, sharedAccessKey));
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
            credential.UserName, credential.Password));
      }

      public async Task<IReadOnlyCollection<BlobId>> ListAsync(ListOptions options, CancellationToken cancellationToken)
      {
         string[] split = options.FolderPath.Split('/');

         DirectoryList results =
            await _client.ListDirectoryAsync(split[0], split[1], options.Recurse, options.MaxResults ?? ListBatchSize);

         return results.Paths.Select(x => new BlobId(x.Name, x.IsDirectory ? BlobItemKind.Folder : BlobItemKind.File))
            .ToList();
      }

      public async Task WriteAsync(string id, Stream sourceStream, bool append, CancellationToken cancellationToken)
      {
         GenericValidation.CheckBlobId(id);
         string[] split = id.Split('/');

         if(!append)
         {
            await _client.CreateFileAsync(split[0], split[1]);
         }

         using(Stream stream = await _client.OpenWriteAsync(split[0], split[1]))
         {
            await sourceStream.CopyToAsync(stream);
         }
      }

      public async Task<Stream> OpenWriteAsync(string id, bool append, CancellationToken cancellationToken)
      {
         GenericValidation.CheckBlobId(id);
         string[] split = id.Split('/');

         return await _client.OpenWriteAsync(split[0], split[1]);
      }

      public Task<Stream> OpenReadAsync(string id, CancellationToken cancellationToken)
      {
         GenericValidation.CheckBlobId(id);
         string[] split = id.Split('/');

         return Task.FromResult(_client.OpenRead(split[0], split[1]));
      }

      public async Task DeleteAsync(IEnumerable<string> ids, CancellationToken cancellationToken)
      {
         var idList = ids.ToList();
         GenericValidation.CheckBlobId(idList);

         await Task.WhenAll(idList.Select(x =>
         {
            string[] split = x.Split('/');
            return _client.DeleteFileAsync(split[0], split[1]);
         }));
      }

      public async Task<IReadOnlyCollection<bool>> ExistsAsync(IEnumerable<string> ids,
         CancellationToken cancellationToken)
      {
         var idList = ids.ToList();
         GenericValidation.CheckBlobId(idList);

         bool[] tasks = await Task.WhenAll(idList.Select(async x =>
         {
            string[] split = x.Split('/');
            Properties properties = await _client.GetPropertiesAsync(split[0], split[1]);
            return properties.Exists;
         }));

         return tasks.ToList();
      }

      public async Task<IEnumerable<BlobMeta>> GetMetaAsync(IEnumerable<string> ids,
         CancellationToken cancellationToken)
      {
         var idList = ids.ToList();
         GenericValidation.CheckBlobId(idList);

         return await Task.WhenAll(idList.Select(async x =>
         {
            string[] split = x.Split('/');
            Properties properties = await _client.GetPropertiesAsync(split[0], split[1]);
            return new BlobMeta(properties.Length, null, properties.LastModified);
         }));
      }

      public void Dispose()
      {
      }

      public Task<ITransaction> OpenTransactionAsync()
      {
         return Task.FromResult(EmptyTransaction.Instance);
      }
   }
}