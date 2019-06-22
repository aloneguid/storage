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
   class AzureDataLakeStoreGen2BlobStorageProvider : IAzureDataLakeGen2Storage
   {
      private AzureDataLakeStoreGen2BlobStorageProvider(IDataLakeGen2Client client)
      {
         Client = client ?? throw new ArgumentNullException(nameof(client));
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
         var info = new PathInformation(options.FolderPath);

         DirectoryList results =
            await Client.ListDirectoryAsync(info.Filesystem, info.Path, options.Recurse, options.MaxResults ?? ListBatchSize, cancellationToken);

         return results.Paths
            .Select(x => new BlobId(x.Name, x.IsDirectory ? BlobItemKind.Folder : BlobItemKind.File))
            .ToList();
      }

      public async Task WriteAsync(string id, Stream sourceStream, bool append, CancellationToken cancellationToken)
      {
         GenericValidation.CheckBlobId(id);
         var info = new PathInformation(id);

         if(!append)
         {
            await Client.CreateFileAsync(info.Filesystem, info.Path, cancellationToken);
         }

         using(Stream stream = await Client.OpenWriteAsync(info.Filesystem, info.Path, cancellationToken))
         {
            await sourceStream.CopyToAsync(stream);
         }
      }

      public async Task<Stream> OpenWriteAsync(string id, bool append, CancellationToken cancellationToken)
      {
         GenericValidation.CheckBlobId(id);
         var info = new PathInformation(id);

         return await Client.OpenWriteAsync(info.Filesystem, info.Path, cancellationToken);
      }

      public Task<Stream> OpenReadAsync(string id, CancellationToken cancellationToken)
      {
         GenericValidation.CheckBlobId(id);
         var info = new PathInformation(id);

         return Task.FromResult(Client.OpenRead(info.Filesystem, info.Path));
      }

      public async Task DeleteAsync(IEnumerable<string> ids, CancellationToken cancellationToken)
      {
         var idList = ids.ToList();
         GenericValidation.CheckBlobId(idList);

         await Task.WhenAll(idList.Select(x =>
         {
            var info = new PathInformation(x);
            return Client.DeleteFileAsync(info.Filesystem, info.Path, cancellationToken);
         }));
      }

      public async Task<IReadOnlyCollection<bool>> ExistsAsync(IEnumerable<string> ids,
         CancellationToken cancellationToken)
      {
         var idList = ids.ToList();
         GenericValidation.CheckBlobId(idList);

         bool[] tasks = await Task.WhenAll(idList.Select(async x =>
         {
            var info = new PathInformation(x);
            Properties properties = await Client.GetPropertiesAsync(info.Filesystem, info.Path, cancellationToken);
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
            var info = new PathInformation(x);
            Properties properties = await Client.GetPropertiesAsync(info.Filesystem, info.Path, cancellationToken);
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

      public IDataLakeGen2Client Client { get; }

      private class PathInformation
      {
         public PathInformation(string id)
         {
            string[] split = id.Split('/');

            if(split.Length < 2)
            {
               throw new ArgumentException("id must contain a filesystem and a path.");
            }

            Filesystem = split.First();
            Path = string.Join("/", split.Skip(1));
         }

         public string Filesystem { get; }
         public string Path { get; }
      }
   }
}