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

      public async Task DeleteAsync(IEnumerable<string> ids, CancellationToken cancellationToken)
      {
         var idList = ids.ToList();
         GenericValidation.CheckBlobId(idList);

         await Task.WhenAll(idList.Select(x => DeleteAsync(x, cancellationToken)).ToList());
      }

      public async Task DeleteAsync(string id, CancellationToken cancellationToken)
      {
         GenericValidation.CheckBlobId(id);
         var info = new PathInformation(id);
         string[] directoryLevels = info.Path.Split('/');

         var levels = await Task.WhenAll(directoryLevels
            .Select((y, z) => string.Join("/", directoryLevels.Take(directoryLevels.Length - z)))
            .Select(async x =>
            {
               Properties properties = await Client.GetPropertiesAsync(info.Filesystem, x, cancellationToken);

               return new
               {
                  Path = x,
                  properties.IsDirectory
               };
            }));

         foreach(var level in levels)
         {
            if(!level.IsDirectory)
            {
               await Client.DeleteFileAsync(info.Filesystem, info.Path, cancellationToken);
               continue;
            }

            DirectoryList directoryList =
                  await Client.ListDirectoryAsync(info.Filesystem, level.Path, true, 2, cancellationToken);

            if(directoryList.Paths.Any())
            {
               return;
            }

            await Client.DeleteDirectoryAsync(info.Filesystem, level.Path, true, cancellationToken);
         }
      }

      public async Task<IReadOnlyCollection<BlobId>> ListAsync(ListOptions options, CancellationToken cancellationToken)
      {
         if(options == null)
         {
            options = new ListOptions()
            {
               FolderPath = "/",
               Recurse = true
            };
         }

         GenericValidation.CheckBlobId(options.FolderPath);

         var blobs = new List<BlobId>();
         var info = new PathInformation(options.FolderPath);

         FilesystemList filesystemList = await Client.ListFilesystemsAsync(cancellationToken: cancellationToken);

         IEnumerable<FilesystemItem> filesystems = filesystemList.Filesystems
            .Where(x => info.Filesystem == "" || x.Name == info.Filesystem)
            .OrderBy(x => x.Name);

         foreach(FilesystemItem filesystem in filesystems)
         {
            try
            {
               DirectoryList directoryList = await Client.ListDirectoryAsync(
                  filesystem.Name, info.Path, options.Recurse,
                  options.MaxResults ?? ListBatchSize, cancellationToken);

               IEnumerable<BlobId> results = directoryList.Paths
                  .Where(x => options.FilePrefix == null || x.Name.StartsWith(options.FilePrefix))
                  .Select(x => new BlobId($"{filesystem.Name}/{x.Name}", x.IsDirectory
                     ? BlobItemKind.Folder
                     : BlobItemKind.File))
                  .Where(x => options.BrowseFilter == null || options.BrowseFilter(x));

               blobs.AddRange(results);
            }
            catch(DataLakeGen2Exception e)
            {
               if(e.StatusCode != HttpStatusCode.NotFound)
               {
                  throw;
               }
            }

            if(blobs.Count >= options.MaxResults)
            {
               return blobs.Take(options.MaxResults.GetValueOrDefault()).ToList();
            }
         }

         return blobs;
      }

      public async Task<Stream> OpenReadAsync(string id, CancellationToken cancellationToken)
      {
         GenericValidation.CheckBlobId(id);
         var info = new PathInformation(id);

         return await TryGetPropertiesAsync(info.Filesystem, info.Path, cancellationToken) == null
            ? null
            : Client.OpenRead(info.Filesystem, info.Path);
      }

      public async Task<Stream> OpenWriteAsync(string id, bool append, CancellationToken cancellationToken)
      {
         GenericValidation.CheckBlobId(id);
         var info = new PathInformation(id);

         return await Client.OpenWriteAsync(info.Filesystem, info.Path, cancellationToken);
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

      public async Task<IReadOnlyCollection<bool>> ExistsAsync(IEnumerable<string> ids,
         CancellationToken cancellationToken)
      {
         var idList = ids.ToList();
         GenericValidation.CheckBlobId(idList);

         bool[] tasks = await Task.WhenAll(idList.Select(async x =>
         {
            var info = new PathInformation(x);
            Properties properties = await TryGetPropertiesAsync(info.Filesystem, info.Path, cancellationToken);
            return properties != null;
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
            Properties properties = await TryGetPropertiesAsync(info.Filesystem, info.Path, cancellationToken);
            return properties == null ? null : new BlobMeta(properties.Length, null, properties.LastModified);
         }));
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
            return await Client.GetPropertiesAsync(filesystem, path, cancellationToken);
         }
         catch(DataLakeGen2Exception e)
         {
            if(e.StatusCode == HttpStatusCode.NotFound)
            {
               return null;
            }

            throw;
         }
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