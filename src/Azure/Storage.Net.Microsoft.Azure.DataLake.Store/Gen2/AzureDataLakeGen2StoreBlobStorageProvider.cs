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

namespace Storage.Net.Microsoft.Azure.DataLake.Store.Gen2
{
   class AzureDataLakeStoreGen2BlobStorageProvider : IBlobStorage
   {
      private readonly DataLakeGen2Client _client;

      private AzureDataLakeStoreGen2BlobStorageProvider(DataLakeGen2Client client)
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

      public static AzureDataLakeStoreGen2BlobStorageProvider CreateByManagedIdentity(string accountName)
      {
         if(accountName == null)
            throw new ArgumentNullException(nameof(accountName));

         return new AzureDataLakeStoreGen2BlobStorageProvider(DataLakeGen2Client.Create(accountName));
      }

      public async Task DeleteAsync(IEnumerable<string> fullPaths, CancellationToken cancellationToken = default)
      {
         var fullPathsList = fullPaths.ToList();
         GenericValidation.CheckBlobFullPaths(fullPathsList);

         await Task.WhenAll(fullPathsList.Select(x => DeleteAsync(x, cancellationToken)).ToList());
      }

      public async Task<IReadOnlyCollection<Blob>> ListAsync(ListOptions options = null, CancellationToken cancellationToken = default)
      {
         if(options == null)
         {
            options = new ListOptions()
            {
               FolderPath = "/",
               Recurse = true
            };
         }

         GenericValidation.CheckBlobFullPath(options.FolderPath);

         var info = new PathInformation(options.FolderPath);
         int maxResults = options.MaxResults ?? ListBatchSize;
         var blobs = new List<Blob>();

         FilesystemList filesystemList = await _client.ListFilesystemsAsync(cancellationToken: cancellationToken);

         IEnumerable<FilesystemItem> filesystems =
            filesystemList.Filesystems
               .Where(x => info.Filesystem == "" || x.Name == info.Filesystem)
               .OrderBy(x => x.Name);

         foreach(FilesystemItem filesystem in filesystems)
         {
            try
            {
               DirectoryList directoryList = await _client.ListDirectoryAsync(
                  filesystem.Name, info.Path, options.Recurse,
                  cancellationToken: cancellationToken);

               IEnumerable<Blob> results = directoryList.Paths
                  .Where(x => options.FilePrefix == null || x.Name.StartsWith(options.FilePrefix))
                  .Select(x =>
                     new Blob($"{filesystem.Name}/{x.Name}",
                        x.IsDirectory ? BlobItemKind.Folder : BlobItemKind.File))
                  .Where(x => options.BrowseFilter == null || options.BrowseFilter(x))
                  .OrderBy(x => x.FullPath);

               blobs.AddRange(results);
            }
            catch(DataLakeGen2Exception e) when(e.StatusCode == HttpStatusCode.NotFound)
            {

            }

            if(blobs.Count >= maxResults)
            {
               return blobs.Take(maxResults).ToList();
            }
         }

         return blobs.ToList();
      }

      public async Task<Stream> OpenReadAsync(string fullPath, CancellationToken cancellationToken = default)
      {
         GenericValidation.CheckBlobFullPath(fullPath);
         var info = new PathInformation(fullPath);

         return await TryGetPropertiesAsync(info.Filesystem, info.Path, cancellationToken) == null
            ? null
            : _client.OpenRead(info.Filesystem, info.Path);
      }

      public async Task<Stream> OpenWriteAsync(string fullPath, bool append = false, CancellationToken cancellationToken = default)
      {
         GenericValidation.CheckBlobFullPath(fullPath);
         var info = new PathInformation(fullPath);

         if(!append)
         {
            await _client.CreateFileAsync(info.Filesystem, info.Path, cancellationToken);
         }

         return await _client.OpenWriteAsync(info.Filesystem, info.Path, cancellationToken);
      }

      public async Task<IReadOnlyCollection<bool>> ExistsAsync(IEnumerable<string> fullPaths,
         CancellationToken cancellationToken = default)
      {
         return (await GetBlobsAsync(fullPaths, cancellationToken)).Select(x => x != null).ToList();
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

      private async Task DeleteAsync(string fullPath, CancellationToken cancellationToken)
      {
         GenericValidation.CheckBlobFullPath(fullPath);
         var info = new PathInformation(fullPath);

         Properties properties = await _client.GetPropertiesAsync(info.Filesystem, info.Path, cancellationToken);

         if(properties.IsDirectory)
         {
            await _client.DeleteDirectoryAsync(info.Filesystem, info.Path, true, cancellationToken);
            return;
         }

         await _client.DeleteFileAsync(info.Filesystem, info.Path, cancellationToken);
      }

      private async Task<Properties> TryGetPropertiesAsync(string filesystem, string path, CancellationToken cancellationToken)
      {
         try
         {
            return await _client.GetPropertiesAsync(filesystem, path, cancellationToken);
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