using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Auth;
using Microsoft.WindowsAzure.Storage.Blob;
using NetBox.Extensions;
using Storage.Net.Blob;
using AzureStorageException = Microsoft.WindowsAzure.Storage.StorageException;

namespace Storage.Net.Microsoft.Azure.Storage.Blob
{
   class AzureUniversalBlobStorageProvider : IBlobStorage, IAzureBlobStorageNativeOperations
   {
      private readonly CloudBlobClient _client;
      private readonly Dictionary<string, CloudBlobContainer> _containerNameToContainer = new Dictionary<string, CloudBlobContainer>();
      private CloudBlobContainer _fixedContainer;
      private readonly string _fixedContainerName;

      public CloudBlobClient NativeBlobClient => _client;

      public AzureUniversalBlobStorageProvider(string accountName, string key, string containerName = null)
      {
         var account = new CloudStorageAccount(
            new StorageCredentials(accountName, key),
            true);

         _client = account.CreateCloudBlobClient();
         _fixedContainerName = containerName;
      }

      private AzureUniversalBlobStorageProvider(Uri sasUri)
      {
         if (sasUri == null)
         {
            throw new ArgumentNullException(nameof(sasUri));
         }

         _fixedContainer = new CloudBlobContainer(sasUri);
         _client = _fixedContainer.ServiceClient;
      }

      public static AzureUniversalBlobStorageProvider CreateWithContainerSasUri(Uri sasUri)
      {
         return new AzureUniversalBlobStorageProvider(sasUri);
      }

      public async Task DeleteAsync(IEnumerable<string> ids, CancellationToken cancellationToken = default)
      {
         GenericValidation.CheckBlobId(ids);

         await Task.WhenAll(ids.Select(id => DeleteAsync(id, cancellationToken)));
      }

      private async Task DeleteAsync(string id, CancellationToken cancellationToken)
      {
         (CloudBlobContainer container, string path) = await GetPartsAsync(id, false);

         CloudBlockBlob blob = container.GetBlockBlobReference(StoragePath.Normalize(path, false));
         await blob.DeleteIfExistsAsync();
      }

      public void Dispose()
      {
         
      }

      public async Task<IReadOnlyCollection<bool>> ExistsAsync(IEnumerable<string> ids, CancellationToken cancellationToken = default)
      {
         var result = new List<bool>();

         foreach (string id in ids)
         {
            GenericValidation.CheckBlobId(id);

            (CloudBlobContainer container, string path) = await GetPartsAsync(id, false);
            if (container == null)
            {
               result.Add(false);
            }
            else
            {
               CloudBlockBlob blob = container.GetBlockBlobReference(StoragePath.Normalize(path, false));
               bool exists = await blob.ExistsAsync();
               result.Add(exists);
            }
         }

         return result;
      }

      public async Task<IEnumerable<BlobMeta>> GetMetaAsync(IEnumerable<string> ids, CancellationToken cancellationToken = default)
      {
         var result = new List<BlobMeta>();
         foreach (string id in ids)
         {
            GenericValidation.CheckBlobId(id);
         }

         return await Task.WhenAll(ids.Select(id => GetMetaAsync(id, cancellationToken)));
      }

      private async Task<BlobMeta> GetMetaAsync(string id, CancellationToken cancellationToken)
      {
         (CloudBlobContainer container, string path) = await GetPartsAsync(id, false);
         if (container == null) return null;

         CloudBlob blob = container.GetBlobReference(StoragePath.Normalize(path, false));
         if (!(await blob.ExistsAsync())) return null;

         await blob.FetchAttributesAsync();

         return GetblobMeta(blob);
      }

      internal static BlobMeta GetblobMeta(CloudBlob blob)
      {
         //ContentMD5 is base64-encoded hash, whereas we work with HEX encoded ones
         string md5 = blob.Properties.ContentMD5.Base64DecodeAsBytes().ToHexString();

         var meta = new BlobMeta(
            blob.Properties.Length,
            md5,
            blob.Properties.LastModified);

         /*meta.Properties["BlobType"] = blob.BlobType.ToString();
         meta.Properties["IsDeleted"] = blob.IsDeleted;
         meta.Properties["IsSnapshot"] = blob.IsSnapshot;
         meta.Properties["ContentDisposition"] = blob.Properties.ContentDisposition;
         meta.Properties["ContentEncoding"] = blob.Properties.ContentEncoding;
         meta.Properties["ContentLanguage"] = blob.Properties.ContentLanguage;
         meta.Properties["ContentType"] = blob.Properties.ContentType;*/

         return meta;
      }

      public async Task<IReadOnlyCollection<BlobId>> ListAsync(ListOptions options, CancellationToken cancellationToken = default)
      {
         if (options == null) options = new ListOptions();

         var result = new List<BlobId>();
         var containers = new List<CloudBlobContainer>();
         CloudBlobContainer fixedContainer = await GetFixedContainerAsync();

         if(fixedContainer != null)
         {
            containers.Add(fixedContainer);
         }
         else if(StoragePath.IsRootPath(options.FolderPath))
         {
            // list all of the containers
            containers.AddRange(await GetCloudBlobContainersAsync(cancellationToken));

            //represent containers as folders in the result
            result.AddRange(containers.Select(c => new BlobId(c.Name, BlobItemKind.Folder)));

            if(!options.Recurse) return result;
         }
         else
         {
            (CloudBlobContainer container, string path) = await GetPartsAsync(options.FolderPath, false);
            if (container == null) return new List<BlobId>();
            options = options.Clone();
            options.FolderPath = path; //scan from subpath now
            containers.Add(container);

            //add container as search result
            //result.Add(new BlobId(container.Name, BlobItemKind.Folder));
         }

         await Task.WhenAll(containers.Select(c => ListAsync(c, fixedContainer, result, options, cancellationToken)));

         if(options.MaxResults != null)
         {
            result = result.Take(options.MaxResults.Value).ToList();
         }

         return result;
      }

      private async Task ListAsync(CloudBlobContainer container, CloudBlobContainer fixedContainer,
         List<BlobId> result,
         ListOptions options,
         CancellationToken cancellationToken)
      {
         var browser = new AzureBlobDirectoryBrowser(container, _fixedContainer == null, options.MaxDegreeOfParalellism);
         IReadOnlyCollection<BlobId> containerBlobs = await browser.ListFolderAsync(options, cancellationToken);
         if (containerBlobs.Count > 0)
         {
            result.AddRange(containerBlobs);
         }
      }

      private async Task<List<CloudBlobContainer>> GetCloudBlobContainersAsync(CancellationToken cancellationToken)
      {
         var result = new List<CloudBlobContainer>();

         ContainerResultSegment firstPage = await _client.ListContainersSegmentedAsync(null);
         result.AddRange(firstPage.Results);

         //todo: list more containers

         return result;
      }

      public async Task<Stream> OpenReadAsync(string id, CancellationToken cancellationToken = default)
      {
         GenericValidation.CheckBlobId(id);

         (CloudBlobContainer container, string path) = await GetPartsAsync(id, false);

         if (container == null) return null;

         CloudBlockBlob blob = container.GetBlockBlobReference(StoragePath.Normalize(path, false));

         try
         {
            return await blob.OpenReadAsync();
         }
         catch (AzureStorageException ex)
         {
            if (AzureStorageValidation.IsDoesntExist(ex)) return null;

            if (!AzureStorageValidation.TryHandleStorageException(ex)) throw;
         }

         throw new Exception("must not be here");
      }

      public Task<ITransaction> OpenTransactionAsync()
      {
         return Task.FromResult(EmptyTransaction.Instance);
      }

      public async Task<Stream> OpenWriteAsync(string id, bool append, CancellationToken cancellationToken)
      {
         (CloudBlobContainer container, string path) = await GetPartsAsync(id);

         if (append)
         {
            CloudAppendBlob cab = container.GetAppendBlobReference(path);

            return await cab.OpenWriteAsync(!append);
         }
         else
         {
            CloudBlockBlob cab = container.GetBlockBlobReference(path);

            return await cab.OpenWriteAsync();

         }
      }

      public async Task WriteAsync(string id, Stream sourceStream, bool append, CancellationToken cancellationToken)
      {
         GenericValidation.CheckSourceStream(sourceStream);

         (CloudBlobContainer container, string path) = await GetPartsAsync(id);

         if (append)
         {
            CloudAppendBlob cab = container.GetAppendBlobReference(StoragePath.Normalize(path, false));
            if (!(await cab.ExistsAsync())) await cab.CreateOrReplaceAsync();

            await cab.AppendFromStreamAsync(sourceStream);

         }
         else
         {
            CloudBlockBlob blob = container.GetBlockBlobReference(StoragePath.Normalize(path, false));

            await blob.UploadFromStreamAsync(sourceStream);
         }
      }

      #region [ Path forking ]

      private async Task<CloudBlobContainer> GetFixedContainerAsync()
      {
         if (_fixedContainer != null)
            return _fixedContainer;

         if (_fixedContainerName != null)
         {
            _fixedContainer = _client.GetContainerReference(_fixedContainerName);
            await _fixedContainer.CreateIfNotExistsAsync();
            return _fixedContainer;
         }

         return null;
      }

      private async Task<(CloudBlobContainer, string)> GetPartsAsync(string path, bool createContainer = true)
      {
         GenericValidation.CheckBlobId(path);

         path = StoragePath.Normalize(path);
         if (path == null) throw new ArgumentNullException(nameof(path));

         CloudBlobContainer fixedContainer = await GetFixedContainerAsync();

         if(fixedContainer != null)
         {
            return (_fixedContainer, path);
         }

         int idx = path.IndexOf(StoragePath.PathSeparator);
         string containerName, relativePath;
         if (idx == -1)
         {
            containerName = path;
            relativePath = string.Empty;
         }
         else
         {
            containerName = path.Substring(0, idx);
            relativePath = path.Substring(idx + 1);
         }

         if (!_containerNameToContainer.TryGetValue(containerName, out CloudBlobContainer container))
         {
            container = _client.GetContainerReference(containerName);
            if(!(await container.ExistsAsync()))
            {
               if(createContainer)
               {
                  await container.CreateIfNotExistsAsync();
               }
               else
               {
                  return (null, null);
               }
            }

            _containerNameToContainer[containerName] = container;
         }

         return (container, relativePath);
      }

      private string GetRelativePath(string path)
      {
         int idx = path.IndexOf(StoragePath.PathSeparator);
         if (idx == -1) throw new ArgumentException("blob path must contain container name", nameof(path));

         return path.Substring(idx);
      }

      #endregion
   }
}
