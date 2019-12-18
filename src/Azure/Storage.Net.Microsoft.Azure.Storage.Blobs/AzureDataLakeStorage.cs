using System;
using System.Collections.Generic;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Azure.Storage;
using Azure.Storage.Blobs;
using Blobs;
using Storage.Net.Blobs;
using Storage.Net.Microsoft.Azure.Storage.Blobs.Gen2.Model;

namespace Storage.Net.Microsoft.Azure.Storage.Blobs
{
   class AzureDataLakeStorage : AzureBlobStorage, IAzureDataLakeStorage
   {
      private readonly ExtendedSdk _extended;

      public AzureDataLakeStorage(BlobServiceClient blobServiceClient, string accountName, StorageSharedKeyCredential sasSigningCredentials = null, string containerName = null) : base(blobServiceClient, accountName, sasSigningCredentials, containerName)
      {
         _extended = new ExtendedSdk(blobServiceClient, accountName);
      }

      #region [ Data Lake Storage ]

      public Task<IReadOnlyCollection<Filesystem>> ListFilesystemsAsync(CancellationToken cancellationToken = default)
      {
         return _extended.ListFilesystemsAsync(cancellationToken);
      }

      public Task CreateFilesystemAsync(string filesystemName, CancellationToken cancellationToken = default)
      {
         return _extended.CreateFilesystemAsync(filesystemName, cancellationToken);
      }

      public Task DeleteFilesystemAsync(string filesystemName, CancellationToken cancellationToken = default)
      {
         return _extended.DeleteFilesystemAsync(filesystemName, cancellationToken);
      }

      public Task SetAccessControlAsync(string fullPath, AccessControl accessControl, CancellationToken cancellationToken = default)
      {
         return _extended.SetAccessControlAsync(fullPath, accessControl, cancellationToken);
      }

      public Task<AccessControl> GetAccessControlAsync(string fullPath, bool getUpn = false, CancellationToken cancellationToken = default)
      {
         return _extended.GetAccessControlAsync(fullPath, getUpn, cancellationToken);
      }

      #endregion

      protected override Task DeleteAsync(string fullPath, CancellationToken cancellationToken)
      {
         return _extended.DeleteAsync(fullPath, cancellationToken);
      }

      public override Task<IReadOnlyCollection<Blob>> ListAsync(
         ListOptions options, CancellationToken cancellationToken)
      {
         return _extended.ListAsync(options, cancellationToken);
      }

      protected override Task<Blob> GetBlobAsync(string fullPath, CancellationToken cancellationToken)
      {
         return _extended.GetBlobAsync(fullPath, cancellationToken);
      }

   }
}
