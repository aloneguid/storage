using System;
using System.Threading;
using System.Threading.Tasks;
using Storage.Net.Blobs;

namespace Storage.Net.Microsoft.Azure.Storage.Blobs
{

   /// <summary>
   /// Azure blob storage specific operations
   /// </summary>
   public interface IAzureBlobStorage : IBlobStorage
   {
      /// <summary>
      /// Acquires a lease
      /// </summary>
      /// <param name="fullPath">Path to container or blob</param>
      /// <param name="maxLeaseTime">When set, leases for a maximum period of time, otherwise lease is set for infinite time.</param>
      /// <param name="proposedLeaseId">When specified, will use as lease ID.</param>
      /// <param name="waitForRelease">When true, the call will wait for the lease to be released.</param>
      /// <param name="cancellationToken"></param>
      /// <returns></returns>
      Task<AzureStorageLease> AcquireLeaseAsync(
         string fullPath,
         TimeSpan? maxLeaseTime = null,
         string proposedLeaseId = null,
         bool waitForRelease = false,
         CancellationToken cancellationToken = default);

      /// <summary>
      /// Breaks container or blob lease
      /// </summary>
      /// <param name="fullPath"></param>
      /// <param name="ignoreErrors">When set, all the errors are ignored. Good for initialisation scenarios.</param>
      /// <param name="cancellationToken"></param>
      /// <returns></returns>
      Task BreakLeaseAsync(
         string fullPath,
         bool ignoreErrors = false,
         CancellationToken cancellationToken = default);

      /// <summary>
      /// Gets public access type for a specific blob container
      /// </summary>
      /// <param name="containerName"></param>
      /// <param name="cancellationToken"></param>
      /// <returns></returns>
      Task<ContainerPublicAccessType> GetContainerPublicAccessAsync(string containerName, CancellationToken cancellationToken = default);

      /// <summary>
      /// Sets public access type for a specific blob container
      /// </summary>
      /// <param name="containerName"></param>
      /// <param name="containerPublicAccessType"></param>
      /// <param name="cancellationToken"></param>
      /// <returns></returns>
      Task SetContainerPublicAccessAsync(
         string containerName, ContainerPublicAccessType containerPublicAccessType, CancellationToken cancellationToken = default);


      /// <summary>
      /// Gets Shared Access Signature for the path.
      /// </summary>
      /// <param name="accountPolicy"></param>
      /// <param name="includeUrl"></param>
      /// <param name="cancellationToken"></param>
      /// <returns></returns>
      Task<string> GetStorageSasAsync(
         AccountSasPolicy accountPolicy,
         bool includeUrl = true,
         CancellationToken cancellationToken = default);

      /// <summary>
      /// Gets Shared Access Signature for a blob container
      /// </summary>
      /// <returns></returns>
      Task<string> GetContainerSasAsync(
         string containerName,
         ContainerSasPolicy containerSasPolicy,
         bool includeUrl = true,
         CancellationToken cancellationToken = default);

      /// <summary>
      /// Gets Shared Access Signature for a single blob
      /// </summary>
      /// <param name="fullPath"></param>
      /// <param name="blobSasPolicy">Access policy, by default set to unlimited read for 1 hour.</param>
      /// <param name="includeUrl"></param>
      /// <param name="cancellationToken"></param>
      /// <returns></returns>
      Task<string> GetBlobSasAsync(
         string fullPath,
         BlobSasPolicy blobSasPolicy = null,
         bool includeUrl = true,
         CancellationToken cancellationToken = default);


   }
}
