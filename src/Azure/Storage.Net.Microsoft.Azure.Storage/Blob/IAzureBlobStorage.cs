using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.WindowsAzure.Storage.Blob;
using Storage.Net.Blob;

namespace Storage.Net.Microsoft.Azure.Storage.Blob
{
   /// <summary>
   /// Provides access to native operations
   /// </summary>
   public interface IAzureBlobStorage : IBlobStorage
   {
      /// <summary>
      /// Returns reference to the native Azure SD blob client.
      /// </summary>
      CloudBlobClient NativeBlobClient { get; }

      /// <summary>
      /// Returns Uri to Azure Blob with Shared Access Token.
      /// </summary>
      Task<string> GetSasUriAsync(string id,
         SharedAccessBlobPolicy sasConstraints,
         SharedAccessBlobHeaders headers = null,
         bool createContainer = false,
         CancellationToken cancellationToken = default);

      /// <summary>
      /// Returns Uri to Azure Blob with read-only Shared Access Token.
      /// </summary>
      Task<string> GetReadOnlySasUriAsync(
         string id,
         SharedAccessBlobHeaders headers = null,
         int minutesToExpiration = 30,
         CancellationToken cancellationToken = default);

      /// <summary>
      /// Returns Uri to Azure Blob with write-only Shared Access Token.
      /// </summary>
      Task<string> GetWriteOnlySasUriAsync(
         string id,
         SharedAccessBlobHeaders headers = null,
         int minutesToExpiration = 30,
         CancellationToken cancellationToken = default);

      /// <summary>
      /// Returns Uri to Azure Blob with read-write Shared Access Token.
      /// </summary>
      Task<string> GetReadWriteSasUriAsync(
         string id,
         SharedAccessBlobHeaders headers = null,
         int minutesToExpiration = 30,
         CancellationToken cancellationToken = default);

      /// <summary>
      /// Acquires a lease
      /// </summary>
      /// <param name="id"></param>
      /// <param name="maxLeaseTime"></param>
      /// <param name="waitForRelease">When true, the call will wait for the lock to be released</param>
      /// <param name="cancellationToken"></param>
      /// <returns></returns>
      Task<BlobLease> AcquireBlobLeaseAsync(string id, TimeSpan maxLeaseTime, bool waitForRelease = false, CancellationToken cancellationToken = default);
   }
}
