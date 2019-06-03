using System;
using System.Threading.Tasks;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Blob;
using AzureStorageException = Microsoft.WindowsAzure.Storage.StorageException;

namespace Storage.Net.Microsoft.Azure.Storage.Blob
{
   /// <summary>
   /// Represents a blob lease
   /// </summary>
   public class BlobLease : IDisposable
   {
      private readonly CloudBlockBlob _blob;

      internal BlobLease(CloudBlockBlob blob, string leaseId)
      {
         _blob = blob;
         LeaseId = leaseId;
      }

      /// <summary>
      /// Original Lease ID
      /// </summary>
      public string LeaseId { get; }

      /// <summary>
      /// Original blob that is leased
      /// </summary>
      public CloudBlockBlob LeasedBlob => _blob;

      /// <summary>
      /// Renews active lease
      /// </summary>
      /// <returns></returns>
      public async Task RenewLeaseAsync()
      {
         await _blob.RenewLeaseAsync(AccessCondition.GenerateLeaseCondition(LeaseId));
      }

      /// <summary>
      /// Releases the lease
      /// </summary>
      public void Dispose()
      {
         try
         {
            _blob.ReleaseLeaseAsync(AccessCondition.GenerateLeaseCondition(LeaseId)).Wait();
         }
         catch(AggregateException egex)
            when(
               (egex.InnerException is AzureStorageException sx) && 
               (sx.RequestInformation.ErrorCode == "LeaseIdMismatchWithLeaseOperation"))
         {
            
         }
      }
   }
}
