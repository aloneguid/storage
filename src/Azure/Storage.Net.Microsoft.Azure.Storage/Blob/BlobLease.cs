using System;
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
      private readonly string _leaseId;

      internal BlobLease(CloudBlockBlob blob, string leaseId)
      {
         _blob = blob;
         _leaseId = leaseId;
      }

      /// <summary>
      /// Releases the lease
      /// </summary>
      public void Dispose()
      {
         try
         {
            _blob.ReleaseLeaseAsync(AccessCondition.GenerateLeaseCondition(_leaseId)).Wait();
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
