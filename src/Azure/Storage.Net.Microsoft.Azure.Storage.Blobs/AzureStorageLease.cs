using System;
using System.Threading.Tasks;
using Azure;
using Azure.Storage.Blobs.Specialized;

namespace Storage.Net.Microsoft.Azure.Storage.Blobs
{
   /// <summary>
   /// Represents a blob lease
   /// </summary>
   public class AzureStorageLease : IDisposable
   {
      internal AzureStorageLease(BlobLeaseClient leaseClient)
      {
         LeaseClient = leaseClient;
      }

      internal BlobLeaseClient LeaseClient { get; }

      /// <summary>
      /// Renews active lease
      /// </summary>
      /// <returns></returns>
      public Task RenewLeaseAsync()
      {
         return LeaseClient.RenewAsync();
      }

      /// <summary>
      /// Releases the lease
      /// </summary>
      public void Dispose()
      {
         try
         {
            LeaseClient.Release();
         }
         catch(RequestFailedException ex) when (ex.ErrorCode == "LeaseIdMismatchWithLeaseOperation")
         {
            //happens when a lease expires
         }
      }
   }
}
