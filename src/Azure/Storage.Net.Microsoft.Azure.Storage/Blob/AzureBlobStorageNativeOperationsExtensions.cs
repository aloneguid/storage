using System;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.WindowsAzure.Storage.Blob;

namespace Storage.Net.Microsoft.Azure.Storage.Blob
{
   /// <summary>
   /// Takes in <see cref="IAzureBlobStorageNativeOperations"/> and adds ready-made operations for creating resource
   /// URIs with SAS tokens.
   /// </summary>
   public static class AzureBlobStorageNativeOperationsExtensions
   {
      /// <summary>
      /// Returns Uri to Azure Blob with read-only Shared Access Token.
      /// </summary>
      public static async Task<string> GetReadOnlySasUriAsync(
         this IAzureBlobStorageNativeOperations provider,
         string id,
         int minutesToExpiration = 30,
         CancellationToken cancellationToken = default)
      {
         return await provider.GetSasUriAsync(
            id,
            GetSharedAccessBlobPolicy(minutesToExpiration, SharedAccessBlobPermissions.Read),
            createContainer: false,
            cancellationToken);
      }

      /// <summary>
      /// Returns Uri to Azure Blob with write-only Shared Access Token.
      /// </summary>
      public static async Task<string> GetWriteOnlySasUriAsync(
         this IAzureBlobStorageNativeOperations provider,
         string id,
         int minutesToExpiration = 30,
         CancellationToken cancellationToken = default)
      {
         return await provider.GetSasUriAsync(
            id,
            GetSharedAccessBlobPolicy(minutesToExpiration, SharedAccessBlobPermissions.Write),
            createContainer: true,
            cancellationToken);
      }

      /// <summary>
      /// Returns Uri to Azure Blob with read-write Shared Access Token.
      /// </summary>
      public static async Task<string> GetReadWriteSasUriAsync(
         this IAzureBlobStorageNativeOperations provider,
         string id,
         int minutesToExpiration = 30,
         CancellationToken cancellationToken = default)
      {
         return await provider.GetSasUriAsync(
            id,
            GetSharedAccessBlobPolicy(minutesToExpiration, SharedAccessBlobPermissions.Read | SharedAccessBlobPermissions.Write),
            createContainer: true,
            cancellationToken);
      }

      private static SharedAccessBlobPolicy GetSharedAccessBlobPolicy(
          int minutesToExpiration,
          SharedAccessBlobPermissions permissions,
          int startTimeCorrection = -5)
      {
         DateTimeOffset now = DateTimeOffset.UtcNow;

         return new SharedAccessBlobPolicy
         {
            SharedAccessStartTime = now.AddMinutes(startTimeCorrection),
            SharedAccessExpiryTime = now.AddMinutes(minutesToExpiration),
            Permissions = permissions,
         };
      }
   }
}
