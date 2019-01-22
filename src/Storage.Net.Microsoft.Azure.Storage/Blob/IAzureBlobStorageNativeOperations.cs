using Microsoft.WindowsAzure.Storage.Blob;

namespace Storage.Net.Microsoft.Azure.Storage.Blob
{
   /// <summary>
   /// Provides access to native operations
   /// </summary>
   public interface IAzureBlobStorageNativeOperations
   {
      /// <summary>
      /// Returns reference to the native Azure SD blob client.
      /// </summary>
      CloudBlobClient NativeBlobClient { get; }
   }
}
