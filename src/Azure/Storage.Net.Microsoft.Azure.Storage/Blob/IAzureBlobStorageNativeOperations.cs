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
   public interface IAzureBlobStorageNativeOperations : IBlobStorage
   {
      /// <summary>
      /// Returns reference to the native Azure SD blob client.
      /// </summary>
      CloudBlobClient NativeBlobClient { get; }

      /// <summary>
      /// Returns Uri to Azure Blob with Shared Access Token.
      /// </summary>
      Task<string> GetSasUriAsync(string id, SharedAccessBlobPolicy sasConstraints, bool createContainer = false, CancellationToken cancellationToken = default);

#if DEBUG
      /// <summary>
      /// Opens Azure Blob for random access
      /// </summary>
      Task<Stream> OpenRandomAccessReadAsync(string id, CancellationToken cancellationToken = default);
#endif
   }
}
