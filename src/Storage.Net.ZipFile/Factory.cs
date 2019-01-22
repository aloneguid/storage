using Storage.Net.Blob;
using Storage.Net.ZipFile;

namespace Storage.Net
{
   /// <summary>
   /// Factory helpers
   /// </summary>
   public static class Factory
   {
      /// <summary>
      /// Zip file
      /// </summary>
      public static IBlobStorage ZipFile(this IBlobStorageFactory blobStorageFactory, string filePath)
      {
         return new ZipFileBlobStorageProvider(filePath);
      }
   }
}
