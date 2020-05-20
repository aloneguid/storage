using Storage.Net.Blobs;

namespace Storage.Net
{
   /// <summary>
   /// Virtual storage
   /// </summary>
   public interface IVirtualStorage : IBlobStorage
   {
      /// <summary>
      /// Mounts a storage to virtual path
      /// </summary>
      void Mount(string path, IBlobStorage storage);
   }
}
