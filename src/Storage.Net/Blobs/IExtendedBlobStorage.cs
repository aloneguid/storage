using System.Threading;
using System.Threading.Tasks;

namespace Storage.Net.Blobs
{
   /// <summary>
   /// Extended blob storage operations that may be supported natively by a provider as are slow otherwise.
   /// </summary>
   public interface IExtendedBlobStorage : IBlobStorage
   {
      /// <summary>
      /// Rename a blob (folder or file)
      /// </summary>
      /// <param name="oldPath"></param>
      /// <param name="newPath"></param>
      /// <param name="cancellationToken"></param>
      /// <returns></returns>
      Task RenameAsync(string oldPath, string newPath, CancellationToken cancellationToken = default);
   }
}
