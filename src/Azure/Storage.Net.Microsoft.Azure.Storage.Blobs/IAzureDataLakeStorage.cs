using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Storage.Net.Microsoft.Azure.Storage.Blobs.Gen2.Model;

namespace Storage.Net.Microsoft.Azure.Storage.Blobs
{
   /// <summary>
   /// Additional Gen 2 storage operations
   /// </summary>
   public interface IAzureDataLakeStorage : IAzureBlobStorage
   {
      /// <summary>
      /// Lists filesystems using Data Lake native REST API
      /// </summary>
      /// <returns></returns>
      Task<IReadOnlyCollection<Filesystem>> ListFilesystemsAsync(CancellationToken cancellationToken = default);

      /// <summary>
      /// Creates a filesystem
      /// </summary>
      /// <param name="filesystemName"></param>
      /// <param name="cancellationToken"></param>
      /// <returns></returns>
      Task CreateFilesystemAsync(string filesystemName, CancellationToken cancellationToken = default);

      /// <summary>
      /// Deletes a filesystem
      /// </summary>
      /// <param name="filesystem"></param>
      /// <param name="cancellationToken"></param>
      /// <returns></returns>
      Task DeleteFilesystemAsync(string filesystem, CancellationToken cancellationToken = default);


      /// <summary>
      /// Sets permissions on an object
      /// </summary>
      /// <param name="fullPath"></param>
      /// <param name="accessControl">Access control rules. A good idea whould be to retreive them using <see cref="GetAccessControlAsync(string, bool, CancellationToken)"/>, modify, and send back via this method.</param>
      /// <param name="cancellationToken"></param>
      /// <returns></returns>
      Task SetAccessControlAsync(string fullPath, AccessControl accessControl, CancellationToken cancellationToken = default);

      /// <summary>
      /// Gets permissions from an object
      /// </summary>
      /// <param name="fullPath"></param>
      /// <param name="getUpn">When true, the call will return UPNs instead of object IDs when querying for permissions</param>
      /// <param name="cancellationToken"></param>
      /// <returns></returns>
      Task<AccessControl> GetAccessControlAsync(string fullPath, bool getUpn = false, CancellationToken cancellationToken = default);

   }
}
